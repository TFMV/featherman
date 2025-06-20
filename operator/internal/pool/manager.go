package pool

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/logger"
	"github.com/TFMV/featherman/operator/internal/metrics"
	"github.com/TFMV/featherman/operator/internal/retry"
)

// Request represents a request for a warm pod
type Request struct {
	ID        string
	Catalog   string
	Resources corev1.ResourceRequirements
	Timeout   time.Duration
	Response  chan Response
}

// Response represents the response to a pod request
type Response struct {
	Pod *WarmPod
	Err error
}

// WarmPod represents a warm DuckDB pod
type WarmPod struct {
	Name       string
	Namespace  string
	PoolName   string
	State      ducklakev1alpha1.PodState
	LastUsed   time.Time
	QueryCount int32
	CreatedAt  time.Time
	client     client.Client
	k8sClient  kubernetes.Interface
	config     *rest.Config
}

// ExecuteQuery executes a SQL query on this warm pod
func (p *WarmPod) ExecuteQuery(ctx context.Context, sql string, timeout time.Duration) (string, error) {
	executor := NewExecutor(p.k8sClient, p.config, nil)
	return executor.ExecuteQuery(ctx, p, sql, timeout)
}

// ExecuteQueryWithRetry executes a query with retries and records metrics
func (p *WarmPod) ExecuteQueryWithRetry(ctx context.Context, sql string, timeout time.Duration, retries int, recorder record.EventRecorder) (string, error) {
	var result string
	attempt := 0
	op := func(ctx context.Context) error {
		attempt++
		r, err := p.ExecuteQuery(ctx, sql, timeout)
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				metrics.RecordQueryTimeout(p.PoolName, p.Namespace)
				if recorder != nil {
					poolRef := &ducklakev1alpha1.DuckLakePool{ObjectMeta: metav1.ObjectMeta{Name: p.PoolName, Namespace: p.Namespace}}
					recorder.Eventf(poolRef, corev1.EventTypeWarning, "QueryTimeout", "query exceeded %s on pod %s", timeout.String(), p.Name)
				}
			}
			if attempt <= retries {
				metrics.RecordRetry(p.PoolName, p.Namespace)
			}
			return err
		}
		result = r
		return nil
	}
	cfg := retry.DefaultRetryConfig.WithMaxRetries(retries)
	err := retry.Do(ctx, op, cfg)
	return result, err
}

// Manager manages a pool of warm DuckDB pods
type Manager struct {
	client    client.Client
	k8sClient kubernetes.Interface
	scheme    *runtime.Scheme
	pool      *ducklakev1alpha1.DuckLakePool
	namespace string
	config    *rest.Config

	mu               sync.RWMutex
	pods             map[string]*WarmPod
	requestQueue     chan Request
	stopCh           chan struct{}
	recorder         record.EventRecorder
	maxQueryDuration time.Duration
	maxRetries       int
	queueTimeout     time.Duration
}

// NewManager creates a new pool manager
func NewManager(
	client client.Client,
	k8sClient kubernetes.Interface,
	scheme *runtime.Scheme,
	pool *ducklakev1alpha1.DuckLakePool,
	namespace string,
	config *rest.Config,
	recorder record.EventRecorder,
) *Manager {
	queueLen := int(pool.Spec.Queue.MaxLength)
	if queueLen <= 0 {
		queueLen = 100
	}
	maxDur := pool.Spec.MaxQueryDuration.Duration
	if maxDur <= 0 {
		maxDur = 60 * time.Second
	}
	retries := int(pool.Spec.MaxRetries)
	if retries <= 0 {
		retries = 3
	}
	qTimeout := pool.Spec.Queue.MaxWaitTime.Duration
	if qTimeout <= 0 {
		qTimeout = 30 * time.Second
	}
	return &Manager{
		client:           client,
		k8sClient:        k8sClient,
		scheme:           scheme,
		pool:             pool,
		namespace:        namespace,
		config:           config,
		pods:             make(map[string]*WarmPod),
		requestQueue:     make(chan Request, queueLen),
		stopCh:           make(chan struct{}),
		recorder:         recorder,
		maxQueryDuration: maxDur,
		maxRetries:       retries,
		queueTimeout:     qTimeout,
	}
}

// GetMaxQueryDuration returns the configured maximum query duration
func (m *Manager) GetMaxQueryDuration() time.Duration {
	return m.maxQueryDuration
}

// GetMaxRetries returns the configured retry count
func (m *Manager) GetMaxRetries() int {
	return m.maxRetries
}

// Start starts the pool manager
func (m *Manager) Start(ctx context.Context) error {
	l := logger.FromContext(ctx).With().
		Str("pool", m.pool.Name).
		Str("namespace", m.namespace).
		Logger()
	ctx = logger.WithContext(ctx, &l)

	l.Info().
		Int32("minSize", m.pool.Spec.MinSize).
		Int32("maxSize", m.pool.Spec.MaxSize).
		Str("scaleInterval", m.pool.Spec.ScalingBehavior.ScaleInterval.Duration.String()).
		Msg("Starting pool manager")

	// Start background workers
	go m.scaleLoop(ctx)
	go m.lifecycleLoop(ctx)
	go m.requestHandler(ctx)

	// Initialize pool to minimum size
	if err := m.ensureMinimumPods(ctx); err != nil {
		l.Error().Err(err).Msg("Failed to ensure minimum pods")
		return fmt.Errorf("failed to ensure minimum pods: %w", err)
	}

	<-ctx.Done()
	close(m.stopCh)
	return ctx.Err()
}

// RequestPod requests a warm pod from the pool
func (m *Manager) RequestPod(ctx context.Context, catalog string, resources corev1.ResourceRequirements, timeout time.Duration) (*WarmPod, error) {
	l := logger.FromContext(ctx).With().
		Str("pool", m.pool.Name).
		Str("namespace", m.namespace).
		Str("catalog", catalog).
		Str("timeout", timeout.String()).
		Logger()

	startTime := time.Now()
	l.Debug().Msg("Requesting warm pod from pool")

	req := Request{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Catalog:   catalog,
		Resources: resources,
		Timeout:   timeout,
		Response:  make(chan Response, 1),
	}

	select {
	case m.requestQueue <- req:
		l.Debug().
			Str("requestID", req.ID).
			Msg("Request queued successfully")
	case <-ctx.Done():
		l.Debug().Msg("Request cancelled by context")
		return nil, ctx.Err()
	case <-time.After(timeout):
		l.Error().Msg("Timeout queueing request")
		return nil, fmt.Errorf("timeout queueing request")
	}

	// Wait for response
	select {
	case resp := <-req.Response:
		waitDuration := time.Since(startTime).Seconds()
		metrics.RecordWaitDuration(m.pool.Name, m.namespace, waitDuration)
		if resp.Err != nil {
			l.Error().
				Err(resp.Err).
				Float64("waitDuration", waitDuration).
				Msg("Failed to acquire pod")
		} else {
			l.Info().
				Str("pod", resp.Pod.Name).
				Float64("waitDuration", waitDuration).
				Msg("Successfully acquired pod")
		}
		return resp.Pod, resp.Err
	case <-ctx.Done():
		l.Debug().Msg("Request cancelled by context while waiting")
		return nil, ctx.Err()
	case <-time.After(timeout):
		l.Error().Msg("Timeout waiting for pod")
		return nil, fmt.Errorf("timeout waiting for pod")
	}
}

// ReleasePod releases a pod back to the pool
func (m *Manager) ReleasePod(ctx context.Context, podName string) error {
	l := logger.FromContext(ctx).With().
		Str("pool", m.pool.Name).
		Str("namespace", m.namespace).
		Str("pod", podName).
		Logger()

	m.mu.Lock()
	defer m.mu.Unlock()

	pod, ok := m.pods[podName]
	if !ok {
		l.Error().Msg("Pod not found in pool")
		return fmt.Errorf("pod %s not found in pool", podName)
	}

	pod.State = ducklakev1alpha1.PodStateIdle
	pod.LastUsed = time.Now()
	pod.QueryCount++

	// Update metrics
	metrics.RecordPodReleased(m.pool.Name, m.namespace)

	l.Info().
		Int32("queryCount", pod.QueryCount).
		Msg("Pod released back to pool")

	return nil
}

// scaleLoop handles pool scaling
func (m *Manager) scaleLoop(ctx context.Context) {
	l := logger.FromContext(ctx)
	ticker := time.NewTicker(m.pool.Spec.ScalingBehavior.ScaleInterval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.evaluateScaling(ctx); err != nil {
				l.Error().Err(err).Msg("Failed to evaluate scaling")
			}
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		}
	}
}

// lifecycleLoop handles pod lifecycle policies
func (m *Manager) lifecycleLoop(ctx context.Context) {
	l := logger.FromContext(ctx)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.enforceLifecyclePolicies(ctx); err != nil {
				l.Error().Err(err).Msg("Failed to enforce lifecycle policies")
			}
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		}
	}
}

// requestHandler handles pod requests
func (m *Manager) requestHandler(ctx context.Context) {
	l := logger.FromContext(ctx)
	l.Debug().Msg("Starting request handler")

	for {
		select {
		case req := <-m.requestQueue:
			l.Debug().
				Str("requestID", req.ID).
				Msg("Processing pod request")
			go m.handleRequest(ctx, req)
		case <-ctx.Done():
			l.Debug().Msg("Request handler stopped by context")
			return
		case <-m.stopCh:
			l.Debug().Msg("Request handler stopped by stop channel")
			return
		}
	}
}

// handleRequest handles a single pod request
func (m *Manager) handleRequest(ctx context.Context, req Request) {
	l := logger.FromContext(ctx).With().
		Str("requestID", req.ID).
		Logger()

	l.Debug().Msg("Handling pod request")
	pod, err := m.acquirePod(ctx, req)

	select {
	case req.Response <- Response{Pod: pod, Err: err}:
		if err != nil {
			l.Error().
				Err(err).
				Msg("Failed to acquire pod")
		} else {
			l.Debug().
				Str("pod", pod.Name).
				Msg("Successfully acquired pod")
		}
	case <-ctx.Done():
		l.Debug().Msg("Request cancelled while sending response")
		if pod != nil {
			if err := m.ReleasePod(context.Background(), pod.Name); err != nil {
				l.Error().
					Err(err).
					Str("pod", pod.Name).
					Msg("Failed to release pod after context cancellation")
			}
		}
	}
}

// acquirePod acquires an idle pod from the pool
func (m *Manager) acquirePod(ctx context.Context, req Request) (*WarmPod, error) {
	l := logger.FromContext(ctx).With().
		Str("requestID", req.ID).
		Logger()

	m.mu.Lock()
	defer m.mu.Unlock()

	l.Debug().Msg("Looking for available pod")

	// Find best matching idle pod
	var bestPod *WarmPod
	var bestScore float64

	for _, pod := range m.pods {
		if pod.State != ducklakev1alpha1.PodStateIdle {
			continue
		}

		score := m.scorePod(pod, req)
		if bestPod == nil || score > bestScore {
			bestPod = pod
			bestScore = score
		}
	}

	if bestPod != nil {
		bestPod.State = ducklakev1alpha1.PodStateBusy
		l.Info().
			Str("pod", bestPod.Name).
			Float64("score", bestScore).
			Msg("Found suitable pod")
		return bestPod, nil
	}

	// No idle pods available, create a new one if possible
	currentSize := int32(len(m.pods))
	if currentSize >= m.pool.Spec.MaxSize {
		l.Error().
			Int32("currentSize", currentSize).
			Int32("maxSize", m.pool.Spec.MaxSize).
			Msg("Cannot create new pod, pool at maximum size")
		return nil, fmt.Errorf("pool at maximum size")
	}

	l.Debug().Msg("Creating new pod")
	pod, err := m.createPod(ctx)
	if err != nil {
		l.Error().
			Err(err).
			Msg("Failed to create new pod")
		return nil, fmt.Errorf("failed to create pod: %w", err)
	}

	pod.State = ducklakev1alpha1.PodStateBusy
	m.pods[pod.Name] = pod

	l.Info().
		Str("pod", pod.Name).
		Msg("Created and acquired new pod")
	return pod, nil
}

// scorePod scores a pod for a request (higher is better)
func (m *Manager) scorePod(pod *WarmPod, req Request) float64 {
	score := 100.0

	// Prefer least recently used
	idleTime := time.Since(pod.LastUsed).Seconds()
	score += idleTime / 60.0 // 1 point per minute idle

	// Prefer pods with fewer queries
	score -= float64(pod.QueryCount) * 0.1

	return score
}

// evaluateScaling evaluates if the pool needs to be scaled
func (m *Manager) evaluateScaling(ctx context.Context) error {
	l := logger.FromContext(ctx).With().
		Str("pool", m.pool.Name).
		Str("namespace", m.namespace).
		Logger()

	m.mu.Lock()
	defer m.mu.Unlock()

	currentSize := int32(len(m.pods))
	busyCount := int32(0)
	idleCount := int32(0)

	for _, pod := range m.pods {
		switch pod.State {
		case ducklakev1alpha1.PodStateBusy:
			busyCount++
		case ducklakev1alpha1.PodStateIdle:
			idleCount++
		}
	}

	l.Debug().
		Int32("currentSize", currentSize).
		Int32("busyPods", busyCount).
		Int32("idlePods", idleCount).
		Msg("Current pool state")

	// Calculate target size based on utilization
	targetUtilization, _ := strconv.ParseFloat(m.pool.Spec.TargetUtilization, 64)
	if targetUtilization <= 0 {
		targetUtilization = 0.8 // Default from CRD
	}

	desiredSize := int32(math.Ceil(float64(busyCount) / targetUtilization))
	desiredSize = max(desiredSize, m.pool.Spec.MinSize)
	desiredSize = min(desiredSize, m.pool.Spec.MaxSize)

	l.Debug().
		Float64("targetUtilization", targetUtilization).
		Int32("desiredSize", desiredSize).
		Msg("Calculated desired size")

	// Scale up if needed
	if desiredSize > currentSize {
		scaleUpCount := min(desiredSize-currentSize, m.pool.Spec.ScalingBehavior.ScaleUpRate)
		if scaleUpCount > 0 {
			l.Info().
				Int32("scaleUpCount", scaleUpCount).
				Msg("Scaling up pool")
			if err := m.scaleUp(ctx, scaleUpCount); err != nil {
				l.Error().
					Err(err).
					Int32("scaleUpCount", scaleUpCount).
					Msg("Failed to scale up")
				return fmt.Errorf("failed to scale up: %w", err)
			}
		}
	}

	// Scale down if needed
	if desiredSize < currentSize {
		scaleDownCount := min(currentSize-desiredSize, m.pool.Spec.ScalingBehavior.ScaleDownRate)
		if scaleDownCount > 0 {
			l.Info().
				Int32("scaleDownCount", scaleDownCount).
				Msg("Scaling down pool")
			if err := m.scaleDown(ctx, scaleDownCount); err != nil {
				l.Error().
					Err(err).
					Int32("scaleDownCount", scaleDownCount).
					Msg("Failed to scale down")
				return fmt.Errorf("failed to scale down: %w", err)
			}
		}
	}

	return nil
}

// scaleUp adds pods to the pool
func (m *Manager) scaleUp(ctx context.Context, count int32) error {
	l := logger.FromContext(ctx)
	l.Info().Int32("count", count).Msg("scaling up pool")

	metrics.RecordScalingEvent(m.pool.Name, m.namespace, "up")

	for i := int32(0); i < count; i++ {
		pod, err := m.createPod(ctx)
		if err != nil {
			return fmt.Errorf("failed to create pod: %w", err)
		}

		m.mu.Lock()
		m.pods[pod.Name] = pod
		m.mu.Unlock()
	}

	// Update last scale time
	now := metav1.Now()
	m.pool.Status.LastScaleTime = &now

	return nil
}

// scaleDown removes pods from the pool
func (m *Manager) scaleDown(ctx context.Context, count int32) error {
	l := logger.FromContext(ctx)
	l.Info().Int32("count", count).Msg("scaling down pool")

	metrics.RecordScalingEvent(m.pool.Name, m.namespace, "down")

	m.mu.Lock()
	defer m.mu.Unlock()

	// Select pods for removal (prefer idle, oldest)
	toRemove := m.selectPodsForRemoval(count)

	for _, podName := range toRemove {
		pod := m.pods[podName]
		pod.State = ducklakev1alpha1.PodStateDraining

		// Delete the pod
		if err := m.deletePod(ctx, pod); err != nil {
			l.Error().Err(err).Str("pod", podName).Msg("failed to delete pod")
			continue
		}

		delete(m.pods, podName)
	}

	// Update last scale time
	now := metav1.Now()
	m.pool.Status.LastScaleTime = &now

	return nil
}

// selectPodsForRemoval selects pods to remove from the pool
func (m *Manager) selectPodsForRemoval(count int32) []string {
	var candidates []string

	// First, select idle pods
	for name, pod := range m.pods {
		if pod.State == ducklakev1alpha1.PodStateIdle {
			candidates = append(candidates, name)
			if int32(len(candidates)) >= count {
				return candidates
			}
		}
	}

	// If not enough idle pods, select oldest pods
	// TODO: Sort by creation time and select oldest

	return candidates
}

// enforceLifecyclePolicies enforces pod lifecycle policies
func (m *Manager) enforceLifecyclePolicies(ctx context.Context) error {
	l := logger.FromContext(ctx).With().
		Str("pool", m.pool.Name).
		Str("namespace", m.namespace).
		Logger()

	l.Debug().Msg("Enforcing lifecycle policies")

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var toRemove []string

	for name, pod := range m.pods {
		if pod.State != ducklakev1alpha1.PodStateIdle {
			continue
		}

		// Check idle time
		if m.pool.Spec.LifecyclePolicies.MaxIdleTime.Duration > 0 &&
			now.Sub(pod.LastUsed) > m.pool.Spec.LifecyclePolicies.MaxIdleTime.Duration {
			l.Debug().
				Str("pod", name).
				Time("lastUsed", pod.LastUsed).
				Dur("idleTime", now.Sub(pod.LastUsed)).
				Msg("Pod exceeded max idle time")
			toRemove = append(toRemove, name)
			continue
		}

		// Check lifetime
		if m.pool.Spec.LifecyclePolicies.MaxLifetime.Duration > 0 &&
			now.Sub(pod.CreatedAt) > m.pool.Spec.LifecyclePolicies.MaxLifetime.Duration {
			l.Debug().
				Str("pod", name).
				Time("createdAt", pod.CreatedAt).
				Dur("lifetime", now.Sub(pod.CreatedAt)).
				Msg("Pod exceeded max lifetime")
			toRemove = append(toRemove, name)
			continue
		}

		// Check query count
		if m.pool.Spec.LifecyclePolicies.MaxQueries > 0 &&
			pod.QueryCount >= m.pool.Spec.LifecyclePolicies.MaxQueries {
			l.Debug().
				Str("pod", name).
				Int32("queryCount", pod.QueryCount).
				Int32("maxQueries", m.pool.Spec.LifecyclePolicies.MaxQueries).
				Msg("Pod exceeded max queries")
			toRemove = append(toRemove, name)
			continue
		}
	}

	// Remove pods that violate policies
	for _, name := range toRemove {
		pod := m.pods[name]
		l.Info().
			Str("pod", name).
			Time("createdAt", pod.CreatedAt).
			Time("lastUsed", pod.LastUsed).
			Int32("queryCount", pod.QueryCount).
			Msg("Removing pod due to lifecycle policy")

		if err := m.deletePod(ctx, pod); err != nil {
			l.Error().
				Err(err).
				Str("pod", name).
				Msg("Failed to delete pod")
			continue
		}
		delete(m.pods, name)
	}

	if len(toRemove) > 0 {
		l.Info().
			Int("removedCount", len(toRemove)).
			Msg("Completed lifecycle policy enforcement")
	} else {
		l.Debug().Msg("No pods need to be removed")
	}

	return nil
}

// ensureMinimumPods ensures the pool has the minimum number of pods
func (m *Manager) ensureMinimumPods(ctx context.Context) error {
	currentSize := int32(len(m.pods))
	if currentSize < m.pool.Spec.MinSize {
		toAdd := m.pool.Spec.MinSize - currentSize
		return m.scaleUp(ctx, toAdd)
	}
	return nil
}

// createPod creates a new warm pod
func (m *Manager) createPod(ctx context.Context) (*WarmPod, error) {
	podName := fmt.Sprintf("%s-pool-%s", m.pool.Name, generateUID())

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: m.namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "ducklake",
				"app.kubernetes.io/component":  "warm-pod",
				"app.kubernetes.io/managed-by": "ducklake-operator",
				"ducklake.featherman.dev/pool": m.pool.Name,
			},
		},
		Spec: m.buildPodSpec(),
	}

	if err := m.client.Create(ctx, pod); err != nil {
		return nil, fmt.Errorf("failed to create pod: %w", err)
	}

	// Record pod creation
	metrics.RecordPodCreated(m.pool.Name, m.namespace)

	// Wait for pod to be ready
	if err := m.waitForPodReady(ctx, podName); err != nil {
		// Clean up failed pod
		_ = m.client.Delete(ctx, pod)
		return nil, fmt.Errorf("pod failed to become ready: %w", err)
	}

	warmPod := &WarmPod{
		Name:       podName,
		Namespace:  m.namespace,
		PoolName:   m.pool.Name,
		State:      ducklakev1alpha1.PodStateIdle,
		LastUsed:   time.Now(),
		QueryCount: 0,
		CreatedAt:  time.Now(),
		client:     m.client,
		k8sClient:  m.k8sClient,
		config:     m.config,
	}

	l := logger.FromContext(ctx).With().
		Str("pod", podName).
		Logger()
	l.Info().Msg("created warm pod")
	return warmPod, nil
}

// buildPodSpec builds the pod spec for a warm pod
func (m *Manager) buildPodSpec() corev1.PodSpec {
	catalogName := m.pool.Spec.CatalogRef.Name
	pvcName := fmt.Sprintf("%s-catalog", catalogName)

	spec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers: []corev1.Container{
			{
				Name:            "duckdb",
				Image:           m.pool.Spec.Template.Image,
				ImagePullPolicy: m.pool.Spec.Template.ImagePullPolicy,
				Resources:       m.pool.Spec.Template.Resources,
				Command: []string{
					"/bin/sh",
					"-c",
					"duckdb /catalog/duck.db -c 'SELECT 1;' && sleep infinity",
				},
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "catalog",
						MountPath: "/catalog",
						ReadOnly:  m.pool.Spec.CatalogRef.ReadOnly,
					},
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						Exec: &corev1.ExecAction{
							Command: []string{
								"duckdb",
								"/catalog/duck.db",
								"-c",
								"SELECT 1;",
							},
						},
					},
					InitialDelaySeconds: 10,
					PeriodSeconds:       30,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						Exec: &corev1.ExecAction{
							Command: []string{
								"duckdb",
								"/catalog/duck.db",
								"-c",
								"SELECT 1;",
							},
						},
					},
					InitialDelaySeconds: 5,
					PeriodSeconds:       10,
				},
			},
		},
		Volumes: []corev1.Volume{
			{
				Name: "catalog",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvcName,
						ReadOnly:  m.pool.Spec.CatalogRef.ReadOnly,
					},
				},
			},
		},
		SecurityContext:              m.pool.Spec.Template.SecurityContext,
		NodeSelector:                 m.pool.Spec.Template.NodeSelector,
		Tolerations:                  m.pool.Spec.Template.Tolerations,
		Affinity:                     m.pool.Spec.Template.Affinity,
		ServiceAccountName:           "ducklake-pool",
		AutomountServiceAccountToken: &[]bool{true}[0],
	}

	return spec
}

// waitForPodReady waits for a pod to become ready
func (m *Manager) waitForPodReady(ctx context.Context, podName string) error {
	timeout := time.After(2 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout waiting for pod to be ready")
		case <-ticker.C:
			pod := &corev1.Pod{}
			if err := m.client.Get(ctx, types.NamespacedName{
				Name:      podName,
				Namespace: m.namespace,
			}, pod); err != nil {
				if errors.IsNotFound(err) {
					continue
				}
				return err
			}

			// Check if pod is ready
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					return nil
				}
			}
		}
	}
}

// deletePod deletes a pod
func (m *Manager) deletePod(ctx context.Context, pod *WarmPod) error {
	k8sPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
	}

	if err := m.client.Delete(ctx, k8sPod); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete pod: %w", err)
	}

	// Record pod deletion
	metrics.RecordPodDeleted(m.pool.Name, m.namespace)

	return nil
}

// GetPoolStatus returns the current pool status
func (m *Manager) GetPoolStatus() ducklakev1alpha1.DuckLakePoolStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := ducklakev1alpha1.DuckLakePoolStatus{
		CurrentSize: int32(len(m.pods)),
		DesiredSize: m.pool.Spec.MinSize, // Will be updated by scaling logic
		Pods:        []ducklakev1alpha1.PodInfo{},
	}

	for _, pod := range m.pods {
		status.Pods = append(status.Pods, ducklakev1alpha1.PodInfo{
			Name:       pod.Name,
			State:      pod.State,
			LastUsed:   metav1.NewTime(pod.LastUsed),
			QueryCount: pod.QueryCount,
			CreatedAt:  metav1.NewTime(pod.CreatedAt),
		})

		switch pod.State {
		case ducklakev1alpha1.PodStateIdle:
			status.IdlePods++
		case ducklakev1alpha1.PodStateBusy:
			status.BusyPods++
		}
	}

	status.QueueLength = int32(len(m.requestQueue))

	return status
}

// Shutdown gracefully shuts down the pool manager
func (m *Manager) Shutdown(ctx context.Context) error {
	l := logger.FromContext(ctx).With().
		Str("pool", m.pool.Name).
		Str("namespace", m.namespace).
		Logger()

	l.Info().Msg("shutting down pool manager")

	// Stop accepting new requests
	close(m.requestQueue)

	// Mark all pods as draining
	m.mu.Lock()
	for _, pod := range m.pods {
		pod.State = ducklakev1alpha1.PodStateDraining
	}
	m.mu.Unlock()

	// Delete all pods
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, pod := range m.pods {
		if err := m.deletePod(ctx, pod); err != nil {
			l.Error().Err(err).Str("pod", pod.Name).Msg("failed to delete pod during shutdown")
		}
	}

	return nil
}

// Helper functions

func generateUID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func min(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}
