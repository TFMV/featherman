package pool

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/metrics"
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

// Manager manages a pool of warm DuckDB pods
type Manager struct {
	client    client.Client
	k8sClient kubernetes.Interface
	scheme    *runtime.Scheme
	logger    *zerolog.Logger
	pool      *ducklakev1alpha1.DuckLakePool
	namespace string
	config    *rest.Config

	mu           sync.RWMutex
	pods         map[string]*WarmPod
	requestQueue chan Request
	stopCh       chan struct{}
}

// NewManager creates a new pool manager
func NewManager(
	client client.Client,
	k8sClient kubernetes.Interface,
	scheme *runtime.Scheme,
	logger *zerolog.Logger,
	pool *ducklakev1alpha1.DuckLakePool,
	namespace string,
	config *rest.Config,
) *Manager {
	return &Manager{
		client:       client,
		k8sClient:    k8sClient,
		scheme:       scheme,
		logger:       logger,
		pool:         pool,
		namespace:    namespace,
		config:       config,
		pods:         make(map[string]*WarmPod),
		requestQueue: make(chan Request, 100),
		stopCh:       make(chan struct{}),
	}
}

// Start starts the pool manager
func (m *Manager) Start(ctx context.Context) error {
	m.logger.Info().Msg("starting pool manager")

	// Start background workers
	go m.scaleLoop(ctx)
	go m.lifecycleLoop(ctx)
	go m.requestHandler(ctx)

	// Initialize pool to minimum size
	if err := m.ensureMinimumPods(ctx); err != nil {
		return fmt.Errorf("failed to ensure minimum pods: %w", err)
	}

	<-ctx.Done()
	close(m.stopCh)
	return ctx.Err()
}

// RequestPod requests a warm pod from the pool
func (m *Manager) RequestPod(ctx context.Context, catalog string, resources corev1.ResourceRequirements, timeout time.Duration) (*WarmPod, error) {
	startTime := time.Now()

	req := Request{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Catalog:   catalog,
		Resources: resources,
		Timeout:   timeout,
		Response:  make(chan Response, 1),
	}

	select {
	case m.requestQueue <- req:
		// Request queued
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout queueing request")
	}

	// Wait for response
	select {
	case resp := <-req.Response:
		waitDuration := time.Since(startTime).Seconds()
		metrics.RecordWaitDuration(m.pool.Name, m.namespace, waitDuration)
		return resp.Pod, resp.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for pod")
	}
}

// ReleasePod releases a pod back to the pool
func (m *Manager) ReleasePod(ctx context.Context, podName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	pod, ok := m.pods[podName]
	if !ok {
		return fmt.Errorf("pod %s not found in pool", podName)
	}

	pod.State = ducklakev1alpha1.PodStateIdle
	pod.LastUsed = time.Now()
	pod.QueryCount++

	// Update metrics
	metrics.RecordPodReleased(m.pool.Name, m.namespace)

	return nil
}

// scaleLoop handles pool scaling
func (m *Manager) scaleLoop(ctx context.Context) {
	ticker := time.NewTicker(m.pool.Spec.ScalingBehavior.ScaleInterval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.evaluateScaling(ctx); err != nil {
				m.logger.Error().Err(err).Msg("failed to evaluate scaling")
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
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.enforceLifecyclePolicies(ctx); err != nil {
				m.logger.Error().Err(err).Msg("failed to enforce lifecycle policies")
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
	for {
		select {
		case req := <-m.requestQueue:
			go m.handleRequest(ctx, req)
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		}
	}
}

// handleRequest handles a single pod request
func (m *Manager) handleRequest(ctx context.Context, req Request) {
	pod, err := m.acquirePod(ctx, req)

	select {
	case req.Response <- Response{Pod: pod, Err: err}:
		// Response sent
	case <-ctx.Done():
		// Context cancelled
		if pod != nil {
			_ = m.ReleasePod(context.Background(), pod.Name)
		}
	}
}

// acquirePod acquires an idle pod from the pool
func (m *Manager) acquirePod(ctx context.Context, req Request) (*WarmPod, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find best matching idle pod
	var bestPod *WarmPod
	var bestScore float64

	for _, pod := range m.pods {
		if pod.State != ducklakev1alpha1.PodStateIdle {
			continue
		}

		// Score based on:
		// - Time since last use (prefer LRU)
		// - Query count (prefer lower)
		// - Resource fit
		score := m.scorePod(pod, req)
		if bestPod == nil || score > bestScore {
			bestPod = pod
			bestScore = score
		}
	}

	if bestPod == nil {
		metrics.RecordPoolQueueLength(m.pool.Name, m.namespace, len(m.requestQueue))
		return nil, fmt.Errorf("no idle pods available")
	}

	// Mark pod as busy
	bestPod.State = ducklakev1alpha1.PodStateBusy
	metrics.RecordPodAcquired(m.pool.Name, m.namespace)

	return bestPod, nil
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

// evaluateScaling evaluates if the pool should scale
func (m *Manager) evaluateScaling(ctx context.Context) error {
	m.mu.RLock()
	currentSize := int32(len(m.pods))
	idleCount := int32(0)
	busyCount := int32(0)

	for _, pod := range m.pods {
		switch pod.State {
		case ducklakev1alpha1.PodStateIdle:
			idleCount++
		case ducklakev1alpha1.PodStateBusy:
			busyCount++
		}
	}
	m.mu.RUnlock()

	// Calculate desired size based on utilization
	targetUtil, _ := strconv.ParseFloat(m.pool.Spec.TargetUtilization, 64)

	desiredSize := int32(math.Ceil(float64(busyCount) / targetUtil))
	desiredSize = max(m.pool.Spec.MinSize, min(m.pool.Spec.MaxSize, desiredSize))

	// Update metrics
	metrics.UpdatePoolMetrics(m.pool.Name, m.namespace, currentSize, desiredSize, idleCount, busyCount)

	// Scale if needed
	if desiredSize > currentSize {
		toAdd := min(desiredSize-currentSize, m.pool.Spec.ScalingBehavior.ScaleUpRate)
		return m.scaleUp(ctx, toAdd)
	} else if desiredSize < currentSize {
		// Check stabilization window
		if m.pool.Status.LastScaleTime != nil {
			timeSinceLastScale := time.Since(m.pool.Status.LastScaleTime.Time)
			if timeSinceLastScale < m.pool.Spec.ScalingBehavior.StabilizationWindow.Duration {
				return nil // Too soon to scale down
			}
		}

		toRemove := min(currentSize-desiredSize, m.pool.Spec.ScalingBehavior.ScaleDownRate)
		return m.scaleDown(ctx, toRemove)
	}

	return nil
}

// scaleUp adds pods to the pool
func (m *Manager) scaleUp(ctx context.Context, count int32) error {
	m.logger.Info().Int32("count", count).Msg("scaling up pool")

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
	m.logger.Info().Int32("count", count).Msg("scaling down pool")

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
			m.logger.Error().Err(err).Str("pod", podName).Msg("failed to delete pod")
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
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	toRemove := []string{}

	for name, pod := range m.pods {
		// Skip busy pods
		if pod.State == ducklakev1alpha1.PodStateBusy {
			continue
		}

		// Check max idle time
		if m.pool.Spec.LifecyclePolicies.MaxIdleTime.Duration > 0 {
			idleTime := now.Sub(pod.LastUsed)
			if idleTime > m.pool.Spec.LifecyclePolicies.MaxIdleTime.Duration {
				toRemove = append(toRemove, name)
				continue
			}
		}

		// Check max lifetime
		if m.pool.Spec.LifecyclePolicies.MaxLifetime.Duration > 0 {
			lifetime := now.Sub(pod.CreatedAt)
			if lifetime > m.pool.Spec.LifecyclePolicies.MaxLifetime.Duration {
				toRemove = append(toRemove, name)
				continue
			}
		}

		// Check max queries
		if m.pool.Spec.LifecyclePolicies.MaxQueries > 0 {
			if pod.QueryCount >= m.pool.Spec.LifecyclePolicies.MaxQueries {
				toRemove = append(toRemove, name)
				continue
			}
		}
	}

	// Remove pods that violate policies
	for _, name := range toRemove {
		m.logger.Info().Str("pod", name).Msg("removing pod due to lifecycle policy")
		pod := m.pods[name]
		if err := m.deletePod(ctx, pod); err != nil {
			m.logger.Error().Err(err).Str("pod", name).Msg("failed to delete pod")
			continue
		}
		delete(m.pods, name)
	}

	// Ensure minimum pods
	return m.ensureMinimumPods(ctx)
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
		State:      ducklakev1alpha1.PodStateIdle,
		LastUsed:   time.Now(),
		QueryCount: 0,
		CreatedAt:  time.Now(),
		client:     m.client,
		k8sClient:  m.k8sClient,
		config:     m.config,
	}

	m.logger.Info().Str("pod", podName).Msg("created warm pod")
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
	m.logger.Info().Msg("shutting down pool manager")

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
			m.logger.Error().Err(err).Str("pod", pod.Name).Msg("failed to delete pod during shutdown")
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
