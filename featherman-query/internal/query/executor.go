package query

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// ExecutorInterface defines the interface for query execution
type ExecutorInterface interface {
	Execute(ctx context.Context, req QueryRequest, w http.ResponseWriter) (string, error)
}

// Executor selects pods and executes queries
type Executor struct {
	client    kubernetes.Interface
	cfg       *rest.Config
	namespace string
	timeout   time.Duration
}

// Ensure Executor implements ExecutorInterface
var _ ExecutorInterface = (*Executor)(nil)

func NewExecutor(c kubernetes.Interface, cfg *rest.Config) *Executor {
	return &Executor{
		client:    c,
		cfg:       cfg,
		namespace: getNamespace(),
		timeout:   5 * time.Minute, // Default timeout for query execution
	}
}

// getNamespace returns the current namespace or default
func getNamespace() string {
	// In a real implementation, this could read from service account or config
	// For now, default to "default" namespace
	return "default"
}

func (e *Executor) Execute(ctx context.Context, req QueryRequest, w http.ResponseWriter) (string, error) {
	// Add timeout to context if not already set
	if _, hasTimeout := ctx.Deadline(); !hasTimeout {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.timeout)
		defer cancel()
	}

	if err := validateSQL(req.SQL); err != nil {
		return "", fmt.Errorf("invalid SQL: %w", err)
	}

	// Try to find a warm pod first
	pod, err := e.selectWarmPod(ctx, req.Catalog)
	if err != nil {
		// No warm pod available, create ephemeral job
		pod, err = e.runEphemeralJob(ctx, req)
		if err != nil {
			return "", fmt.Errorf("failed to create ephemeral job: %w", err)
		}
		defer func() {
			// Clean up job in background
			go e.cleanupJob(context.Background(), pod)
		}()
	}

	// Set appropriate content type
	switch req.Format {
	case "arrow":
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	case "json":
		w.Header().Set("Content-Type", "application/json")
	case "parquet":
		w.Header().Set("Content-Type", "application/octet-stream")
	default:
		w.Header().Set("Content-Type", "text/csv")
	}

	if err := e.execInPod(ctx, pod, req.SQL, req.Format, w); err != nil {
		return "", fmt.Errorf("failed to execute query in pod %s: %w", pod.Name, err)
	}

	return pod.Name, nil
}

func validateSQL(sql string) error {
	if strings.TrimSpace(sql) == "" {
		return fmt.Errorf("SQL query cannot be empty")
	}

	banned := []string{"ATTACH", "INSTALL", "COPY", "EXPORT", "PRAGMA"}
	upper := strings.ToUpper(sql)
	for _, b := range banned {
		if strings.Contains(upper, b+" ") || strings.HasPrefix(upper, b) {
			return fmt.Errorf("statement contains forbidden keyword: %s", b)
		}
	}
	return nil
}

func (e *Executor) selectWarmPod(ctx context.Context, catalog string) (*corev1.Pod, error) {
	pods, err := e.client.CoreV1().Pods(e.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("ducklake.featherman.dev/catalog=%s,warm-pod=true", catalog),
		FieldSelector: fields.OneTermEqualSelector("status.phase", string(corev1.PodRunning)).String(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list warm pods: %w", err)
	}

	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no warm pod")
	}

	// Return the first running pod
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodRunning {
			return &pod, nil
		}
	}

	return nil, fmt.Errorf("no warm pod in running state")
}

func (e *Executor) execInPod(ctx context.Context, pod *corev1.Pod, sql, format string, w io.Writer) error {
	// Build DuckDB command based on format
	var cmd []string
	switch format {
	case "json":
		cmd = []string{"duckdb", "/catalog/duck.db", "-json", "-c", sql}
	case "arrow":
		cmd = []string{"duckdb", "/catalog/duck.db", "-arrow", "-c", sql}
	case "parquet":
		cmd = []string{"duckdb", "/catalog/duck.db", "-parquet", "-c", sql}
	default:
		cmd = []string{"duckdb", "/catalog/duck.db", "-csv", "-c", sql}
	}

	req := e.client.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "duckdb",
			Command:   cmd,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(e.cfg, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create SPDY executor: %w", err)
	}

	return executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: w,
		Stderr: w,
	})
}

func (e *Executor) runEphemeralJob(ctx context.Context, req QueryRequest) (*corev1.Pod, error) {
	// Build command for ephemeral job
	var cmd []string
	switch req.Format {
	case "json":
		cmd = []string{"duckdb", "/catalog/duck.db", "-json", "-c", req.SQL}
	case "arrow":
		cmd = []string{"duckdb", "/catalog/duck.db", "-arrow", "-c", req.SQL}
	case "parquet":
		cmd = []string{"duckdb", "/catalog/duck.db", "-parquet", "-c", req.SQL}
	default:
		cmd = []string{"duckdb", "/catalog/duck.db", "-csv", "-c", req.SQL}
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "query-job-",
			Namespace:    e.namespace,
			Labels: map[string]string{
				"app":                                "featherman-query",
				"ducklake.featherman.dev/catalog":    req.Catalog,
				"ducklake.featherman.dev/query-type": "ephemeral",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:    "duckdb",
					Image:   "datacatering/duckdb:v1.3.0",
					Command: cmd,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("256Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("1000m"),
							corev1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
	}

	createdPod, err := e.client.CoreV1().Pods(e.namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create pod: %w", err)
	}

	// Wait for pod to be running with timeout
	return e.waitForPodRunning(ctx, createdPod)
}

func (e *Executor) waitForPodRunning(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	// Set up a watch for the pod
	watchOptions := metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", pod.Name).String(),
		Watch:         true,
	}

	watcher, err := e.client.CoreV1().Pods(pod.Namespace).Watch(ctx, watchOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to watch pod: %w", err)
	}
	defer watcher.Stop()

	// Wait for pod to be running or failed
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout waiting for pod to be ready: %w", ctx.Err())
		case event := <-watcher.ResultChan():
			if event.Type == watch.Error {
				return nil, fmt.Errorf("error watching pod")
			}

			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}

			switch pod.Status.Phase {
			case corev1.PodRunning:
				return pod, nil
			case corev1.PodFailed:
				return nil, fmt.Errorf("pod failed to start: %s", pod.Status.Reason)
			case corev1.PodSucceeded:
				// Pod completed too quickly, might be an error
				return nil, fmt.Errorf("pod completed before query execution")
			}
		}
	}
}

func (e *Executor) cleanupJob(ctx context.Context, pod *corev1.Pod) {
	// Set a reasonable timeout for cleanup
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err := e.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
		GracePeriodSeconds: &[]int64{0}[0], // Force immediate deletion
	})
	if err != nil {
		// Log error but don't fail - cleanup is best effort
		// In a real implementation, this would use proper logging
		fmt.Printf("Warning: failed to cleanup pod %s: %v\n", pod.Name, err)
	}
}
