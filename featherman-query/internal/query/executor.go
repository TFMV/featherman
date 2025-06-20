package query

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// Executor selects pods and executes queries
type Executor struct {
	client kubernetes.Interface
	cfg    *rest.Config
}

func NewExecutor(c kubernetes.Interface, cfg *rest.Config) *Executor {
	return &Executor{client: c, cfg: cfg}
}

func (e *Executor) Execute(ctx context.Context, req QueryRequest, w http.ResponseWriter) (string, error) {
	if err := validateSQL(req.SQL); err != nil {
		return "", err
	}
	pod, err := e.selectWarmPod(ctx, req.Catalog)
	if err != nil {
		pod, err = e.runEphemeralJob(ctx, req)
		if err != nil {
			return "", err
		}
		defer e.cleanupJob(ctx, pod)
	}
	if req.Format == "arrow" {
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	} else {
		w.Header().Set("Content-Type", "text/csv")
	}
	if err := e.execInPod(ctx, pod, req.SQL, w); err != nil {
		return "", err
	}
	return pod.Name, nil
}

func validateSQL(sql string) error {
	banned := []string{"ATTACH", "INSTALL", "COPY", "EXPORT"}
	upper := strings.ToUpper(sql)
	for _, b := range banned {
		if strings.Contains(upper, b) {
			return fmt.Errorf("statement contains forbidden keyword: %s", b)
		}
	}
	return nil
}

func (e *Executor) selectWarmPod(ctx context.Context, catalog string) (*corev1.Pod, error) {
	pods, err := e.client.CoreV1().Pods("default").List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("ducklake.featherman.dev/catalog=%s,warm-pod=true", catalog),
	})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no warm pod")
	}
	return &pods.Items[0], nil
}

func (e *Executor) execInPod(ctx context.Context, pod *corev1.Pod, sql string, w io.Writer) error {
	cmd := []string{"duckdb", "/catalog/duck.db", "-c", sql}
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
		}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(e.cfg, "POST", req.URL())
	if err != nil {
		return err
	}
	return executor.Stream(remotecommand.StreamOptions{Stdout: w, Stderr: w})
}

func (e *Executor) runEphemeralJob(ctx context.Context, req QueryRequest) (*corev1.Pod, error) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{GenerateName: "query-job-", Namespace: "default"},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "duckdb", Image: "datacatering/duckdb:v1.3.0", Command: []string{"duckdb", "/catalog/duck.db", "-c", req.SQL}}}}}
	pod, err := e.client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	for {
		p, _ := e.client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if p.Status.Phase == corev1.PodRunning {
			return p, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (e *Executor) cleanupJob(ctx context.Context, pod *corev1.Pod) {
	_ = e.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
}
