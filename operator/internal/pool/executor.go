package pool

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rs/zerolog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/TFMV/featherman/operator/internal/logger"
	"github.com/TFMV/featherman/operator/internal/metrics"
	"github.com/TFMV/featherman/operator/internal/retry"
)

// Executor executes SQL queries on warm pods
type Executor struct {
	k8sClient kubernetes.Interface
	config    *rest.Config
	logger    *zerolog.Logger
}

// NewExecutor creates a new executor
func NewExecutor(k8sClient kubernetes.Interface, config *rest.Config, logger *zerolog.Logger) *Executor {
	return &Executor{
		k8sClient: k8sClient,
		config:    config,
		logger:    logger,
	}
}

// ExecuteQuery executes a SQL query on a warm pod
func (e *Executor) ExecuteQuery(ctx context.Context, pod *WarmPod, sql string, timeout time.Duration) (string, error) {
	if e.logger == nil {
		l := logger.FromContext(ctx)
		e.logger = &l
	}

	startTime := time.Now()
	defer func() {
		e.logger.Debug().
			Str("pod", pod.Name).
			Dur("duration", time.Since(startTime)).
			Msg("query execution completed")
	}()

	// Create a timeout context
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Prepare the command
	// Escape single quotes in SQL by doubling them
	escapedSQL := strings.ReplaceAll(sql, "'", "''")
	command := []string{
		"duckdb",
		"/catalog/duck.db",
		"-c",
		escapedSQL,
	}

	// Execute the command
	stdout, stderr, err := e.execInPod(execCtx, pod.Namespace, pod.Name, "duckdb", command)
	if err != nil {
		e.logger.Error().
			Err(err).
			Str("pod", pod.Name).
			Str("stderr", stderr).
			Msg("failed to execute query")
		return "", fmt.Errorf("failed to execute query: %w (stderr: %s)", err, stderr)
	}

	if stderr != "" {
		e.logger.Warn().
			Str("pod", pod.Name).
			Str("stderr", stderr).
			Msg("query produced stderr output")
	}

	return stdout, nil
}

// ExecuteScript executes a SQL script file on a warm pod
func (e *Executor) ExecuteScript(ctx context.Context, pod *WarmPod, scriptPath string, timeout time.Duration) (string, error) {
	startTime := time.Now()
	defer func() {
		e.logger.Debug().
			Str("pod", pod.Name).
			Str("script", scriptPath).
			Dur("duration", time.Since(startTime)).
			Msg("script execution completed")
	}()

	// Create a timeout context
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Execute the script file
	command := []string{
		"duckdb",
		"/catalog/duck.db",
		"<",
		scriptPath,
	}

	stdout, stderr, err := e.execInPod(execCtx, pod.Namespace, pod.Name, "duckdb", command)
	if err != nil {
		e.logger.Error().
			Err(err).
			Str("pod", pod.Name).
			Str("script", scriptPath).
			Str("stderr", stderr).
			Msg("failed to execute script")
		return "", fmt.Errorf("failed to execute script: %w (stderr: %s)", err, stderr)
	}

	if stderr != "" {
		e.logger.Warn().
			Str("pod", pod.Name).
			Str("script", scriptPath).
			Str("stderr", stderr).
			Msg("script produced stderr output")
	}

	return stdout, nil
}

// UploadScript uploads a SQL script to a warm pod
func (e *Executor) UploadScript(ctx context.Context, pod *WarmPod, scriptContent string, scriptPath string) error {
	e.logger.Debug().
		Str("pod", pod.Name).
		Str("path", scriptPath).
		Int("size", len(scriptContent)).
		Msg("uploading script to pod")

	// Create the script file using echo
	// We need to be careful with escaping
	escapedContent := strings.ReplaceAll(scriptContent, "\\", "\\\\")
	escapedContent = strings.ReplaceAll(escapedContent, "\"", "\\\"")
	escapedContent = strings.ReplaceAll(escapedContent, "$", "\\$")
	escapedContent = strings.ReplaceAll(escapedContent, "`", "\\`")

	command := []string{
		"sh",
		"-c",
		fmt.Sprintf("cat > %s << 'EOF'\n%s\nEOF", scriptPath, scriptContent),
	}

	_, stderr, err := e.execInPod(ctx, pod.Namespace, pod.Name, "duckdb", command)
	if err != nil {
		e.logger.Error().
			Err(err).
			Str("pod", pod.Name).
			Str("stderr", stderr).
			Msg("failed to upload script")
		return fmt.Errorf("failed to upload script: %w (stderr: %s)", err, stderr)
	}

	return nil
}

// HealthCheck performs a health check on a warm pod
func (e *Executor) HealthCheck(ctx context.Context, pod *WarmPod) error {
	command := []string{
		"duckdb",
		"/catalog/duck.db",
		"-c",
		"SELECT 1;",
	}

	stdout, stderr, err := e.execInPod(ctx, pod.Namespace, pod.Name, "duckdb", command)
	if err != nil {
		return fmt.Errorf("health check failed: %w (stderr: %s)", err, stderr)
	}

	// Check if we got the expected output
	if !strings.Contains(stdout, "1") {
		return fmt.Errorf("unexpected health check output: %s", stdout)
	}

	return nil
}

// execInPod executes a command in a pod and returns stdout, stderr, and error
func (e *Executor) execInPod(ctx context.Context, namespace, podName, containerName string, command []string) (string, string, error) {
	req := e.k8sClient.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   command,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(e.config, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("failed to create executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  nil,
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	})

	return stdout.String(), stderr.String(), err
}

// StreamQuery executes a query and streams the results
func (e *Executor) StreamQuery(ctx context.Context, pod *WarmPod, sql string, writer io.Writer) error {
	// Escape single quotes in SQL
	escapedSQL := strings.ReplaceAll(sql, "'", "''")
	command := []string{
		"duckdb",
		"/catalog/duck.db",
		"-c",
		escapedSQL,
	}

	req := e.k8sClient.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "duckdb",
			Command:   command,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(e.config, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create executor: %w", err)
	}

	var stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  nil,
		Stdout: writer,
		Stderr: &stderr,
		Tty:    false,
	})

	if err != nil {
		return fmt.Errorf("failed to stream query: %w (stderr: %s)", err, stderr.String())
	}

	if stderr.Len() > 0 {
		e.logger.Warn().
			Str("pod", pod.Name).
			Str("stderr", stderr.String()).
			Msg("query produced stderr output")
	}

	return nil
}

// QueryResult represents the result of a query execution
type QueryResult struct {
	Output   string
	Duration time.Duration
	Error    error
}
