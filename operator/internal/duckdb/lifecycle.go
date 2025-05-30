package duckdb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/TFMV/featherman/operator/internal/logger"
	"github.com/TFMV/featherman/operator/internal/retry"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// JobPhase represents the phase of a job
type JobPhase string

const (
	// Job phases
	JobPhasePending  JobPhase = "Pending"
	JobPhaseRunning  JobPhase = "Running"
	JobPhaseComplete JobPhase = "Complete"
	JobPhaseFailed   JobPhase = "Failed"

	// JobHistoryLimit is the number of completed/failed jobs to keep
	JobHistoryLimit = 3

	// JobTimeoutDuration is the default timeout for jobs
	JobTimeoutDuration = 30 * time.Minute

	// JobCleanupAge is the age after which completed/failed jobs are cleaned up
	JobCleanupAge = 24 * time.Hour
)

// LifecycleManager manages the lifecycle of DuckDB jobs
type LifecycleManager struct {
	client      client.Client
	kubeClient  kubernetes.Interface
	retryConfig retry.RetryConfig
}

// NewLifecycleManager creates a new job lifecycle manager
func NewLifecycleManager(client client.Client, kubeClient kubernetes.Interface) *LifecycleManager {
	return &LifecycleManager{
		client:      client,
		kubeClient:  kubeClient,
		retryConfig: retry.DefaultRetryConfig,
	}
}

// JobState represents the state of a job
type JobState struct {
	Phase          JobPhase
	StartTime      *metav1.Time
	CompletionTime *metav1.Time
	Failed         bool
	Active         bool
	Succeeded      bool
	Message        string
	Conditions     []batchv1.JobCondition
}

// GetJobState returns the current state of a job
func (m *LifecycleManager) GetJobState(ctx context.Context, job *batchv1.Job) (*JobState, error) {
	l := logger.FromContext(ctx)

	// Get fresh job state
	if err := m.client.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil {
		l.Error().Err(err).
			Str("job", job.Name).
			Str("namespace", job.Namespace).
			Msg("failed to get job state")
		return nil, fmt.Errorf("failed to get job state: %w", err)
	}

	state := &JobState{
		StartTime:      job.Status.StartTime,
		CompletionTime: job.Status.CompletionTime,
		Failed:         IsJobFailed(job),
		Active:         job.Status.Active > 0,
		Succeeded:      IsJobComplete(job),
		Conditions:     job.Status.Conditions,
	}

	// Determine phase
	switch {
	case state.Succeeded:
		state.Phase = JobPhaseComplete
	case state.Failed:
		state.Phase = JobPhaseFailed
	case state.Active:
		state.Phase = JobPhaseRunning
	default:
		state.Phase = JobPhasePending
	}

	// Check for timeout
	if state.Active && state.StartTime != nil {
		if time.Since(state.StartTime.Time) > JobTimeoutDuration {
			state.Message = "Job timed out"
			state.Failed = true
			state.Phase = JobPhaseFailed
		}
	}

	// Get message from conditions
	if state.Failed {
		if cond := GetJobCondition(job, batchv1.JobFailed); cond != nil {
			state.Message = cond.Message
		} else {
			state.Message = "Job failed"
		}
	} else if state.Succeeded {
		state.Message = "Job completed successfully"
	} else if state.Active {
		state.Message = "Job is running"
	} else {
		state.Message = "Job is pending"
	}

	return state, nil
}

// GetJobCondition returns the condition of the given type from the job status
func GetJobCondition(job *batchv1.Job, condType batchv1.JobConditionType) *batchv1.JobCondition {
	for i := range job.Status.Conditions {
		c := &job.Status.Conditions[i]
		if c.Type == condType {
			return c
		}
	}
	return nil
}

// CleanupJobs removes old completed/failed jobs
func (m *LifecycleManager) CleanupJobs(ctx context.Context, namespace string, selector labels.Selector) error {
	l := logger.FromContext(ctx)

	// List all jobs
	var jobs batchv1.JobList
	if err := m.client.List(ctx, &jobs, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		l.Error().Err(err).
			Str("namespace", namespace).
			Msg("failed to list jobs for cleanup")
		return fmt.Errorf("failed to list jobs: %w", err)
	}

	// Sort jobs by creation timestamp
	completedJobs := make([]*batchv1.Job, 0)
	failedJobs := make([]*batchv1.Job, 0)

	for i := range jobs.Items {
		job := &jobs.Items[i]
		if IsJobComplete(job) {
			completedJobs = append(completedJobs, job)
		} else if IsJobFailed(job) {
			failedJobs = append(failedJobs, job)
		}
	}

	// Keep only JobHistoryLimit most recent completed/failed jobs
	jobsToDelete := make([]*batchv1.Job, 0)

	if len(completedJobs) > JobHistoryLimit {
		jobsToDelete = append(jobsToDelete, completedJobs[JobHistoryLimit:]...)
	}
	if len(failedJobs) > JobHistoryLimit {
		jobsToDelete = append(jobsToDelete, failedJobs[JobHistoryLimit:]...)
	}

	// Delete old jobs
	for _, job := range jobsToDelete {
		// Skip if job is not old enough
		if job.Status.CompletionTime != nil && time.Since(job.Status.CompletionTime.Time) < JobCleanupAge {
			continue
		}

		if err := m.client.Delete(ctx, job); err != nil && !errors.IsNotFound(err) {
			l.Error().Err(err).
				Str("job", job.Name).
				Str("namespace", job.Namespace).
				Msg("failed to delete old job")
			continue
		}

		l.Info().
			Str("job", job.Name).
			Str("namespace", job.Namespace).
			Time("completionTime", job.Status.CompletionTime.Time).
			Msg("deleted old job")
	}

	return nil
}

// WaitForJobCompletion waits for a job to complete with retries
func (m *LifecycleManager) WaitForJobCompletion(ctx context.Context, job *batchv1.Job) (*JobState, error) {
	l := logger.FromContext(ctx)

	var state *JobState
	var err error

	op := func(ctx context.Context) error {
		state, err = m.GetJobState(ctx, job)
		if err != nil {
			return err
		}

		if state.Active {
			return fmt.Errorf("job still running")
		}

		if state.Failed {
			return fmt.Errorf("job failed: %s", state.Message)
		}

		if !state.Succeeded {
			return fmt.Errorf("job in unexpected state")
		}

		return nil
	}

	if err := retry.Do(ctx, op, m.retryConfig); err != nil {
		l.Error().Err(err).
			Str("job", job.Name).
			Str("namespace", job.Namespace).
			Msg("failed to wait for job completion")
		return state, fmt.Errorf("failed to wait for job completion: %w", err)
	}

	return state, nil
}

// GetJobPods returns all pods associated with a job
func (m *LifecycleManager) GetJobPods(ctx context.Context, job *batchv1.Job) ([]corev1.Pod, error) {
	l := logger.FromContext(ctx)

	var pods corev1.PodList
	if err := m.client.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{"job-name": job.Name}); err != nil {
		l.Error().Err(err).
			Str("job", job.Name).
			Str("namespace", job.Namespace).
			Msg("failed to list job pods")
		return nil, fmt.Errorf("failed to list job pods: %w", err)
	}

	return pods.Items, nil
}

// GetJobLogs returns logs from all pods associated with a job
func (m *LifecycleManager) GetJobLogs(ctx context.Context, job *batchv1.Job) (map[string]string, error) {
	l := logger.FromContext(ctx)

	pods, err := m.GetJobPods(ctx, job)
	if err != nil {
		return nil, err
	}

	logs := make(map[string]string)
	for _, pod := range pods {
		req := m.kubeClient.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{})
		stream, err := req.Stream(ctx)
		if err != nil {
			l.Error().Err(err).
				Str("pod", pod.Name).
				Str("namespace", pod.Namespace).
				Msg("failed to get pod logs")
			continue
		}
		defer stream.Close()

		// Read logs
		buf := new(bytes.Buffer)
		if _, err := io.Copy(buf, stream); err != nil {
			l.Error().Err(err).
				Str("pod", pod.Name).
				Str("namespace", pod.Namespace).
				Msg("failed to read pod logs")
			continue
		}

		logs[pod.Name] = buf.String()
	}

	return logs, nil
}

// IsJobActive returns true if the job is still running
func IsJobActive(job *batchv1.Job) bool {
	return job.Status.Active > 0
}

// IsJobTimedOut returns true if the job has exceeded its timeout
func IsJobTimedOut(job *batchv1.Job) bool {
	if job.Status.StartTime == nil {
		return false
	}
	return time.Since(job.Status.StartTime.Time) > JobTimeoutDuration
}

// GetJobMessage returns a human-readable message describing the job's state
func GetJobMessage(job *batchv1.Job) string {
	if IsJobComplete(job) {
		return "Job completed successfully"
	}
	if IsJobFailed(job) {
		if cond := GetJobCondition(job, batchv1.JobFailed); cond != nil {
			return cond.Message
		}
		return "Job failed"
	}
	if IsJobActive(job) {
		return "Job is running"
	}
	if IsJobTimedOut(job) {
		return "Job timed out"
	}
	return "Job is pending"
}
