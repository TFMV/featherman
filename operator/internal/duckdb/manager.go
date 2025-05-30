package duckdb

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// jobManager implements JobManager
type jobManager struct {
	client kubernetes.Interface
}

// NewJobManager creates a new JobManager instance
func NewJobManager(client kubernetes.Interface) JobManager {
	return &jobManager{
		client: client,
	}
}

// CreateJob implements JobManager
func (m *jobManager) CreateJob(ctx context.Context, config JobConfig) (*batchv1.Job, error) {
	logger := log.FromContext(ctx)
	logger.Info("creating DuckDB job", "name", config.Name, "namespace", config.Namespace)

	job, err := CreateJobSpec(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create job spec: %w", err)
	}

	// Add owner reference if provided
	if config.OwnerReference != nil {
		job.OwnerReferences = []metav1.OwnerReference{*config.OwnerReference}
	}

	// Create the job
	created, err := m.client.BatchV1().Jobs(config.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create job: %w", err)
	}

	logger.Info("created DuckDB job successfully", "name", created.Name)
	return created, nil
}

// DeleteJob implements JobManager
func (m *jobManager) DeleteJob(ctx context.Context, namespace, name string) error {
	logger := log.FromContext(ctx)
	logger.Info("deleting DuckDB job", "name", name, "namespace", namespace)

	propagation := metav1.DeletePropagationForeground
	err := m.client.BatchV1().Jobs(namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	if err != nil {
		return fmt.Errorf("failed to delete job: %w", err)
	}

	logger.Info("deleted DuckDB job successfully", "name", name)
	return nil
}

// GetJob implements JobManager
func (m *jobManager) GetJob(ctx context.Context, namespace, name string) (*batchv1.Job, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info("getting DuckDB job", "name", name, "namespace", namespace)

	job, err := m.client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	return job, nil
}

// IsJobComplete checks if a job has completed successfully
func IsJobComplete(job *batchv1.Job) bool {
	if job == nil {
		return false
	}

	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == "True" {
			return true
		}
	}
	return false
}

// IsJobFailed checks if a job has failed
func IsJobFailed(job *batchv1.Job) bool {
	if job == nil {
		return false
	}

	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == "True" {
			return true
		}
	}
	return false
}
