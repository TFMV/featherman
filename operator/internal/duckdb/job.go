package duckdb

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// JobConfig represents the configuration for a DuckDB job
type JobConfig struct {
	// Name of the job
	Name string
	// Namespace where the job will run
	Namespace string
	// SQL script to execute
	SQL string
	// CatalogPath is the path to the DuckDB catalog file
	CatalogPath string
	// ObjectStore configuration
	ObjectStore ObjectStoreConfig
	// Resources for the DuckDB container
	Resources corev1.ResourceRequirements
	// ReadOnly indicates if this is a read-only operation
	ReadOnly bool
	// OwnerReference for the job
	OwnerReference *metav1.OwnerReference
}

// ObjectStoreConfig contains S3-compatible storage configuration
type ObjectStoreConfig struct {
	// Endpoint is the S3-compatible endpoint
	Endpoint string
	// Bucket is the S3 bucket name
	Bucket string
	// Region is the S3 region (optional)
	Region string
	// CredentialsSecret references the secret containing credentials
	CredentialsSecret corev1.LocalObjectReference
}

// JobManager handles DuckDB job lifecycle
type JobManager interface {
	// CreateJob creates a new DuckDB job
	CreateJob(ctx context.Context, config JobConfig) (*batchv1.Job, error)
	// DeleteJob deletes a DuckDB job
	DeleteJob(ctx context.Context, namespace, name string) error
	// GetJob gets a DuckDB job
	GetJob(ctx context.Context, namespace, name string) (*batchv1.Job, error)
}

// NewJobConfig creates a new JobConfig with default values
func NewJobConfig(name, namespace string) *JobConfig {
	return &JobConfig{
		Name:      name,
		Namespace: namespace,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		},
	}
}

// CreateJobSpec creates a Job specification for DuckDB
func CreateJobSpec(config JobConfig) (*batchv1.Job, error) {
	if config.Name == "" || config.Namespace == "" {
		return nil, fmt.Errorf("job name and namespace are required")
	}

	// Create init container for flock
	initContainer := corev1.Container{
		Name:  "init-flock",
		Image: "busybox:latest",
		Command: []string{
			"sh",
			"-c",
			fmt.Sprintf("flock %s.lock -c 'touch %s'", config.CatalogPath, config.CatalogPath),
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "catalog",
				MountPath: "/catalog",
			},
		},
	}

	// Create main DuckDB container
	mainContainer := corev1.Container{
		Name:  "duckdb",
		Image: "duckdb/duckdb:latest", // TODO: Make configurable
		Command: []string{
			"duckdb",
			config.CatalogPath,
		},
		Args: []string{
			"-c",
			config.SQL,
		},
		Resources: config.Resources,
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "catalog",
				MountPath: "/catalog",
				ReadOnly:  config.ReadOnly,
			},
		},
		Env: []corev1.EnvVar{
			{
				Name: "AWS_ACCESS_KEY_ID",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: config.ObjectStore.CredentialsSecret,
						Key:                  "access-key",
					},
				},
			},
			{
				Name: "AWS_SECRET_ACCESS_KEY",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: config.ObjectStore.CredentialsSecret,
						Key:                  "secret-key",
					},
				},
			},
			{
				Name:  "AWS_ENDPOINT_URL",
				Value: config.ObjectStore.Endpoint,
			},
			{
				Name:  "AWS_REGION",
				Value: config.ObjectStore.Region,
			},
		},
	}

	// Create the Job specification
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      config.Name,
			Namespace: config.Namespace,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{initContainer},
					Containers:     []corev1.Container{mainContainer},
					RestartPolicy:  corev1.RestartPolicyOnFailure,
					Volumes: []corev1.Volume{
						{
							Name: "catalog",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "catalog-pvc", // TODO: Make configurable
								},
							},
						},
					},
				},
			},
		},
	}

	return job, nil
}
