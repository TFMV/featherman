package duckdb

import (
	"fmt"
	"path"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/config"
)

// OperationType represents the type of DuckDB operation
type OperationType string

const (
	// OperationTypeRead represents a read-only operation
	OperationTypeRead OperationType = "read"
	// OperationTypeWrite represents a write operation
	OperationTypeWrite OperationType = "write"
)

// Operation represents a DuckDB operation configuration
type Operation struct {
	metav1.TypeMeta
	metav1.ObjectMeta
	Type          OperationType
	SQL           string
	Table         *ducklakev1alpha1.DuckLakeTable
	Catalog       *ducklakev1alpha1.DuckLakeCatalog
	PodTemplate   *config.PodTemplateConfig
	JobNameSuffix string
}

// DeepCopyObject implements runtime.Object
func (o *Operation) DeepCopyObject() runtime.Object {
	if o == nil {
		return nil
	}
	out := new(Operation)
	o.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies all properties of this object into another object of the same type
func (o *Operation) DeepCopyInto(out *Operation) {
	*out = *o
	out.TypeMeta = o.TypeMeta
	o.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	if o.Table != nil {
		out.Table = o.Table.DeepCopy()
	}
	if o.Catalog != nil {
		out.Catalog = o.Catalog.DeepCopy()
	}
	if o.PodTemplate != nil {
		out.PodTemplate = o.PodTemplate.DeepCopy()
	}
}

// JobConfig contains the configuration for a DuckDB job
type JobConfig struct {
	ReadOnly        bool
	Resources       corev1.ResourceRequirements
	SecurityContext *corev1.SecurityContext
	InitContainers  []corev1.Container
	Sidecars        []corev1.Container
	Volumes         []corev1.Volume
	VolumeMounts    []corev1.VolumeMount
	EnvFrom         []corev1.EnvFromSource
	Env             []corev1.EnvVar
}

// SetResources sets the resource requirements
func (c *JobConfig) SetResources(resources corev1.ResourceRequirements) {
	c.Resources = resources
}

// SetSecurityContext sets the security context
func (c *JobConfig) SetSecurityContext(securityContext *corev1.SecurityContext) {
	c.SecurityContext = securityContext
}

// AddVolumes adds volumes to the configuration
func (c *JobConfig) AddVolumes(volumes []corev1.Volume) {
	c.Volumes = append(c.Volumes, volumes...)
}

// AddVolumeMounts adds volume mounts to the configuration
func (c *JobConfig) AddVolumeMounts(mounts []corev1.VolumeMount) {
	c.VolumeMounts = append(c.VolumeMounts, mounts...)
}

// AddEnv adds environment variables to the configuration
func (c *JobConfig) AddEnv(env []corev1.EnvVar) {
	c.Env = append(c.Env, env...)
}

// AddEnvFrom adds environment from sources to the configuration
func (c *JobConfig) AddEnvFrom(envFrom []corev1.EnvFromSource) {
	c.EnvFrom = append(c.EnvFrom, envFrom...)
}

// DefaultReadJobConfig returns the default configuration for read jobs
func DefaultReadJobConfig() *JobConfig {
	return &JobConfig{
		ReadOnly: true,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		},
		SecurityContext: &corev1.SecurityContext{
			ReadOnlyRootFilesystem: &[]bool{true}[0],
			RunAsNonRoot:           &[]bool{true}[0],
			RunAsUser:              &[]int64{1000}[0],
		},
	}
}

// DefaultWriteJobConfig returns the default configuration for write jobs
func DefaultWriteJobConfig() *JobConfig {
	return &JobConfig{
		ReadOnly: false,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("4Gi"),
			},
		},
		SecurityContext: &corev1.SecurityContext{
			ReadOnlyRootFilesystem: &[]bool{false}[0],
			RunAsNonRoot:           &[]bool{true}[0],
			RunAsUser:              &[]int64{1000}[0],
		},
	}
}

// JobManager manages DuckDB job operations
type JobManager struct {
	k8sClient kubernetes.Interface
}

// NewJobManager creates a new job manager
func NewJobManager(k8sClient kubernetes.Interface) *JobManager {
	return &JobManager{
		k8sClient: k8sClient,
	}
}

// CreateJob creates a Kubernetes Job for a DuckDB operation
func (m *JobManager) CreateJob(op *Operation) (*batchv1.Job, error) {
	if op == nil {
		return nil, fmt.Errorf("operation cannot be nil")
	}

	return op.CreateJob()
}

// CreateJob creates a Kubernetes Job for a DuckDB operation
func (op *Operation) CreateJob() (*batchv1.Job, error) {
	// Determine job configuration based on operation type
	var jobConfig *JobConfig
	switch op.Type {
	case OperationTypeRead:
		jobConfig = DefaultReadJobConfig()
	case OperationTypeWrite:
		jobConfig = DefaultWriteJobConfig()
	default:
		return nil, fmt.Errorf("unsupported operation type: %s", op.Type)
	}

	// Apply pod template configuration
	if op.PodTemplate != nil {
		if err := op.PodTemplate.ApplyToJobConfig(jobConfig); err != nil {
			return nil, fmt.Errorf("failed to apply pod template: %w", err)
		}
	}

	// Get object store environment variables
	objectStoreEnv := op.GetObjectStoreEnv()
	jobConfig.AddEnv(objectStoreEnv)

	// Create the job
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s-%s", op.Table.Name, string(op.Type), op.JobNameSuffix),
			Namespace: op.Table.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":        "featherman",
				"app.kubernetes.io/component":   "duckdb",
				"app.kubernetes.io/operation":   string(op.Type),
				"ducklake.featherman.dev/table": op.Table.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &[]int32{3}[0],
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":        "featherman",
						"app.kubernetes.io/component":   "duckdb",
						"app.kubernetes.io/operation":   string(op.Type),
						"ducklake.featherman.dev/table": op.Table.Name,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:  "duckdb",
							Image: "datacatering/duckdb:v1.3.0",
							Command: []string{
								"/duckdb",
								path.Join("/catalog", op.Catalog.Spec.CatalogPath),
								"-c",
								op.SQL,
							},
							Resources:       jobConfig.Resources,
							SecurityContext: jobConfig.SecurityContext,
							VolumeMounts: append([]corev1.VolumeMount{
								{
									Name:      "catalog",
									MountPath: "/catalog",
									ReadOnly:  jobConfig.ReadOnly,
								},
							}, jobConfig.VolumeMounts...),
							Env:     jobConfig.Env,
							EnvFrom: jobConfig.EnvFrom,
						},
					},
					InitContainers: jobConfig.InitContainers,
					Volumes: append([]corev1.Volume{
						{
							Name: "catalog",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: fmt.Sprintf("%s-catalog", op.Catalog.Name),
									ReadOnly:  jobConfig.ReadOnly,
								},
							},
						},
					}, jobConfig.Volumes...),
				},
			},
		},
	}

	return job, nil
}

// GetObjectStoreEnv returns environment variables for object store configuration
func (op *Operation) GetObjectStoreEnv() []corev1.EnvVar {
	var objectStore *ducklakev1alpha1.ObjectStoreSpec
	if op.Table.Spec.ObjectStore != nil {
		objectStore = op.Table.Spec.ObjectStore
	} else {
		objectStore = &op.Catalog.Spec.ObjectStore
	}

	return []corev1.EnvVar{
		{
			Name:  "DUCKDB_S3_ENDPOINT",
			Value: objectStore.Endpoint,
		},
		{
			Name:  "DUCKDB_S3_REGION",
			Value: objectStore.Region,
		},
		{
			Name: "DUCKDB_S3_ACCESS_KEY_ID",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: objectStore.CredentialsSecret.Name,
					},
					Key: objectStore.CredentialsSecret.AccessKeyField,
				},
			},
		},
		{
			Name: "DUCKDB_S3_SECRET_ACCESS_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: objectStore.CredentialsSecret.Name,
					},
					Key: objectStore.CredentialsSecret.SecretKeyField,
				},
			},
		},
	}
}

// IsJobComplete checks if a job is complete
func IsJobComplete(job *batchv1.Job) bool {
	return job != nil && job.Status.Succeeded > 0
}

// IsJobFailed checks if a job has failed
func IsJobFailed(job *batchv1.Job) bool {
	return job != nil && job.Status.Failed > 0
}

// CreateBackupJob creates a job to backup a catalog
func CreateBackupJob(catalog *ducklakev1alpha1.DuckLakeCatalog, backupPath string) (*batchv1.Job, error) {
	// Generate backup script
	backupScript := fmt.Sprintf(`#!/bin/sh
set -e

# Wait for catalog file to be available
while [ ! -f %s ]; do
  echo "Waiting for catalog file..."
  sleep 1
done

# Create backup using DuckDB CLI
/duckdb %s -c ".backup /tmp/backup.duckdb"

# Upload to S3
aws s3 cp /tmp/backup.duckdb s3://%s/%s \
	--endpoint-url %s \
	--region %s
`,
		path.Join("/catalog", catalog.Spec.CatalogPath),
		path.Join("/catalog", catalog.Spec.CatalogPath),
		catalog.Spec.ObjectStore.Bucket,
		backupPath,
		catalog.Spec.ObjectStore.Endpoint,
		catalog.Spec.ObjectStore.Region)

	// Create job
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-backup-%s", catalog.Name, time.Now().Format("20060102150405")),
			Namespace: catalog.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "featherman",
				"app.kubernetes.io/component": "backup",
				"app.kubernetes.io/part-of":   "ducklake",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &[]int32{3}[0],
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":      "featherman",
						"app.kubernetes.io/component": "backup",
						"app.kubernetes.io/part-of":   "ducklake",
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &[]bool{true}[0],
						RunAsUser:    &[]int64{1000}[0],
						FSGroup:      &[]int64{1000}[0],
					},
					Containers: []corev1.Container{
						{
							Name:  "backup",
							Image: "datacatering/duckdb:v1.3.0",
							Command: []string{
								"sh",
								"-c",
								backupScript,
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("1Gi"),
								},
							},
							SecurityContext: &corev1.SecurityContext{
								ReadOnlyRootFilesystem: &[]bool{false}[0],
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "catalog",
									MountPath: "/catalog",
									ReadOnly:  true,
								},
								{
									Name:      "tmp",
									MountPath: "/tmp",
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  "AWS_ENDPOINT_URL",
									Value: catalog.Spec.ObjectStore.Endpoint,
								},
								{
									Name:  "AWS_REGION",
									Value: catalog.Spec.ObjectStore.Region,
								},
								{
									Name: "AWS_ACCESS_KEY_ID",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: catalog.Spec.ObjectStore.CredentialsSecret.Name,
											},
											Key: catalog.Spec.ObjectStore.CredentialsSecret.AccessKeyField,
										},
									},
								},
								{
									Name: "AWS_SECRET_ACCESS_KEY",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: catalog.Spec.ObjectStore.CredentialsSecret.Name,
											},
											Key: catalog.Spec.ObjectStore.CredentialsSecret.SecretKeyField,
										},
									},
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "catalog",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: fmt.Sprintf("%s-catalog", catalog.Name),
									ReadOnly:  true,
								},
							},
						},
						{
							Name: "tmp",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
		},
	}

	return job, nil
}
