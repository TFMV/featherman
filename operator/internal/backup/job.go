package backup

import (
	"fmt"
	"path"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
)

const (
	// Default resource requirements for backup jobs
	defaultCPURequest    = "100m"
	defaultCPULimit      = "500m"
	defaultMemoryRequest = "256Mi"
	defaultMemoryLimit   = "512Mi"
)

// CreateBackupJob creates a job to backup a catalog
func CreateBackupJob(catalog *ducklakev1alpha1.DuckLakeCatalog, backupPath string) (*batchv1.Job, error) {
	// Generate backup script with safety checks and proper cleanup
	backupScript := fmt.Sprintf(`#!/bin/sh
set -euo pipefail

# Function to cleanup temporary files
cleanup() {
    echo "Cleaning up temporary files..."
    rm -f /tmp/backup.duckdb /tmp/backup.duckdb-shm /tmp/backup.duckdb-wal
}

# Ensure cleanup runs on exit
trap cleanup EXIT

# Validate catalog file exists and is readable
if [ ! -f %s ]; then
    echo "Error: Catalog file not found at %s"
    exit 1
fi

if [ ! -r %s ]; then
    echo "Error: Catalog file not readable at %s"
    exit 1
fi

echo "Starting backup of catalog %s/%s..."

# Create backup with write-ahead-log sync
echo "Creating backup copy..."
cp %s /tmp/backup.duckdb
if [ -f %s-shm ]; then
    cp %s-shm /tmp/backup.duckdb-shm
fi
if [ -f %s-wal ]; then
    cp %s-wal /tmp/backup.duckdb-wal
fi

# Verify backup files
echo "Verifying backup files..."
if [ ! -f /tmp/backup.duckdb ]; then
    echo "Error: Backup file creation failed"
    exit 1
fi

# Upload to S3 with verification
echo "Uploading to S3..."
aws s3 cp /tmp/backup.duckdb s3://%s/%s \
    --endpoint-url %s \
    --region %s

# Verify upload
echo "Verifying S3 upload..."
aws s3api head-object \
    --bucket %s \
    --key %s \
    --endpoint-url %s \
    --region %s

echo "Backup completed successfully"
`,
		catalog.Spec.CatalogPath,
		catalog.Spec.CatalogPath,
		catalog.Spec.CatalogPath,
		catalog.Spec.CatalogPath,
		catalog.Namespace,
		catalog.Name,
		catalog.Spec.CatalogPath,
		catalog.Spec.CatalogPath,
		catalog.Spec.CatalogPath,
		catalog.Spec.CatalogPath,
		catalog.Spec.CatalogPath,
		catalog.Spec.ObjectStore.Bucket,
		backupPath,
		catalog.Spec.ObjectStore.Endpoint,
		catalog.Spec.ObjectStore.Region,
		catalog.Spec.ObjectStore.Bucket,
		backupPath,
		catalog.Spec.ObjectStore.Endpoint,
		catalog.Spec.ObjectStore.Region,
	)

	// Create job with proper configuration
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-backup-", catalog.Name),
			Namespace:    catalog.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":          "featherman",
				"app.kubernetes.io/component":     "backup",
				"app.kubernetes.io/instance":      catalog.Name,
				"app.kubernetes.io/managed-by":    "featherman-operator",
				"ducklake.featherman.dev/catalog": catalog.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         ducklakev1alpha1.GroupVersion.String(),
					Kind:               "DuckLakeCatalog",
					Name:               catalog.Name,
					UID:                catalog.UID,
					Controller:         &[]bool{true}[0],
					BlockOwnerDeletion: &[]bool{true}[0],
				},
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &[]int32{3}[0],
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":          "featherman",
						"app.kubernetes.io/component":     "backup",
						"app.kubernetes.io/instance":      catalog.Name,
						"app.kubernetes.io/managed-by":    "featherman-operator",
						"ducklake.featherman.dev/catalog": catalog.Name,
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
							Image: "amazon/aws-cli:2.13.0",
							Command: []string{
								"sh",
								"-c",
								backupScript,
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(defaultCPURequest),
									corev1.ResourceMemory: resource.MustParse(defaultMemoryRequest),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(defaultCPULimit),
									corev1.ResourceMemory: resource.MustParse(defaultMemoryLimit),
								},
							},
							SecurityContext: &corev1.SecurityContext{
								ReadOnlyRootFilesystem: &[]bool{true}[0],
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "catalog",
									MountPath: path.Dir(catalog.Spec.CatalogPath),
									ReadOnly:  true,
								},
								{
									Name:      "tmp",
									MountPath: "/tmp",
								},
							},
							Env: []corev1.EnvVar{
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
								EmptyDir: &corev1.EmptyDirVolumeSource{
									Medium: corev1.StorageMediumMemory,
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
