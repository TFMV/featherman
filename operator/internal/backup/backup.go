package backup

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/config"
)

// Manager handles catalog backup operations
type Manager struct {
	k8sClient   kubernetes.Interface
	s3Client    *s3.Client
	podTemplate *config.PodTemplateConfig
}

// NewManager creates a new backup manager
func NewManager(k8sClient kubernetes.Interface, s3Client *s3.Client, podTemplate *config.PodTemplateConfig) *Manager {
	return &Manager{
		k8sClient:   k8sClient,
		s3Client:    s3Client,
		podTemplate: podTemplate,
	}
}

// CreateBackupCronJob creates or updates a CronJob for catalog backups
func (m *Manager) CreateBackupCronJob(catalog *ducklakev1alpha1.DuckLakeCatalog) (*batchv1.CronJob, error) {
	if catalog.Spec.BackupPolicy == nil {
		return nil, fmt.Errorf("backup policy not configured")
	}

	backupScript := fmt.Sprintf(`
		#!/bin/sh
		set -e
		
		# Wait for any active transactions
		flock %s.lock -c '
			# Create backup
			cp %s %s.bak
			
			# Upload to S3
			aws s3 cp %s.bak s3://%s/%s/catalog-%s.db
			
			# Cleanup
			rm %s.bak
		'
	`,
		catalog.Spec.CatalogPath,
		catalog.Spec.CatalogPath,
		catalog.Spec.CatalogPath,
		catalog.Spec.CatalogPath,
		catalog.Spec.ObjectStore.Bucket,
		path.Join("backups", catalog.Name),
		"${NOW}",
		catalog.Spec.CatalogPath,
	)

	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-backup", catalog.Name),
			Namespace: catalog.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(catalog, ducklakev1alpha1.GroupVersion.WithKind("DuckLakeCatalog")),
			},
		},
		Spec: batchv1.CronJobSpec{
			Schedule:          catalog.Spec.BackupPolicy.Schedule,
			ConcurrencyPolicy: batchv1.ForbidConcurrent,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							Containers: []corev1.Container{
								{
									Name:  "backup",
									Image: "amazon/aws-cli:latest",
									Command: []string{
										"/bin/sh",
										"-c",
										fmt.Sprintf("NOW=$(date +%%Y%%m%%d-%%H%%M%%S) && %s", backupScript),
									},
									VolumeMounts: []corev1.VolumeMount{
										{
											Name:      "catalog",
											MountPath: "/catalog",
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
										{
											Name:  "AWS_ENDPOINT_URL",
											Value: catalog.Spec.ObjectStore.Endpoint,
										},
										{
											Name:  "AWS_REGION",
											Value: catalog.Spec.ObjectStore.Region,
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
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Apply pod template configuration
	if m.podTemplate != nil {
		m.podTemplate.ApplyToPodSpec(&cronJob.Spec.JobTemplate.Spec.Template.Spec)
	}

	return cronJob, nil
}

// ListBackups lists all backups for a catalog
func (m *Manager) ListBackups(ctx context.Context, catalog *ducklakev1alpha1.DuckLakeCatalog) ([]types.Object, error) {
	prefix := path.Join("backups", catalog.Name)
	input := &s3.ListObjectsV2Input{
		Bucket: &catalog.Spec.ObjectStore.Bucket,
		Prefix: &prefix,
	}

	result, err := m.s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list backups: %w", err)
	}

	return result.Contents, nil
}

// CleanupOldBackups removes backups older than retention period
func (m *Manager) CleanupOldBackups(ctx context.Context, catalog *ducklakev1alpha1.DuckLakeCatalog) error {
	if catalog.Spec.BackupPolicy == nil || catalog.Spec.BackupPolicy.RetentionDays == 0 {
		return nil
	}

	backups, err := m.ListBackups(ctx, catalog)
	if err != nil {
		return err
	}

	retentionPeriod := time.Duration(catalog.Spec.BackupPolicy.RetentionDays) * 24 * time.Hour
	cutoff := time.Now().Add(-retentionPeriod)

	var objectsToDelete []types.ObjectIdentifier
	for _, backup := range backups {
		if backup.LastModified.Before(cutoff) {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key: backup.Key,
			})
		}
	}

	if len(objectsToDelete) > 0 {
		input := &s3.DeleteObjectsInput{
			Bucket: &catalog.Spec.ObjectStore.Bucket,
			Delete: &types.Delete{
				Objects: objectsToDelete,
			},
		}

		if _, err := m.s3Client.DeleteObjects(ctx, input); err != nil {
			return fmt.Errorf("failed to delete old backups: %w", err)
		}
	}

	return nil
}
