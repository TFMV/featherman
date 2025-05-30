package backup

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/logger"
	"github.com/TFMV/featherman/operator/internal/retry"
)

const (
	// DefaultBackupRetention is the default number of days to retain backups
	DefaultBackupRetention = 30 * 24 * time.Hour

	// DefaultBackupSchedule is the default cron schedule for backups (daily at 2am)
	DefaultBackupSchedule = "0 2 * * *"
)

// BackupManager manages catalog backups
type BackupManager struct {
	client      client.Client
	s3Client    *s3.Client
	cron        *cron.Cron
	logger      *zerolog.Logger
	retryConfig retry.RetryConfig
}

// NewBackupManager creates a new backup manager
func NewBackupManager(client client.Client, s3Client *s3.Client, logger *zerolog.Logger) *BackupManager {
	return &BackupManager{
		client:      client,
		s3Client:    s3Client,
		cron:        cron.New(),
		logger:      logger,
		retryConfig: retry.DefaultRetryConfig,
	}
}

// Start starts the backup manager
func (m *BackupManager) Start(ctx context.Context) error {
	// Start cron scheduler
	m.cron.Start()

	// List all catalogs
	var catalogs ducklakev1alpha1.DuckLakeCatalogList
	if err := m.client.List(ctx, &catalogs); err != nil {
		return fmt.Errorf("failed to list catalogs: %w", err)
	}

	// Schedule backups for each catalog
	for i := range catalogs.Items {
		catalog := &catalogs.Items[i]
		if err := m.ScheduleBackup(ctx, catalog); err != nil {
			m.logger.Error().
				Err(err).
				Str("catalog", catalog.Name).
				Str("namespace", catalog.Namespace).
				Msg("failed to schedule backup")
		}
	}

	return nil
}

// Stop stops the backup manager
func (m *BackupManager) Stop() {
	m.cron.Stop()
}

// ScheduleBackup schedules backups for a catalog
func (m *BackupManager) ScheduleBackup(ctx context.Context, catalog *ducklakev1alpha1.DuckLakeCatalog) error {
	l := logger.FromContext(ctx)

	// Skip if no backup policy
	if catalog.Spec.BackupPolicy == nil {
		l.Info().
			Str("catalog", catalog.Name).
			Str("namespace", catalog.Namespace).
			Msg("no backup policy configured")
		return nil
	}

	// Add cron job
	_, err := m.cron.AddFunc(catalog.Spec.BackupPolicy.Schedule, func() {
		if err := m.BackupCatalog(context.Background(), catalog); err != nil {
			l.Error().
				Err(err).
				Str("catalog", catalog.Name).
				Str("namespace", catalog.Namespace).
				Msg("failed to backup catalog")
		}
	})
	if err != nil {
		return fmt.Errorf("failed to schedule backup: %w", err)
	}

	l.Info().
		Str("catalog", catalog.Name).
		Str("namespace", catalog.Namespace).
		Str("schedule", catalog.Spec.BackupPolicy.Schedule).
		Int("retentionDays", catalog.Spec.BackupPolicy.RetentionDays).
		Msg("scheduled backup")

	return nil
}

// BackupCatalog performs a backup of a catalog
func (m *BackupManager) BackupCatalog(ctx context.Context, catalog *ducklakev1alpha1.DuckLakeCatalog) error {
	l := logger.FromContext(ctx)

	// Get fresh catalog state
	if err := m.client.Get(ctx, types.NamespacedName{
		Name:      catalog.Name,
		Namespace: catalog.Namespace,
	}, catalog); err != nil {
		if errors.IsNotFound(err) {
			return nil // Catalog was deleted
		}
		return fmt.Errorf("failed to get catalog: %w", err)
	}

	// Skip if no backup policy
	if catalog.Spec.BackupPolicy == nil {
		return nil
	}

	// Generate backup name
	timestamp := time.Now().UTC().Format("20060102-150405")
	backupName := fmt.Sprintf("%s-%s.duckdb", catalog.Name, timestamp)
	backupPath := path.Join("backups", catalog.Namespace, catalog.Name, backupName)

	// Create backup job
	job, err := CreateBackupJob(catalog, backupPath)
	if err != nil {
		l.Error().
			Err(err).
			Str("catalog", catalog.Name).
			Str("namespace", catalog.Namespace).
			Msg("failed to create backup job")
		return fmt.Errorf("failed to create backup job: %w", err)
	}

	// Create the job
	if err := m.client.Create(ctx, job); err != nil {
		l.Error().
			Err(err).
			Str("catalog", catalog.Name).
			Str("namespace", catalog.Namespace).
			Msg("failed to create backup job")
		return fmt.Errorf("failed to create backup job: %w", err)
	}

	// Wait for job completion with retries
	waitOp := func(ctx context.Context) error {
		if err := m.client.Get(ctx, types.NamespacedName{
			Name:      job.Name,
			Namespace: job.Namespace,
		}, job); err != nil {
			return fmt.Errorf("failed to get job: %w", err)
		}

		if job.Status.Succeeded > 0 {
			return nil
		}

		if job.Status.Failed > 0 {
			return fmt.Errorf("backup job failed")
		}

		return fmt.Errorf("job still running")
	}

	if err := retry.Do(ctx, waitOp, m.retryConfig); err != nil {
		l.Error().
			Err(err).
			Str("catalog", catalog.Name).
			Str("namespace", catalog.Namespace).
			Str("job", job.Name).
			Msg("failed to wait for backup job")
		return fmt.Errorf("failed to wait for backup job: %w", err)
	}

	// Update catalog status
	catalog.Status.LastBackup = &metav1.Time{Time: time.Now()}
	if err := m.client.Status().Update(ctx, catalog); err != nil {
		l.Error().
			Err(err).
			Str("catalog", catalog.Name).
			Str("namespace", catalog.Namespace).
			Msg("failed to update catalog status")
		return fmt.Errorf("failed to update catalog status: %w", err)
	}

	l.Info().
		Str("catalog", catalog.Name).
		Str("namespace", catalog.Namespace).
		Str("backup", backupPath).
		Msg("backup completed successfully")

	return nil
}

// CleanupOldBackups removes backups older than retention period
func (m *BackupManager) CleanupOldBackups(ctx context.Context, catalog *ducklakev1alpha1.DuckLakeCatalog) error {
	l := logger.FromContext(ctx)

	// Skip if no backup policy
	if catalog.Spec.BackupPolicy == nil {
		return nil
	}

	// Calculate retention period
	retentionDays := catalog.Spec.BackupPolicy.RetentionDays
	if retentionDays <= 0 {
		retentionDays = 7 // Default from CRD
	}

	// List backups
	prefix := path.Join("backups", catalog.Namespace, catalog.Name)
	input := &s3.ListObjectsV2Input{
		Bucket: &catalog.Spec.ObjectStore.Bucket,
		Prefix: &prefix,
	}

	var objectsToDelete []s3types.ObjectIdentifier
	cutoff := time.Now().Add(-time.Duration(retentionDays) * 24 * time.Hour)

	paginator := s3.NewListObjectsV2Paginator(m.s3Client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			l.Error().
				Err(err).
				Str("catalog", catalog.Name).
				Str("namespace", catalog.Namespace).
				Str("prefix", prefix).
				Msg("failed to list backups")
			return fmt.Errorf("failed to list backups: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.LastModified.Before(cutoff) {
				objectsToDelete = append(objectsToDelete, s3types.ObjectIdentifier{
					Key: obj.Key,
				})
				l.Debug().
					Str("catalog", catalog.Name).
					Str("namespace", catalog.Namespace).
					Str("key", *obj.Key).
					Time("lastModified", *obj.LastModified).
					Msg("marking backup for deletion")
			}
		}
	}

	// Delete old backups
	if len(objectsToDelete) > 0 {
		deleteInput := &s3.DeleteObjectsInput{
			Bucket: &catalog.Spec.ObjectStore.Bucket,
			Delete: &s3types.Delete{
				Objects: objectsToDelete,
			},
		}

		output, err := m.s3Client.DeleteObjects(ctx, deleteInput)
		if err != nil {
			l.Error().
				Err(err).
				Str("catalog", catalog.Name).
				Str("namespace", catalog.Namespace).
				Int("count", len(objectsToDelete)).
				Msg("failed to delete old backups")
			return fmt.Errorf("failed to delete old backups: %w", err)
		}

		if len(output.Errors) > 0 {
			for _, err := range output.Errors {
				l.Error().
					Str("catalog", catalog.Name).
					Str("namespace", catalog.Namespace).
					Str("key", *err.Key).
					Str("code", *err.Code).
					Str("message", *err.Message).
					Msg("failed to delete backup")
			}
			return fmt.Errorf("failed to delete some backups")
		}

		l.Info().
			Str("catalog", catalog.Name).
			Str("namespace", catalog.Namespace).
			Int("count", len(objectsToDelete)).
			Int("retentionDays", retentionDays).
			Msg("cleaned up old backups")
	} else {
		l.Debug().
			Str("catalog", catalog.Name).
			Str("namespace", catalog.Namespace).
			Int("retentionDays", retentionDays).
			Msg("no old backups to clean up")
	}

	return nil
}
