package controller

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/backup"
	"github.com/TFMV/featherman/operator/internal/logger"
	"github.com/TFMV/featherman/operator/internal/metrics"
	"github.com/TFMV/featherman/operator/internal/retry"
	"github.com/TFMV/featherman/operator/internal/storage"
)

// DuckLakeCatalogReconciler reconciles a DuckLakeCatalog object
type DuckLakeCatalogReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Recorder    record.EventRecorder
	Logger      *zerolog.Logger
	RetryConfig retry.RetryConfig
	Backup      *backup.BackupManager
	Storage     storage.ObjectStore
}

// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklakecatalogs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklakecatalogs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklakecatalogs/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs;cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch

// Reconcile handles DuckLakeCatalog resources
func (r *DuckLakeCatalogReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := logger.WithValues(r.Logger,
		"controller", "DuckLakeCatalog",
		"namespace", req.Namespace,
		"name", req.Name)
	ctx = logger.WithContext(ctx, l)

	startTime := time.Now()
	defer func() {
		metrics.RecordJobDuration("reconcile", "completed", time.Since(startTime).Seconds())
	}()

	// Get the DuckLakeCatalog instance
	catalog := &ducklakev1alpha1.DuckLakeCatalog{}
	if err := r.Get(ctx, req.NamespacedName, catalog); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		l.Error().Err(err).Msg("failed to get DuckLakeCatalog")
		return ctrl.Result{}, fmt.Errorf("failed to get DuckLakeCatalog: %w", err)
	}

	// Handle deletion
	if !catalog.DeletionTimestamp.IsZero() {
		metrics.RecordTableOperation("delete", "started")
		result, err := r.handleDeletion(ctx, catalog)
		if err != nil {
			metrics.RecordTableOperation("delete", "failed")
			return result, err
		}
		metrics.RecordTableOperation("delete", "succeeded")
		return result, nil
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(catalog, "ducklakecatalog.featherman.dev") {
		controllerutil.AddFinalizer(catalog, "ducklakecatalog.featherman.dev")
		if err := r.Update(ctx, catalog); err != nil {
			l.Error().Err(err).Msg("failed to add finalizer")
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer: %w", err)
		}
	}

	// Validate storage class
	storageClass := &storagev1.StorageClass{}
	if err := r.Get(ctx, client.ObjectKey{Name: catalog.Spec.StorageClass}, storageClass); err != nil {
		if errors.IsNotFound(err) {
			l.Error().Str("storageClass", catalog.Spec.StorageClass).Msg("storage class not found")
			r.setCondition(catalog, "Ready", metav1.ConditionFalse, "StorageClassNotFound", fmt.Sprintf("Storage class %s not found", catalog.Spec.StorageClass))
			catalog.Status.Phase = ducklakev1alpha1.CatalogPhaseFailed
			if err := r.Status().Update(ctx, catalog); err != nil {
				l.Error().Err(err).Msg("failed to update status")
			}
			return ctrl.Result{}, fmt.Errorf("storage class %s not found", catalog.Spec.StorageClass)
		}
		return ctrl.Result{}, fmt.Errorf("failed to get storage class: %w", err)
	}

	// Validate S3 credentials and connection
	if err := r.validateS3Connection(ctx, catalog); err != nil {
		l.Error().Err(err).Msg("failed to validate S3 connection")
		r.setCondition(catalog, "Ready", metav1.ConditionFalse, "S3ConnectionFailed", fmt.Sprintf("Failed to connect to S3: %v", err))
		catalog.Status.Phase = ducklakev1alpha1.CatalogPhaseFailed
		if err := r.Status().Update(ctx, catalog); err != nil {
			l.Error().Err(err).Msg("failed to update status")
		}
		return ctrl.Result{}, fmt.Errorf("failed to validate S3 connection: %w", err)
	}

	// Create or update PVC
	pvc, err := r.reconcilePVC(ctx, catalog)
	if err != nil {
		l.Error().Err(err).Msg("failed to reconcile PVC")
		r.setCondition(catalog, "Ready", metav1.ConditionFalse, "PVCReconcileFailed", fmt.Sprintf("Failed to reconcile PVC: %v", err))
		catalog.Status.Phase = ducklakev1alpha1.CatalogPhaseFailed
		if err := r.Status().Update(ctx, catalog); err != nil {
			l.Error().Err(err).Msg("failed to update status")
		}
		metrics.RecordTableOperation("reconcile_pvc", "failed")
		r.Recorder.Event(catalog, corev1.EventTypeWarning, "PVCReconcileFailed", err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to reconcile PVC: %w", err)
	}
	metrics.RecordTableOperation("reconcile_pvc", "succeeded")

	// Update storage metrics
	if pvc.Status.Phase == corev1.ClaimBound {
		if quantity, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok {
			metrics.SetCatalogSize(catalog.Name, float64(quantity.Value()))
		}
	}

	// Schedule backup if needed
	if catalog.Spec.BackupPolicy != nil {
		if err := r.Backup.ScheduleBackup(ctx, catalog); err != nil {
			l.Error().Err(err).Msg("failed to schedule backup")
			r.setCondition(catalog, "Ready", metav1.ConditionFalse, "BackupScheduleFailed", fmt.Sprintf("Failed to schedule backup: %v", err))
			r.Recorder.Event(catalog, corev1.EventTypeWarning, "BackupScheduleFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to schedule backup: %w", err)
		}
		metrics.RecordTableOperation("reconcile_backup", "succeeded")
	}

	// Update status
	r.setCondition(catalog, "Ready", metav1.ConditionTrue, "CatalogReady", "Catalog is ready")
	catalog.Status.Phase = ducklakev1alpha1.CatalogPhaseSucceeded
	catalog.Status.ObservedGeneration = catalog.Generation
	if err := r.Status().Update(ctx, catalog); err != nil {
		metrics.RecordTableOperation("status_update", "failed")
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}
	metrics.RecordTableOperation("status_update", "succeeded")

	l.Info().Msg("reconciled DuckLakeCatalog successfully")
	return ctrl.Result{}, nil
}

// validateS3Connection validates the S3 connection using the provided credentials
func (r *DuckLakeCatalogReconciler) validateS3Connection(ctx context.Context, catalog *ducklakev1alpha1.DuckLakeCatalog) error {
	// Get S3 credentials from secret
	secret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: catalog.Namespace,
		Name:      catalog.Spec.ObjectStore.CredentialsSecret.Name,
	}, secret); err != nil {
		return fmt.Errorf("failed to get S3 credentials secret: %w", err)
	}

	// Test connection by listing buckets
	_, err := r.Storage.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("failed to list buckets: %w", err)
	}

	return nil
}

// setCondition updates the condition in the catalog status
func (r *DuckLakeCatalogReconciler) setCondition(catalog *ducklakev1alpha1.DuckLakeCatalog, conditionType string, status metav1.ConditionStatus, reason, message string) {
	now := metav1.Now()
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
	}

	// Find and update existing condition or append new one
	for i, c := range catalog.Status.Conditions {
		if c.Type == conditionType {
			if c.Status != status {
				catalog.Status.Conditions[i] = condition
			}
			return
		}
	}
	catalog.Status.Conditions = append(catalog.Status.Conditions, condition)
}

// handleDeletion handles the deletion of a DuckLakeCatalog
func (r *DuckLakeCatalogReconciler) handleDeletion(ctx context.Context, catalog *ducklakev1alpha1.DuckLakeCatalog) (ctrl.Result, error) {
	l := logger.FromContext(ctx)
	l.Info().Msg("handling DuckLakeCatalog deletion")

	// Check if PVC exists
	pvc := &corev1.PersistentVolumeClaim{}
	pvcName := fmt.Sprintf("%s-catalog", catalog.Name)
	err := r.Get(ctx, client.ObjectKey{Namespace: catalog.Namespace, Name: pvcName}, pvc)
	if err != nil && !errors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("failed to get PVC: %w", err)
	}

	// Delete PVC if it exists
	if err == nil {
		if err := r.Delete(ctx, pvc); err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("failed to delete PVC: %w", err)
		}
		r.Recorder.Event(catalog, corev1.EventTypeNormal, "PVCDeleted", "Deleted catalog PVC")
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(catalog, "ducklakecatalog.featherman.dev")
	if err := r.Update(ctx, catalog); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to remove finalizer: %w", err)
	}

	l.Info().Msg("handled DuckLakeCatalog deletion successfully")
	return ctrl.Result{}, nil
}

// reconcilePVC ensures the PVC exists and is configured correctly
func (r *DuckLakeCatalogReconciler) reconcilePVC(ctx context.Context, catalog *ducklakev1alpha1.DuckLakeCatalog) (*corev1.PersistentVolumeClaim, error) {
	pvcName := fmt.Sprintf("%s-catalog", catalog.Name)
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, client.ObjectKey{Namespace: catalog.Namespace, Name: pvcName}, pvc)
	if err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get PVC: %w", err)
		}
		// PVC doesn't exist, create it
		pvc = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: catalog.Namespace,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse(catalog.Spec.Size),
					},
				},
				StorageClassName: &catalog.Spec.StorageClass,
			},
		}
		if err := ctrl.SetControllerReference(catalog, pvc, r.Scheme); err != nil {
			return nil, fmt.Errorf("failed to set controller reference: %w", err)
		}
		if err := r.Create(ctx, pvc); err != nil {
			return nil, fmt.Errorf("failed to create PVC: %w", err)
		}
	}
	// PVC exists, no need to update immutable fields
	return pvc, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DuckLakeCatalogReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Initialize logger
	r.Logger = &zerolog.Logger{}
	*r.Logger = zerolog.New(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339Nano,
	}).With().Timestamp().Logger()

	// Initialize retry config
	r.RetryConfig = retry.DefaultRetryConfig

	// Initialize S3 client
	customEndpoint := "http://minio.minio-test.svc.cluster.local:9000"
	s3Client := awss3.NewFromConfig(aws.Config{
		Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(
			"minioadmin", // Default MinIO access key
			"minioadmin", // Default MinIO secret key
			"",
		),
	}, func(o *awss3.Options) {
		o.BaseEndpoint = &customEndpoint
		o.UsePathStyle = true
	})

	// Initialize backup manager
	r.Backup = backup.NewBackupManager(r.Client, s3Client, r.Logger)

	// Add cleanup on shutdown
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		// Wait for cache to be ready
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			return fmt.Errorf("failed to wait for caches to sync")
		}

		// Start backup manager after cache is ready
		if err := r.Backup.Start(ctx); err != nil {
			return fmt.Errorf("failed to start backup manager: %w", err)
		}

		<-ctx.Done()
		r.Backup.Stop()
		return nil
	})); err != nil {
		return fmt.Errorf("failed to add backup manager cleanup: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&ducklakev1alpha1.DuckLakeCatalog{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}
