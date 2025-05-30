package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/config"
	"github.com/TFMV/featherman/operator/internal/metrics"
)

// DuckLakeCatalogReconciler reconciles a DuckLakeCatalog object
type DuckLakeCatalogReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// PodTemplate holds the current pod template configuration
	PodTemplate *config.PodTemplateConfig
}

// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklakecatalogs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklakecatalogs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklakecatalogs/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// SetupWithManager sets up the controller with the Manager.
func (r *DuckLakeCatalogReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Initialize with default pod template
	r.PodTemplate = config.DefaultPodTemplateConfig()

	return ctrl.NewControllerManagedBy(mgr).
		For(&ducklakev1alpha1.DuckLakeCatalog{}).
		// Watch ConfigMaps for pod template updates
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.findCatalogsForConfigMap),
		).
		Complete(r)
}

// findCatalogsForConfigMap maps ConfigMap changes to DuckLakeCatalog reconcile requests
func (r *DuckLakeCatalogReconciler) findCatalogsForConfigMap(ctx context.Context, obj client.Object) []ctrl.Request {
	configMap := obj.(*corev1.ConfigMap)
	if configMap.Name != "ducklake-pod-template" {
		return nil
	}

	// Load new pod template configuration
	if data, ok := configMap.Data["config"]; ok {
		if newTemplate, err := config.LoadFromConfigMap(data); err == nil {
			r.PodTemplate = newTemplate
		}
	}

	// List all DuckLakeCatalogs
	var catalogs ducklakev1alpha1.DuckLakeCatalogList
	if err := r.List(ctx, &catalogs); err != nil {
		return nil
	}

	var requests []ctrl.Request
	for _, catalog := range catalogs.Items {
		requests = append(requests, ctrl.Request{
			NamespacedName: client.ObjectKey{
				Name:      catalog.Name,
				Namespace: catalog.Namespace,
			},
		})
	}
	return requests
}

// Reconcile handles DuckLakeCatalog resources
func (r *DuckLakeCatalogReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling DuckLakeCatalog", "namespacedName", req.NamespacedName)

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
		metrics.RecordTableOperation("get", "failed")
		return ctrl.Result{}, fmt.Errorf("failed to get DuckLakeCatalog: %w", err)
	}

	// Initialize status if needed
	if catalog.Status.Phase == "" {
		catalog.Status.Phase = ducklakev1alpha1.CatalogPhasePending
		if err := r.Status().Update(ctx, catalog); err != nil {
			metrics.RecordTableOperation("status_update", "failed")
			return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
		}
		metrics.RecordTableOperation("status_update", "succeeded")
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(catalog, "ducklakecatalog.featherman.dev") {
		controllerutil.AddFinalizer(catalog, "ducklakecatalog.featherman.dev")
		if err := r.Update(ctx, catalog); err != nil {
			metrics.RecordTableOperation("add_finalizer", "failed")
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer: %w", err)
		}
		metrics.RecordTableOperation("add_finalizer", "succeeded")
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

	// Create or update PVC
	pvc, err := r.reconcilePVC(ctx, catalog)
	if err != nil {
		catalog.Status.Phase = ducklakev1alpha1.CatalogPhaseFailed
		metrics.RecordTableOperation("reconcile_pvc", "failed")
		r.Recorder.Event(catalog, corev1.EventTypeWarning, "PVCReconcileFailed", err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to reconcile PVC: %w", err)
	}
	metrics.RecordTableOperation("reconcile_pvc", "succeeded")

	// Update storage metrics
	if pvc.Status.Phase == corev1.ClaimBound {
		if quantity, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok {
			metrics.UpdateStorageUsage(catalog.Name, "catalog", float64(quantity.Value()))
		}
	}

	// Update backup metrics if backup policy exists
	if catalog.Spec.BackupPolicy != nil {
		if catalog.Status.LastBackup != nil {
			metrics.UpdateCatalogBackupStatus(catalog.Name, true)
		} else {
			metrics.UpdateCatalogBackupStatus(catalog.Name, false)
		}
	}

	// Update status
	catalog.Status.Phase = ducklakev1alpha1.CatalogPhaseSucceeded
	catalog.Status.ObservedGeneration = catalog.Generation
	if err := r.Status().Update(ctx, catalog); err != nil {
		metrics.RecordTableOperation("status_update", "failed")
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}
	metrics.RecordTableOperation("status_update", "succeeded")

	logger.Info("reconciled DuckLakeCatalog successfully")
	return ctrl.Result{}, nil
}

// handleDeletion handles the deletion of a DuckLakeCatalog
func (r *DuckLakeCatalogReconciler) handleDeletion(ctx context.Context, catalog *ducklakev1alpha1.DuckLakeCatalog) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("handling DuckLakeCatalog deletion")

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

	logger.Info("handled DuckLakeCatalog deletion successfully")
	return ctrl.Result{}, nil
}

// reconcilePVC ensures the PVC exists and is configured correctly
func (r *DuckLakeCatalogReconciler) reconcilePVC(ctx context.Context, catalog *ducklakev1alpha1.DuckLakeCatalog) (*corev1.PersistentVolumeClaim, error) {
	// Ensure PVC exists
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-catalog", catalog.Name),
			Namespace: catalog.Namespace,
		},
	}

	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, pvc, func() error {
		// Set owner reference
		if err := ctrl.SetControllerReference(catalog, pvc, r.Scheme); err != nil {
			return err
		}

		// Update PVC spec
		pvc.Spec = corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(catalog.Spec.Size),
				},
			},
			StorageClassName: &catalog.Spec.StorageClass,
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to ensure PVC: %w", err)
	}

	return pvc, nil
}
