package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
)

// DuckLakeCatalogReconciler reconciles a DuckLakeCatalog object
type DuckLakeCatalogReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklakecatalogs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklakecatalogs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklakecatalogs/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile handles DuckLakeCatalog resources
func (r *DuckLakeCatalogReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling DuckLakeCatalog", "namespacedName", req.NamespacedName)

	// Get the DuckLakeCatalog instance
	catalog := &ducklakev1alpha1.DuckLakeCatalog{}
	if err := r.Get(ctx, req.NamespacedName, catalog); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get DuckLakeCatalog: %w", err)
	}

	// Initialize status if needed
	if catalog.Status.Phase == "" {
		catalog.Status.Phase = ducklakev1alpha1.CatalogPhasePending
		if err := r.Status().Update(ctx, catalog); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
		}
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(catalog, "ducklakecatalog.featherman.dev") {
		controllerutil.AddFinalizer(catalog, "ducklakecatalog.featherman.dev")
		if err := r.Update(ctx, catalog); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer: %w", err)
		}
	}

	// Handle deletion
	if !catalog.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, catalog)
	}

	// Ensure PVC exists
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-catalog", catalog.Name),
			Namespace: catalog.Namespace,
		},
	}

	result, err := ctrl.CreateOrUpdate(ctx, r.Client, pvc, func() error {
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
	})

	if err != nil {
		catalog.Status.Phase = ducklakev1alpha1.CatalogPhaseFailed
		r.Recorder.Event(catalog, corev1.EventTypeWarning, "PVCCreationFailed", err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to ensure PVC: %w", err)
	}

	if result == controllerutil.OperationResultCreated {
		r.Recorder.Event(catalog, corev1.EventTypeNormal, "PVCCreated", "Created catalog PVC")
	}

	// Update status
	catalog.Status.Phase = ducklakev1alpha1.CatalogPhaseSucceeded
	catalog.Status.ObservedGeneration = catalog.Generation
	if err := r.Status().Update(ctx, catalog); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}

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

// SetupWithManager sets up the controller with the Manager.
func (r *DuckLakeCatalogReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ducklakev1alpha1.DuckLakeCatalog{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}
