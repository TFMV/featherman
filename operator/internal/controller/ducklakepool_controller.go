package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/logger"
	"github.com/TFMV/featherman/operator/internal/pool"
)

// DuckLakePoolReconciler reconciles a DuckLakePool object
type DuckLakePoolReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder
	Logger    *zerolog.Logger
	K8sClient kubernetes.Interface
	Config    *rest.Config

	// Pool managers by namespace/name
	poolManagers map[string]*pool.Manager
}

// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklakepools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklakepools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklakepools/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch

// Reconcile handles DuckLakePool resources
func (r *DuckLakePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := logger.WithValues(r.Logger,
		"controller", "DuckLakePool",
		"namespace", req.Namespace,
		"name", req.Name)
	ctx = logger.WithContext(ctx, l)

	// Get the DuckLakePool instance
	duckLakePool := &ducklakev1alpha1.DuckLakePool{}
	if err := r.Get(ctx, req.NamespacedName, duckLakePool); err != nil {
		if errors.IsNotFound(err) {
			// Pool was deleted, shut down manager if exists
			managerKey := fmt.Sprintf("%s/%s", req.Namespace, req.Name)
			if mgr, ok := r.poolManagers[managerKey]; ok {
				l.Info().Msg("shutting down pool manager for deleted pool")
				if err := mgr.Shutdown(ctx); err != nil {
					l.Error().Err(err).Msg("failed to shut down pool manager")
				}
				delete(r.poolManagers, managerKey)
			}
			return ctrl.Result{}, nil
		}
		l.Error().Err(err).Msg("failed to get DuckLakePool")
		return ctrl.Result{}, err
	}

	// Handle deletion
	if !duckLakePool.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, duckLakePool)
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(duckLakePool, "ducklakepool.featherman.dev") {
		controllerutil.AddFinalizer(duckLakePool, "ducklakepool.featherman.dev")
		if err := r.Update(ctx, duckLakePool); err != nil {
			l.Error().Err(err).Msg("failed to add finalizer")
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer: %w", err)
		}
	}

	// Validate catalog reference
	catalog := &ducklakev1alpha1.DuckLakeCatalog{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: duckLakePool.Namespace,
		Name:      duckLakePool.Spec.CatalogRef.Name,
	}, catalog); err != nil {
		if errors.IsNotFound(err) {
			r.setCondition(duckLakePool, "Ready", metav1.ConditionFalse, "CatalogNotFound",
				fmt.Sprintf("Referenced catalog %s not found", duckLakePool.Spec.CatalogRef.Name))
			duckLakePool.Status.Phase = ducklakev1alpha1.PoolPhaseFailed
			if err := r.Status().Update(ctx, duckLakePool); err != nil {
				l.Error().Err(err).Msg("failed to update status")
			}
			r.Recorder.Event(duckLakePool, corev1.EventTypeWarning, "CatalogNotFound",
				fmt.Sprintf("Referenced catalog %s not found", duckLakePool.Spec.CatalogRef.Name))
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get catalog: %w", err)
	}

	// Ensure service account exists
	if err := r.ensureServiceAccount(ctx, duckLakePool); err != nil {
		l.Error().Err(err).Msg("failed to ensure service account")
		r.setCondition(duckLakePool, "Ready", metav1.ConditionFalse, "ServiceAccountFailed",
			fmt.Sprintf("Failed to create service account: %v", err))
		duckLakePool.Status.Phase = ducklakev1alpha1.PoolPhaseFailed
		if err := r.Status().Update(ctx, duckLakePool); err != nil {
			l.Error().Err(err).Msg("failed to update status")
		}
		return ctrl.Result{}, fmt.Errorf("failed to ensure service account: %w", err)
	}

	// Get or create pool manager
	managerKey := fmt.Sprintf("%s/%s", duckLakePool.Namespace, duckLakePool.Name)
	mgr, exists := r.poolManagers[managerKey]

	if !exists {
		// Create new pool manager
		mgr = pool.NewManager(r.Client, r.K8sClient, r.Scheme, l, duckLakePool, duckLakePool.Namespace, r.Config)
		r.poolManagers[managerKey] = mgr

		// Start the manager in a goroutine
		go func() {
			if err := mgr.Start(ctx); err != nil {
				l.Error().Err(err).Msg("pool manager stopped with error")
			}
		}()

		r.Recorder.Event(duckLakePool, corev1.EventTypeNormal, "PoolManagerStarted", "Pool manager started")
	}

	// Update pool status
	status := mgr.GetPoolStatus()
	duckLakePool.Status = status
	duckLakePool.Status.Phase = ducklakev1alpha1.PoolPhaseRunning
	duckLakePool.Status.ObservedGeneration = duckLakePool.Generation

	// Set ready condition
	r.setCondition(duckLakePool, "Ready", metav1.ConditionTrue, "PoolReady", "Pool is ready")

	if err := r.Status().Update(ctx, duckLakePool); err != nil {
		l.Error().Err(err).Msg("failed to update status")
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}

	// Requeue to update status periodically
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// handleDeletion handles the deletion of a DuckLakePool
func (r *DuckLakePoolReconciler) handleDeletion(ctx context.Context, duckLakePool *ducklakev1alpha1.DuckLakePool) (ctrl.Result, error) {
	l := logger.FromContext(ctx)
	l.Info().Msg("handling DuckLakePool deletion")

	// Update phase
	duckLakePool.Status.Phase = ducklakev1alpha1.PoolPhaseTerminating
	if err := r.Status().Update(ctx, duckLakePool); err != nil {
		l.Error().Err(err).Msg("failed to update status")
	}

	// Shut down pool manager
	managerKey := fmt.Sprintf("%s/%s", duckLakePool.Namespace, duckLakePool.Name)
	if mgr, ok := r.poolManagers[managerKey]; ok {
		l.Info().Msg("shutting down pool manager")
		if err := mgr.Shutdown(ctx); err != nil {
			l.Error().Err(err).Msg("failed to shut down pool manager")
			return ctrl.Result{Requeue: true}, err
		}
		delete(r.poolManagers, managerKey)
	}

	// Remove finalizer
	if controllerutil.ContainsFinalizer(duckLakePool, "ducklakepool.featherman.dev") {
		controllerutil.RemoveFinalizer(duckLakePool, "ducklakepool.featherman.dev")
		if err := r.Update(ctx, duckLakePool); err != nil {
			l.Error().Err(err).Msg("failed to remove finalizer")
			return ctrl.Result{}, fmt.Errorf("failed to remove finalizer: %w", err)
		}
		r.Recorder.Event(duckLakePool, corev1.EventTypeNormal, "FinalizerRemoved", "Finalizer removed")
	}

	l.Info().Msg("DuckLakePool deletion completed")
	return ctrl.Result{}, nil
}

// ensureServiceAccount ensures the service account exists for pool pods
func (r *DuckLakePoolReconciler) ensureServiceAccount(ctx context.Context, duckLakePool *ducklakev1alpha1.DuckLakePool) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ducklake-pool",
			Namespace: duckLakePool.Namespace,
		},
	}

	if err := ctrl.SetControllerReference(duckLakePool, sa, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	// Check if service account exists
	existing := &corev1.ServiceAccount{}
	err := r.Get(ctx, client.ObjectKey{
		Namespace: sa.Namespace,
		Name:      sa.Name,
	}, existing)

	if err != nil {
		if errors.IsNotFound(err) {
			// Create service account
			if err := r.Create(ctx, sa); err != nil {
				return fmt.Errorf("failed to create service account: %w", err)
			}
		} else {
			return fmt.Errorf("failed to get service account: %w", err)
		}
	}

	return nil
}

// setCondition updates the condition in the pool status
func (r *DuckLakePoolReconciler) setCondition(duckLakePool *ducklakev1alpha1.DuckLakePool, conditionType string, status metav1.ConditionStatus, reason, message string) {
	now := metav1.Now()
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
	}

	// Find and update existing condition or append new one
	for i, c := range duckLakePool.Status.Conditions {
		if c.Type == conditionType {
			if c.Status != status {
				duckLakePool.Status.Conditions[i] = condition
			}
			return
		}
	}
	duckLakePool.Status.Conditions = append(duckLakePool.Status.Conditions, condition)
}

// GetPoolManager returns the pool manager for a given pool
func (r *DuckLakePoolReconciler) GetPoolManager(namespace, name string) (*pool.Manager, bool) {
	managerKey := fmt.Sprintf("%s/%s", namespace, name)
	mgr, ok := r.poolManagers[managerKey]
	return mgr, ok
}

// SetupWithManager sets up the controller with the Manager
func (r *DuckLakePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Initialize map if not already done
	if r.poolManagers == nil {
		r.poolManagers = make(map[string]*pool.Manager)
	}

	// Store the config
	r.Config = mgr.GetConfig()

	// Set up K8s client if not already set
	if r.K8sClient == nil {
		k8sClient, err := kubernetes.NewForConfig(mgr.GetConfig())
		if err != nil {
			return fmt.Errorf("failed to create kubernetes client: %w", err)
		}
		r.K8sClient = k8sClient
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&ducklakev1alpha1.DuckLakePool{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.ServiceAccount{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(r)
}
