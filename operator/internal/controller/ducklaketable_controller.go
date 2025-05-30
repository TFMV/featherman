package controller

import (
	"context"
	"fmt"
	"path"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/duckdb"
	"github.com/TFMV/featherman/operator/internal/metrics"
	"github.com/TFMV/featherman/operator/internal/sql"
)

// DuckLakeTableReconciler reconciles a DuckLakeTable object
type DuckLakeTableReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Recorder   record.EventRecorder
	JobManager duckdb.JobManager
	SQLGen     *sql.Generator
}

// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklaketables,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklaketables/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklaketables/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile handles DuckLakeTable resources
func (r *DuckLakeTableReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling DuckLakeTable", "namespacedName", req.NamespacedName)

	startTime := time.Now()
	defer func() {
		metrics.RecordJobDuration("reconcile", "completed", time.Since(startTime).Seconds())
	}()

	// Get the DuckLakeTable instance
	table := &ducklakev1alpha1.DuckLakeTable{}
	if err := r.Get(ctx, req.NamespacedName, table); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		metrics.RecordTableOperation("get", "failed")
		return ctrl.Result{}, fmt.Errorf("failed to get DuckLakeTable: %w", err)
	}

	// Initialize status if needed
	if table.Status.Phase == "" {
		table.Status.Phase = ducklakev1alpha1.TablePhasePending
		if err := r.Status().Update(ctx, table); err != nil {
			metrics.RecordTableOperation("status_update", "failed")
			return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
		}
		metrics.RecordTableOperation("status_update", "succeeded")
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(table, "ducklaketable.featherman.dev") {
		controllerutil.AddFinalizer(table, "ducklaketable.featherman.dev")
		if err := r.Update(ctx, table); err != nil {
			metrics.RecordTableOperation("add_finalizer", "failed")
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer: %w", err)
		}
		metrics.RecordTableOperation("add_finalizer", "succeeded")
	}

	// Handle deletion
	if !table.DeletionTimestamp.IsZero() {
		metrics.RecordTableOperation("delete", "started")
		result, err := r.handleDeletion(ctx, table)
		if err != nil {
			metrics.RecordTableOperation("delete", "failed")
			return result, err
		}
		metrics.RecordTableOperation("delete", "succeeded")
		return result, nil
	}

	// Generate SQL for table creation
	createSQL, err := r.SQLGen.CreateTableSQL(table)
	if err != nil {
		table.Status.Phase = ducklakev1alpha1.TablePhaseFailed
		metrics.RecordTableOperation("generate_sql", "failed")
		r.Recorder.Event(table, corev1.EventTypeWarning, "SQLGenerationFailed", err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to generate create table SQL: %w", err)
	}

	// Generate SQL for attaching Parquet files
	attachSQL, err := r.SQLGen.AttachParquetSQL(table, table.Spec.Location)
	if err != nil {
		table.Status.Phase = ducklakev1alpha1.TablePhaseFailed
		metrics.RecordTableOperation("generate_sql", "failed")
		r.Recorder.Event(table, corev1.EventTypeWarning, "SQLGenerationFailed", err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to generate attach SQL: %w", err)
	}

	// Create DuckDB job
	jobConfig := duckdb.JobConfig{
		Name:        fmt.Sprintf("%s-table-create", table.Name),
		Namespace:   table.Namespace,
		SQL:         sql.TransactionSQL(createSQL, attachSQL),
		CatalogPath: path.Join("/catalog", "catalog.db"),
		ReadOnly:    false,
	}

	// Set owner reference
	jobConfig.OwnerReference = metav1.NewControllerRef(table, ducklakev1alpha1.GroupVersion.WithKind("DuckLakeTable"))

	// Create the job
	job, err := r.JobManager.CreateJob(ctx, jobConfig)
	if err != nil {
		table.Status.Phase = ducklakev1alpha1.TablePhaseFailed
		metrics.RecordTableOperation("create_job", "failed")
		r.Recorder.Event(table, corev1.EventTypeWarning, "JobCreationFailed", err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to create job: %w", err)
	}
	metrics.RecordTableOperation("create_job", "succeeded")

	// Check job status
	if duckdb.IsJobComplete(job) {
		table.Status.Phase = ducklakev1alpha1.TablePhaseSucceeded
		table.Status.LastModified = &metav1.Time{Time: job.Status.CompletionTime.Time}
		table.Status.ObservedGeneration = table.Generation

		// Update metrics
		if len(table.Spec.Format.Partitioning) > 0 {
			metrics.UpdateTablePartitionCount(table.Name, float64(len(table.Spec.Format.Partitioning)))
		}

		r.Recorder.Event(table, corev1.EventTypeNormal, "TableCreated", "Table created successfully")
		metrics.RecordTableOperation("reconcile", "succeeded")
	} else if duckdb.IsJobFailed(job) {
		table.Status.Phase = ducklakev1alpha1.TablePhaseFailed
		r.Recorder.Event(table, corev1.EventTypeWarning, "TableCreationFailed", "Job failed")
		metrics.RecordTableOperation("reconcile", "failed")
		return ctrl.Result{}, fmt.Errorf("job failed")
	}

	// Update status
	if err := r.Status().Update(ctx, table); err != nil {
		metrics.RecordTableOperation("status_update", "failed")
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}
	metrics.RecordTableOperation("status_update", "succeeded")

	logger.Info("reconciled DuckLakeTable successfully")
	return ctrl.Result{}, nil
}

// handleDeletion handles the deletion of a DuckLakeTable
func (r *DuckLakeTableReconciler) handleDeletion(ctx context.Context, table *ducklakev1alpha1.DuckLakeTable) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("handling DuckLakeTable deletion")

	// Generate SQL for dropping table
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s;", table.Spec.Name)

	// Create DuckDB job for cleanup
	jobConfig := duckdb.JobConfig{
		Name:        fmt.Sprintf("%s-table-drop", table.Name),
		Namespace:   table.Namespace,
		SQL:         dropSQL,
		CatalogPath: path.Join("/catalog", "catalog.db"),
		ReadOnly:    false,
	}

	// Set owner reference
	jobConfig.OwnerReference = metav1.NewControllerRef(table, ducklakev1alpha1.GroupVersion.WithKind("DuckLakeTable"))

	// Create the job
	job, err := r.JobManager.CreateJob(ctx, jobConfig)
	if err != nil {
		r.Recorder.Event(table, corev1.EventTypeWarning, "CleanupFailed", err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to create cleanup job: %w", err)
	}

	// Wait for job completion
	if !duckdb.IsJobComplete(job) && !duckdb.IsJobFailed(job) {
		return ctrl.Result{Requeue: true}, nil
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(table, "ducklaketable.featherman.dev")
	if err := r.Update(ctx, table); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to remove finalizer: %w", err)
	}

	logger.Info("handled DuckLakeTable deletion successfully")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DuckLakeTableReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ducklakev1alpha1.DuckLakeTable{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
