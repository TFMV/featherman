package controller

import (
	"context"
	"fmt"
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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/config"
	"github.com/TFMV/featherman/operator/internal/duckdb"
	"github.com/TFMV/featherman/operator/internal/metrics"
	"github.com/TFMV/featherman/operator/internal/sql"
)

// DuckLakeTableReconciler reconciles a DuckLakeTable object
type DuckLakeTableReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Recorder    record.EventRecorder
	JobManager  *duckdb.JobManager
	SQLGen      *sql.Generator
	PodTemplate *config.PodTemplateConfig
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

	// List all jobs for this table
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(table.Namespace), client.MatchingLabels{
		"ducklake.featherman.dev/table": table.Name,
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list jobs: %w", err)
	}

	// Find the most recent job
	var latestJob *batchv1.Job
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if latestJob == nil || job.CreationTimestamp.After(latestJob.CreationTimestamp.Time) {
			latestJob = job
		}
	}

	// If we have a job, update the status based on its state
	if latestJob != nil {
		if duckdb.IsJobComplete(latestJob) {
			table.Status.Phase = ducklakev1alpha1.TablePhaseSucceeded
			table.Status.LastModified = &metav1.Time{Time: latestJob.Status.CompletionTime.Time}

			// Record metrics
			if latestJob.Labels["app.kubernetes.io/operation"] == string(duckdb.OperationTypeWrite) {
				metrics.RecordTableOperation("write", "succeeded")
			} else {
				metrics.RecordTableOperation("read", "succeeded")
			}

			// Update status
			if err := r.Status().Update(ctx, table); err != nil {
				r.Recorder.Event(table, corev1.EventTypeWarning, "StatusUpdateFailed", err.Error())
				return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
			}

			// Job is complete, nothing more to do
			return ctrl.Result{}, nil
		} else if duckdb.IsJobFailed(latestJob) {
			table.Status.Phase = ducklakev1alpha1.TablePhaseFailed

			// Record metrics
			if latestJob.Labels["app.kubernetes.io/operation"] == string(duckdb.OperationTypeWrite) {
				metrics.RecordTableOperation("write", "failed")
			} else {
				metrics.RecordTableOperation("read", "failed")
			}

			// Update status
			if err := r.Status().Update(ctx, table); err != nil {
				r.Recorder.Event(table, corev1.EventTypeWarning, "StatusUpdateFailed", err.Error())
				return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
			}
		}
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

	// Only create a new job if there's no existing job or if the existing job has failed
	if latestJob == nil || duckdb.IsJobFailed(latestJob) {
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

		// Get the catalog
		catalog := &ducklakev1alpha1.DuckLakeCatalog{}
		if err := r.Get(ctx, client.ObjectKey{
			Namespace: table.Namespace,
			Name:      table.Spec.CatalogRef,
		}, catalog); err != nil {
			r.Recorder.Event(table, corev1.EventTypeWarning, "CatalogNotFound", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to get catalog: %w", err)
		}

		// Create DuckDB job
		op := &duckdb.Operation{
			Type:          duckdb.OperationTypeWrite,
			SQL:           sql.TransactionSQL(createSQL, attachSQL),
			Table:         table,
			Catalog:       catalog,
			PodTemplate:   r.PodTemplate,
			JobNameSuffix: "create",
		}

		// Create the job
		job, err := r.JobManager.CreateJob(op)
		if err != nil {
			table.Status.Phase = ducklakev1alpha1.TablePhaseFailed
			metrics.RecordTableOperation("create_job", "failed")
			r.Recorder.Event(table, corev1.EventTypeWarning, "JobCreationFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to create job: %w", err)
		}
		metrics.RecordTableOperation("create_job", "succeeded")

		// Set controller reference
		if err := ctrl.SetControllerReference(table, job, r.Scheme); err != nil {
			r.Recorder.Event(table, corev1.EventTypeWarning, "SetControllerReferenceFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to set controller reference: %w", err)
		}

		// Create the job
		if err := r.Create(ctx, job); err != nil {
			r.Recorder.Event(table, corev1.EventTypeWarning, "JobCreationFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to create job: %w", err)
		}

		// Update status
		table.Status.Phase = ducklakev1alpha1.TablePhasePending
		table.Status.ObservedGeneration = table.Generation
		if err := r.Status().Update(ctx, table); err != nil {
			r.Recorder.Event(table, corev1.EventTypeWarning, "StatusUpdateFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
		}

		logger.Info("created table creation job", "job", job.Name)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// If we get here, there's an existing job that's still running
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// handleDeletion handles the deletion of a DuckLakeTable
func (r *DuckLakeTableReconciler) handleDeletion(ctx context.Context, table *ducklakev1alpha1.DuckLakeTable) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("handling DuckLakeTable deletion")

	// Generate SQL for dropping table
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s;", table.Spec.Name)

	// Get the catalog
	catalog := &ducklakev1alpha1.DuckLakeCatalog{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: table.Namespace,
		Name:      table.Spec.CatalogRef,
	}, catalog); err != nil {
		r.Recorder.Event(table, corev1.EventTypeWarning, "CatalogNotFound", err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to get catalog: %w", err)
	}

	// Create DuckDB job for cleanup
	op := &duckdb.Operation{
		Type:          duckdb.OperationTypeWrite,
		SQL:           dropSQL,
		Table:         table,
		Catalog:       catalog,
		PodTemplate:   r.PodTemplate,
		JobNameSuffix: "drop",
	}

	// Create the job
	job, err := r.JobManager.CreateJob(op)
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
	// Initialize with default pod template
	r.PodTemplate = config.DefaultPodTemplateConfig()

	return ctrl.NewControllerManagedBy(mgr).
		For(&ducklakev1alpha1.DuckLakeTable{}).
		Owns(&batchv1.Job{}).
		// Watch ConfigMaps for pod template updates
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.findTablesForConfigMap),
		).
		Complete(r)
}

// findTablesForConfigMap maps ConfigMap changes to DuckLakeTable reconcile requests
func (r *DuckLakeTableReconciler) findTablesForConfigMap(ctx context.Context, obj client.Object) []ctrl.Request {
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

	// List all DuckLakeTables
	var tables ducklakev1alpha1.DuckLakeTableList
	if err := r.List(ctx, &tables); err != nil {
		return nil
	}

	var requests []ctrl.Request
	for _, table := range tables.Items {
		requests = append(requests, ctrl.Request{
			NamespacedName: client.ObjectKey{
				Name:      table.Name,
				Namespace: table.Namespace,
			},
		})
	}
	return requests
}
