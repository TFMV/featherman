package controller

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
	"github.com/TFMV/featherman/operator/internal/config"
	"github.com/TFMV/featherman/operator/internal/duckdb"
	"github.com/TFMV/featherman/operator/internal/logger"
	"github.com/TFMV/featherman/operator/internal/metrics"
	"github.com/TFMV/featherman/operator/internal/retry"
	"github.com/TFMV/featherman/operator/internal/sql"
	"github.com/TFMV/featherman/operator/internal/storage"
)

// DuckLakeTableReconciler reconciles a DuckLakeTable object
type DuckLakeTableReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Recorder    record.EventRecorder
	JobManager  *duckdb.JobManager
	SQLGen      *sql.Generator
	PodTemplate *config.PodTemplateConfig
	Consistency *storage.ConsistencyChecker
	Logger      *zerolog.Logger
	RetryConfig retry.RetryConfig
	Lifecycle   *duckdb.LifecycleManager
}

// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklaketables,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklaketables/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklaketables/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile handles DuckLakeTable resources
func (r *DuckLakeTableReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := logger.WithValues(r.Logger,
		"controller", "DuckLakeTable",
		"namespace", req.Namespace,
		"name", req.Name)
	ctx = logger.WithContext(ctx, l)

	l.Info().Msg("reconciling DuckLakeTable")

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
		l.Error().Err(err).Msg("failed to get DuckLakeTable")
		metrics.RecordTableOperation("get", "failed")
		return ctrl.Result{}, fmt.Errorf("failed to get DuckLakeTable: %w", err)
	}

	// List all jobs for this table
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(table.Namespace), client.MatchingLabels{
		"ducklake.featherman.dev/table": table.Name,
	}); err != nil {
		l.Error().Err(err).Msg("failed to list jobs")
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
		state, err := r.Lifecycle.GetJobState(ctx, latestJob)
		if err != nil {
			l.Error().Err(err).Msg("failed to get job state")
			return ctrl.Result{}, fmt.Errorf("failed to get job state: %w", err)
		}

		if state.Phase == duckdb.JobPhaseComplete {
			// Verify Parquet file consistency with retries
			verifyOp := func(ctx context.Context) error {
				return r.Consistency.VerifyParquetConsistency(ctx, table)
			}

			if err := retry.Do(ctx, verifyOp, r.RetryConfig); err != nil {
				l.Error().Err(err).Msg("failed to verify Parquet consistency")
				table.Status.Phase = ducklakev1alpha1.TablePhaseFailed
				metrics.RecordTableOperation("verify_consistency", "failed")
				r.Recorder.Event(table, corev1.EventTypeWarning, "ConsistencyCheckFailed", err.Error())
				if err := r.Status().Update(ctx, table); err != nil {
					l.Error().Err(err).Msg("failed to update status")
					return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
				}
				return ctrl.Result{}, fmt.Errorf("failed to verify Parquet consistency: %w", err)
			}
			metrics.RecordTableOperation("verify_consistency", "succeeded")

			// List Parquet files to update status
			listOp := func(ctx context.Context) error {
				objects, err := r.Consistency.ListParquetFiles(ctx, table)
				if err != nil {
					return err
				}

				// Calculate total bytes written
				var totalBytes int64
				for _, obj := range objects {
					totalBytes += *obj.Size
				}

				// Update status
				table.Status.Phase = ducklakev1alpha1.TablePhaseSucceeded
				table.Status.LastModified = &metav1.Time{Time: state.CompletionTime.Time}
				table.Status.BytesWritten = totalBytes

				return nil
			}

			if err := retry.Do(ctx, listOp, r.RetryConfig); err != nil {
				l.Error().Err(err).Msg("failed to list Parquet files")
				r.Recorder.Event(table, corev1.EventTypeWarning, "ListParquetFilesFailed", err.Error())
				return ctrl.Result{}, fmt.Errorf("failed to list Parquet files: %w", err)
			}

			// Record metrics
			if latestJob.Labels["app.kubernetes.io/operation"] == string(duckdb.OperationTypeWrite) {
				metrics.RecordTableOperation("write", "succeeded")
			} else {
				metrics.RecordTableOperation("read", "succeeded")
			}

			// Update status
			if err := r.Status().Update(ctx, table); err != nil {
				l.Error().Err(err).Msg("failed to update status")
				r.Recorder.Event(table, corev1.EventTypeWarning, "StatusUpdateFailed", err.Error())
				return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
			}

			// Get job logs for debugging
			logs, err := r.Lifecycle.GetJobLogs(ctx, latestJob)
			if err != nil {
				l.Warn().Err(err).Msg("failed to get job logs")
			} else {
				for podName, log := range logs {
					l.Debug().
						Str("pod", podName).
						Str("logs", log).
						Msg("job pod logs")
				}
			}

			l.Info().
				Int64("bytesWritten", table.Status.BytesWritten).
				Time("lastModified", table.Status.LastModified.Time).
				Msg("table reconciliation completed successfully")

			return ctrl.Result{}, nil
		} else if state.Phase == duckdb.JobPhaseFailed {
			table.Status.Phase = ducklakev1alpha1.TablePhaseFailed

			// Get job logs for debugging
			logs, err := r.Lifecycle.GetJobLogs(ctx, latestJob)
			if err != nil {
				l.Warn().Err(err).Msg("failed to get job logs")
			} else {
				for podName, log := range logs {
					l.Error().
						Str("pod", podName).
						Str("logs", log).
						Msg("job failed, pod logs")
				}
			}

			// Record metrics
			if latestJob.Labels["app.kubernetes.io/operation"] == string(duckdb.OperationTypeWrite) {
				metrics.RecordTableOperation("write", "failed")
			} else {
				metrics.RecordTableOperation("read", "failed")
			}

			// Update status
			if err := r.Status().Update(ctx, table); err != nil {
				l.Error().Err(err).Msg("failed to update status")
				r.Recorder.Event(table, corev1.EventTypeWarning, "StatusUpdateFailed", err.Error())
				return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
			}

			l.Warn().
				Str("jobName", latestJob.Name).
				Str("message", state.Message).
				Msg("job failed")

			return ctrl.Result{}, fmt.Errorf("job failed: %s", state.Message)
		}
	}

	// Initialize status if needed
	if table.Status.Phase == "" {
		table.Status.Phase = ducklakev1alpha1.TablePhasePending
		if err := r.Status().Update(ctx, table); err != nil {
			l.Error().Err(err).Msg("failed to update status")
			metrics.RecordTableOperation("status_update", "failed")
			return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
		}
		metrics.RecordTableOperation("status_update", "succeeded")
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(table, "ducklaketable.featherman.dev") {
		controllerutil.AddFinalizer(table, "ducklaketable.featherman.dev")
		if err := r.Update(ctx, table); err != nil {
			l.Error().Err(err).Msg("failed to add finalizer")
			metrics.RecordTableOperation("add_finalizer", "failed")
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer: %w", err)
		}
		metrics.RecordTableOperation("add_finalizer", "succeeded")
	}

	// Handle deletion
	if !table.DeletionTimestamp.IsZero() {
		l.Info().Msg("handling table deletion")
		metrics.RecordTableOperation("delete", "started")
		result, err := r.handleDeletion(ctx, table)
		if err != nil {
			l.Error().Err(err).Msg("failed to handle deletion")
			metrics.RecordTableOperation("delete", "failed")
			return result, err
		}
		metrics.RecordTableOperation("delete", "succeeded")
		return result, nil
	}

	// Only create a new job if there's no existing job or if the existing job has failed
	var state *duckdb.JobState
	var err error
	if latestJob != nil {
		state, err = r.Lifecycle.GetJobState(ctx, latestJob)
		if err != nil {
			l.Error().Err(err).Msg("failed to get job state")
			return ctrl.Result{}, fmt.Errorf("failed to get job state: %w", err)
		}
	}

	if latestJob == nil || (state != nil && state.Phase == duckdb.JobPhaseFailed) {
		// Generate SQL for table creation
		createSQL, err := r.SQLGen.CreateTableSQL(table)
		if err != nil {
			l.Error().Err(err).Msg("failed to generate create table SQL")
			table.Status.Phase = ducklakev1alpha1.TablePhaseFailed
			metrics.RecordTableOperation("generate_sql", "failed")
			r.Recorder.Event(table, corev1.EventTypeWarning, "SQLGenerationFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to generate create table SQL: %w", err)
		}

		// Generate SQL for attaching Parquet files
		attachSQL, err := r.SQLGen.AttachParquetSQL(table, table.Spec.Location)
		if err != nil {
			l.Error().Err(err).Msg("failed to generate attach SQL")
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
			l.Error().Err(err).
				Str("catalogRef", table.Spec.CatalogRef).
				Msg("failed to get catalog")
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

		// Create the job with retries
		var job *batchv1.Job
		createOp := func(ctx context.Context) error {
			var err error
			job, err = r.JobManager.CreateJob(op)
			return err
		}

		if err := retry.Do(ctx, createOp, r.RetryConfig); err != nil {
			l.Error().Err(err).Msg("failed to create job")
			table.Status.Phase = ducklakev1alpha1.TablePhaseFailed
			metrics.RecordTableOperation("create_job", "failed")
			r.Recorder.Event(table, corev1.EventTypeWarning, "JobCreationFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to create job: %w", err)
		}
		metrics.RecordTableOperation("create_job", "succeeded")

		// Set controller reference
		if err := ctrl.SetControllerReference(table, job, r.Scheme); err != nil {
			l.Error().Err(err).Msg("failed to set controller reference")
			r.Recorder.Event(table, corev1.EventTypeWarning, "SetControllerReferenceFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to set controller reference: %w", err)
		}

		// Create the job
		if err := r.Create(ctx, job); err != nil {
			l.Error().Err(err).Msg("failed to create job")
			r.Recorder.Event(table, corev1.EventTypeWarning, "JobCreationFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to create job: %w", err)
		}

		// Update status
		table.Status.Phase = ducklakev1alpha1.TablePhasePending
		table.Status.ObservedGeneration = table.Generation
		if err := r.Status().Update(ctx, table); err != nil {
			l.Error().Err(err).Msg("failed to update status")
			r.Recorder.Event(table, corev1.EventTypeWarning, "StatusUpdateFailed", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
		}

		l.Info().
			Str("jobName", job.Name).
			Str("phase", string(table.Status.Phase)).
			Msg("created table creation job")

		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// If we get here, there's an existing job that's still running
	l.Debug().
		Str("jobName", latestJob.Name).
		Str("phase", string(table.Status.Phase)).
		Msg("job still running")

	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// handleDeletion handles the deletion of a DuckLakeTable
func (r *DuckLakeTableReconciler) handleDeletion(ctx context.Context, table *ducklakev1alpha1.DuckLakeTable) (ctrl.Result, error) {
	l := logger.WithValues(r.Logger,
		"controller", "DuckLakeTable",
		"namespace", table.Namespace,
		"name", table.Name)
	ctx = logger.WithContext(ctx, l)

	l.Info().Msg("handling table deletion")

	// Generate SQL for dropping table
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s;", table.Spec.Name)

	// Get the catalog
	catalog := &ducklakev1alpha1.DuckLakeCatalog{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: table.Namespace,
		Name:      table.Spec.CatalogRef,
	}, catalog); err != nil {
		l.Error().Err(err).Msg("failed to get catalog")
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
		l.Error().Err(err).Msg("failed to create cleanup job")
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
		l.Error().Err(err).Msg("failed to remove finalizer")
		return ctrl.Result{}, fmt.Errorf("failed to remove finalizer: %w", err)
	}

	l.Info().Msg("handled table deletion successfully")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DuckLakeTableReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Initialize with default pod template
	r.PodTemplate = config.DefaultPodTemplateConfig()

	// Initialize logger if not already set
	if r.Logger == nil {
		logger := zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339Nano,
		}).With().Timestamp().Logger()
		r.Logger = &logger
	}

	// Initialize retry config
	r.RetryConfig = retry.DefaultRetryConfig

	// Initialize lifecycle manager
	kubeClient, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}
	r.Lifecycle = duckdb.NewLifecycleManager(r.Client, kubeClient)

	// Initialize consistency checker
	s3Config, err := storage.GetS3Config(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get S3 config: %w", err)
	}
	s3Client := s3.NewFromConfig(s3Config)

	r.Consistency = storage.NewConsistencyChecker(r.Client, s3Client).
		WithTimeout(storage.DefaultTimeout).
		WithInterval(storage.DefaultInterval).
		WithRetries(storage.DefaultRetries)

	// Start job cleanup goroutine
	cleanupCtx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		defer cancel()

		for {
			select {
			case <-mgr.Elected():
				// Only run cleanup when we're the leader
				if err := r.Lifecycle.CleanupJobs(cleanupCtx, "", labels.SelectorFromSet(labels.Set{
					"app.kubernetes.io/managed-by": "featherman",
				})); err != nil {
					r.Logger.Error().Err(err).Msg("failed to cleanup jobs")
				}
			case <-cleanupCtx.Done():
				return
			case <-ticker.C:
				// Run cleanup on ticker
				if err := r.Lifecycle.CleanupJobs(cleanupCtx, "", labels.SelectorFromSet(labels.Set{
					"app.kubernetes.io/managed-by": "featherman",
				})); err != nil {
					r.Logger.Error().Err(err).Msg("failed to cleanup jobs")
				}
			}
		}
	}()

	// Add cleanup context cancellation to manager
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		<-ctx.Done()
		cancel()
		return nil
	})); err != nil {
		return fmt.Errorf("failed to add cleanup context cancellation: %w", err)
	}

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
		} else {
			r.Logger.Error().Err(err).Msg("failed to load pod template config")
			return nil
		}
	}

	// List all DuckLakeTables
	var tables ducklakev1alpha1.DuckLakeTableList
	if err := r.List(ctx, &tables); err != nil {
		r.Logger.Error().Err(err).Msg("failed to list tables")
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
