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
	"k8s.io/client-go/rest"
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
	"github.com/TFMV/featherman/operator/internal/pool"
	"github.com/TFMV/featherman/operator/internal/retry"
	"github.com/TFMV/featherman/operator/internal/sql"
	"github.com/TFMV/featherman/operator/internal/storage"
	"github.com/TFMV/featherman/operator/pkg/materializer"
)

// DuckLakeTableReconciler reconciles a DuckLakeTable object
type DuckLakeTableReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Recorder     record.EventRecorder
	Logger       *zerolog.Logger
	JobManager   *duckdb.JobManager
	SQLGen       *sql.Generator
	Materializer *materializer.Runner
	PodTemplate  *config.PodTemplateConfig
	Consistency  *storage.ConsistencyChecker
	RetryConfig  retry.RetryConfig
	Lifecycle    *duckdb.LifecycleManager
	PoolManager  PoolManager
	Config       *rest.Config
	K8sClient    kubernetes.Interface
}

// PoolManager provides access to warm pod pools
type PoolManager interface {
	GetPoolManager(namespace, name string) (*pool.Manager, bool)
}

// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklaketables,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklaketables/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ducklake.featherman.dev,resources=ducklaketables/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch

// Reconcile handles DuckLakeTable resources
func (r *DuckLakeTableReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.FromContext(ctx).With().
		Str("controller", "DuckLakeTable").
		Str("namespace", req.Namespace).
		Str("name", req.Name).
		Logger()
	l := &log
	ctx = logger.WithContext(ctx, l)

	startTime := time.Now()
	defer func() {
		metrics.RecordJobDuration("reconcile", "completed", time.Since(startTime).Seconds())
	}()

	// Get the DuckLakeTable instance
	table := &ducklakev1alpha1.DuckLakeTable{}
	if err := r.Get(ctx, req.NamespacedName, table); err != nil {
		if errors.IsNotFound(err) {
			l.Debug().Msg("DuckLakeTable not found, ignoring")
			return ctrl.Result{}, nil
		}
		l.Error().Err(err).Msg("Failed to get DuckLakeTable")
		metrics.RecordTableOperation("get", "failed")
		return ctrl.Result{}, fmt.Errorf("failed to get DuckLakeTable: %w", err)
	}

	l.Info().
		Str("phase", string(table.Status.Phase)).
		Bool("deletion", !table.DeletionTimestamp.IsZero()).
		Msg("Starting reconciliation")

	// List all jobs for this table
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(table.Namespace), client.MatchingLabels{
		"ducklake.featherman.dev/table": table.Name,
	}); err != nil {
		l.Error().Err(err).Msg("Failed to list jobs")
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
		l.Debug().
			Str("jobName", latestJob.Name).
			Time("creationTime", latestJob.CreationTimestamp.Time).
			Msg("Processing latest job")

		state, err := r.Lifecycle.GetJobState(ctx, latestJob)
		if err != nil {
			l.Error().Err(err).Msg("Failed to get job state")
			return ctrl.Result{}, fmt.Errorf("failed to get job state: %w", err)
		}

		if state.Phase == duckdb.JobPhaseComplete {
			opType := latestJob.Labels["app.kubernetes.io/operation"]
			l.Info().
				Str("jobName", latestJob.Name).
				Time("completionTime", state.CompletionTime.Time).
				Msg("Job completed successfully")

			if opType == string(duckdb.OperationTypeWrite) {
				// Verify Parquet file consistency with retries
				l.Debug().Msg("Verifying Parquet file consistency")
				verifyOp := func(ctx context.Context) error {
					return r.Consistency.VerifyParquetConsistency(ctx, table)
				}

				if err := retry.Do(ctx, verifyOp, r.RetryConfig); err != nil {
					l.Error().Err(err).Msg("Failed to verify Parquet consistency")
					table.Status.Phase = ducklakev1alpha1.TablePhaseFailed
					metrics.RecordTableOperation("verify_consistency", "failed")
					r.Recorder.Event(table, corev1.EventTypeWarning, "ConsistencyCheckFailed", err.Error())
					if err := r.Status().Update(ctx, table); err != nil {
						l.Error().Err(err).Msg("Failed to update status")
						return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
					}
					return ctrl.Result{}, fmt.Errorf("failed to verify Parquet consistency: %w", err)
				}
				metrics.RecordTableOperation("verify_consistency", "succeeded")
				l.Info().Msg("Parquet file consistency verified")

				// List Parquet files to update status
				l.Debug().Msg("Listing Parquet files")
				listOp := func(ctx context.Context) error {
					objects, err := r.Consistency.ListParquetFiles(ctx, table)
					if err != nil {
						return err
					}

					var totalBytes int64
					for _, obj := range objects {
						totalBytes += *obj.Size
					}

					table.Status.Phase = ducklakev1alpha1.TablePhaseSucceeded
					table.Status.LastModified = &metav1.Time{Time: state.CompletionTime.Time}
					table.Status.BytesWritten = totalBytes

					return nil
				}

				if err := retry.Do(ctx, listOp, r.RetryConfig); err != nil {
					l.Error().Err(err).Msg("Failed to list Parquet files")
					r.Recorder.Event(table, corev1.EventTypeWarning, "ListParquetFilesFailed", err.Error())
					return ctrl.Result{}, fmt.Errorf("failed to list Parquet files: %w", err)
				}

				metrics.RecordTableOperation("write", "succeeded")

				if err := r.Status().Update(ctx, table); err != nil {
					l.Error().Err(err).Msg("Failed to update status")
					r.Recorder.Event(table, corev1.EventTypeWarning, "StatusUpdateFailed", err.Error())
					return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
				}

				if table.Spec.MaterializeTo != nil && table.Spec.MaterializeTo.Enabled {
					matSQL, err := r.SQLGen.GenerateMaterializationSQL(table)
					if err != nil {
						l.Error().Err(err).Msg("failed to generate materialization SQL")
						return ctrl.Result{}, fmt.Errorf("failed to generate materialization SQL: %w", err)
					}

					catalog := &ducklakev1alpha1.DuckLakeCatalog{}
					if err := r.Get(ctx, client.ObjectKey{Namespace: table.Namespace, Name: table.Spec.CatalogRef}, catalog); err != nil {
						l.Error().Err(err).Msg("failed to get catalog for materialization")
						return ctrl.Result{}, fmt.Errorf("failed to get catalog: %w", err)
					}

					op := &duckdb.Operation{
						Type:          duckdb.OperationTypeRead,
						SQL:           matSQL,
						Table:         table,
						Catalog:       catalog,
						PodTemplate:   r.PodTemplate,
						JobNameSuffix: "materialize",
					}

					var mJob *batchv1.Job
					createOp := func(ctx context.Context) error {
						var err error
						mJob, err = r.JobManager.CreateJob(op)
						return err
					}

					if err := retry.Do(ctx, createOp, r.RetryConfig); err != nil {
						l.Error().Err(err).Msg("failed to create materialization job")
						return ctrl.Result{}, fmt.Errorf("failed to create materialization job: %w", err)
					}

					if err := ctrl.SetControllerReference(table, mJob, r.Scheme); err != nil {
						l.Error().Err(err).Msg("failed to set controller reference on materialization job")
						return ctrl.Result{}, fmt.Errorf("failed to set controller reference: %w", err)
					}

					if err := r.Create(ctx, mJob); err != nil {
						l.Error().Err(err).Msg("failed to create materialization job")
						return ctrl.Result{}, fmt.Errorf("failed to create materialization job: %w", err)
					}

					l.Info().Str("jobName", mJob.Name).Msg("materialization job created")
				}

				logs, err := r.Lifecycle.GetJobLogs(ctx, latestJob)
				if err != nil {
					l.Warn().Err(err).Msg("Failed to get job logs")
				} else {
					for podName, log := range logs {
						l.Debug().Str("pod", podName).Str("logs", log).Msg("Job pod logs")
					}
				}

				l.Info().Int64("bytesWritten", table.Status.BytesWritten).Time("lastModified", table.Status.LastModified.Time).Msg("Table reconciliation completed successfully")

				return ctrl.Result{}, nil
			}

			// Materialization job completed
			if table.Spec.MaterializeTo != nil && table.Spec.MaterializeTo.Enabled {
				table.Status.Materialization = &ducklakev1alpha1.MaterializationStatus{
					LastRun:    &metav1.Time{Time: state.CompletionTime.Time},
					Duration:   metav1.Duration{Duration: state.CompletionTime.Sub(state.StartTime.Time)},
					OutputPath: fmt.Sprintf("s3://%s/%s", table.Spec.MaterializeTo.Destination.Bucket, table.Spec.MaterializeTo.Destination.Prefix),
				}
				if err := r.Status().Update(ctx, table); err != nil {
					l.Error().Err(err).Msg("Failed to update materialization status")
					return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
				}
			}

			metrics.RecordTableOperation("read", "succeeded")

			logs, err := r.Lifecycle.GetJobLogs(ctx, latestJob)
			if err != nil {
				l.Warn().Err(err).Msg("Failed to get job logs")
			} else {
				for podName, log := range logs {
					l.Debug().Str("pod", podName).Str("logs", log).Msg("Job pod logs")
				}
			}

			l.Info().Msg("Materialization completed successfully")

			return ctrl.Result{}, nil
		} else if state.Phase == duckdb.JobPhaseFailed {
			l.Warn().
				Str("jobName", latestJob.Name).
				Str("message", state.Message).
				Msg("Job failed")

			table.Status.Phase = ducklakev1alpha1.TablePhaseFailed

			// Get job logs for debugging
			logs, err := r.Lifecycle.GetJobLogs(ctx, latestJob)
			if err != nil {
				l.Warn().Err(err).Msg("Failed to get job logs")
			} else {
				for podName, log := range logs {
					l.Error().
						Str("pod", podName).
						Str("logs", log).
						Msg("Job failed, pod logs")
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
				l.Error().Err(err).Msg("Failed to update status")
				r.Recorder.Event(table, corev1.EventTypeWarning, "StatusUpdateFailed", err.Error())
				return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
			}

			return ctrl.Result{}, fmt.Errorf("job failed: %s", state.Message)
		}
	}

	// Initialize status if needed
	if table.Status.Phase == "" {
		l.Debug().Msg("Initializing table status")
		table.Status.Phase = ducklakev1alpha1.TablePhasePending
		if err := r.Status().Update(ctx, table); err != nil {
			l.Error().Err(err).Msg("Failed to update status")
			metrics.RecordTableOperation("status_update", "failed")
			return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
		}
		metrics.RecordTableOperation("status_update", "succeeded")
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(table, "ducklaketable.featherman.dev") {
		l.Debug().Msg("Adding finalizer")
		controllerutil.AddFinalizer(table, "ducklaketable.featherman.dev")
		if err := r.Update(ctx, table); err != nil {
			l.Error().Err(err).Msg("Failed to add finalizer")
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer: %w", err)
		}
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

		// Try to use warm pod if available
		if r.PoolManager != nil {
			poolName := table.Annotations["ducklake.featherman.dev/pool"]
			if poolName == "" {
				poolName = "default-pool" // Use default pool if not specified
			}

			if poolMgr, ok := r.PoolManager.GetPoolManager(table.Namespace, poolName); ok {
				l.Info().Str("pool", poolName).Msg("attempting to use warm pod")

				// Request a warm pod
				resources := corev1.ResourceRequirements{}
				if r.PodTemplate != nil && r.PodTemplate.Resources != nil {
					resources = *r.PodTemplate.Resources
				}
				pod, err := poolMgr.RequestPod(ctx, catalog.Name,
					resources,
					30*time.Second)

				if err == nil && pod != nil {
					l.Info().Str("pod", pod.Name).Msg("acquired warm pod")

					// Create executor
					kubeClient, err := kubernetes.NewForConfig(r.Config)
					if err != nil {
						l.Error().Err(err).Msg("failed to create kubernetes client")
						_ = poolMgr.ReleasePod(ctx, pod.Name)
					} else {
						executor := pool.NewExecutor(kubeClient, r.Config, l)

						// Execute SQL
						startTime := time.Now()
						output, err := executor.ExecuteQuery(ctx, pod, op.SQL, 5*time.Minute)
						duration := time.Since(startTime)

						// Release pod
						_ = poolMgr.ReleasePod(ctx, pod.Name)

						if err != nil {
							l.Error().Err(err).Msg("warm pod execution failed, falling back to job")
							metrics.RecordTableOperation("warm_pod_exec", "failed")
							// Fall through to job creation
						} else {
							l.Info().
								Str("pod", pod.Name).
								Dur("duration", duration).
								Int("outputBytes", len(output)).
								Msg("warm pod execution succeeded")

							metrics.RecordTableOperation("warm_pod_exec", "succeeded")

							// Update status
							table.Status.Phase = ducklakev1alpha1.TablePhaseSucceeded
							table.Status.LastModified = &metav1.Time{Time: time.Now()}
							table.Status.ObservedGeneration = table.Generation
							if err := r.Status().Update(ctx, table); err != nil {
								l.Error().Err(err).Msg("failed to update status")
								return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
							}

							r.Recorder.Event(table, corev1.EventTypeNormal, "WarmPodExecuted",
								fmt.Sprintf("Successfully executed on warm pod %s", pod.Name))

							return ctrl.Result{}, nil
						}
					}
				} else {
					l.Warn().Err(err).Msg("failed to acquire warm pod, falling back to job")
					metrics.RecordTableOperation("warm_pod_request", "failed")
				}
			}
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
	l := logger.FromContext(ctx)

	l.Info().
		Str("table", table.Name).
		Str("namespace", table.Namespace).
		Msg("Starting DuckLakeTable deletion process")

	// Delete any associated jobs
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(table.Namespace), client.MatchingLabels{
		"ducklake.featherman.dev/table": table.Name,
	}); err != nil {
		l.Error().
			Err(err).
			Msg("Failed to list jobs during deletion")
		return ctrl.Result{}, fmt.Errorf("failed to list jobs: %w", err)
	}

	// Delete each job
	for i := range jobs.Items {
		job := &jobs.Items[i]
		l.Debug().
			Str("job", job.Name).
			Msg("Deleting associated job")
		if err := r.Delete(ctx, job); err != nil && !errors.IsNotFound(err) {
			l.Error().
				Err(err).
				Str("job", job.Name).
				Msg("Failed to delete job")
			return ctrl.Result{}, fmt.Errorf("failed to delete job %s: %w", job.Name, err)
		}
	}

	// Remove finalizer
	if controllerutil.ContainsFinalizer(table, "ducklaketable.featherman.dev") {
		l.Debug().Msg("Removing finalizer")
		controllerutil.RemoveFinalizer(table, "ducklaketable.featherman.dev")
		if err := r.Update(ctx, table); err != nil {
			l.Error().
				Err(err).
				Msg("Failed to remove finalizer")
			return ctrl.Result{}, fmt.Errorf("failed to remove finalizer: %w", err)
		}
		l.Info().Msg("Successfully removed finalizer")
	}

	l.Info().Msg("Successfully completed DuckLakeTable deletion")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DuckLakeTableReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Initialize with default pod template
	r.PodTemplate = config.DefaultPodTemplateConfig()

	// Store the config
	r.Config = mgr.GetConfig()
	if r.Materializer == nil {
		r.Materializer = materializer.NewRunner()
	}

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
	if r.K8sClient == nil {
		kubeClient, err := kubernetes.NewForConfig(mgr.GetConfig())
		if err != nil {
			return fmt.Errorf("failed to create kubernetes client: %w", err)
		}
		r.K8sClient = kubeClient
	}
	r.Lifecycle = duckdb.NewLifecycleManager(r.Client, r.K8sClient)

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
