package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	once sync.Once

	// Table operation metrics
	tableOperationCounter *prometheus.CounterVec

	// Job metrics
	jobDurationHistogram *prometheus.HistogramVec
	jobExecutionTotal    *prometheus.CounterVec

	// Reconciliation metrics
	reconciliationTotal    *prometheus.CounterVec
	reconciliationDuration *prometheus.HistogramVec
	reconciliationErrors   *prometheus.CounterVec

	// Pool metrics
	poolSizeCurrent *prometheus.GaugeVec
	poolSizeDesired *prometheus.GaugeVec
	poolPodsIdle    *prometheus.GaugeVec
	poolPodsBusy    *prometheus.GaugeVec
	poolQueueLength *prometheus.GaugeVec

	// Pool performance metrics
	poolQueryDuration  *prometheus.HistogramVec
	poolWaitDuration   *prometheus.HistogramVec
	poolPodAcquisition *prometheus.CounterVec
	poolPodRelease     *prometheus.CounterVec

	// Pool lifecycle metrics
	poolPodsCreated   *prometheus.CounterVec
	poolPodsDeleted   *prometheus.CounterVec
	poolScalingEvents *prometheus.CounterVec

	// Storage metrics
	s3OperationTotal    *prometheus.CounterVec
	s3OperationDuration *prometheus.HistogramVec
	catalogSize         *prometheus.GaugeVec

	// Backup metrics
	backupTotal    *prometheus.CounterVec
	backupDuration *prometheus.HistogramVec
)

// InitMetrics initializes all metrics - should be called once at startup
func InitMetrics(registry prometheus.Registerer) {
	once.Do(func() {
		// Table operation metrics
		tableOperationCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ducklake_table_operations_total",
				Help: "Total number of table operations",
			},
			[]string{"operation", "status"},
		)

		// Job metrics
		jobDurationHistogram = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "ducklake_job_duration_seconds",
				Help:    "Duration of DuckDB jobs in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"job_type", "status"},
		)

		jobExecutionTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ducklake_job_execution_total",
				Help: "Total number of job executions",
			},
			[]string{"status"},
		)

		// Reconciliation metrics
		reconciliationTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ducklake_reconciliation_total",
				Help: "Total number of reconciliations",
			},
			[]string{"resource", "result"},
		)

		reconciliationDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "ducklake_reconciliation_duration_seconds",
				Help:    "Reconciliation duration in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"resource"},
		)

		reconciliationErrors = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ducklake_reconciliation_errors_total",
				Help: "Total number of reconciliation errors",
			},
			[]string{"resource", "error_type"},
		)

		// Pool size metrics
		poolSizeCurrent = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ducklake_pool_size_current",
				Help: "Current number of pods in the pool",
			},
			[]string{"pool_name", "namespace"},
		)

		poolSizeDesired = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ducklake_pool_size_desired",
				Help: "Desired number of pods in the pool",
			},
			[]string{"pool_name", "namespace"},
		)

		poolPodsIdle = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ducklake_pool_pods_idle",
				Help: "Number of idle pods in the pool",
			},
			[]string{"pool_name", "namespace"},
		)

		poolPodsBusy = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ducklake_pool_pods_busy",
				Help: "Number of busy pods in the pool",
			},
			[]string{"pool_name", "namespace"},
		)

		poolQueueLength = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ducklake_pool_queue_length",
				Help: "Number of pending requests in the queue",
			},
			[]string{"pool_name", "namespace"},
		)

		// Pool performance metrics
		poolQueryDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "ducklake_pool_query_duration_seconds",
				Help:    "Query execution time in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"pool_name", "namespace"},
		)

		poolWaitDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "ducklake_pool_wait_duration_seconds",
				Help:    "Time waiting for a pod in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"pool_name", "namespace"},
		)

		poolPodAcquisition = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ducklake_pool_pod_acquisitions_total",
				Help: "Total number of pod acquisitions",
			},
			[]string{"pool_name", "namespace"},
		)

		poolPodRelease = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ducklake_pool_pod_releases_total",
				Help: "Total number of pod releases",
			},
			[]string{"pool_name", "namespace"},
		)

		// Pool lifecycle metrics
		poolPodsCreated = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ducklake_pool_pods_created_total",
				Help: "Total number of pods created",
			},
			[]string{"pool_name", "namespace"},
		)

		poolPodsDeleted = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ducklake_pool_pods_deleted_total",
				Help: "Total number of pods deleted",
			},
			[]string{"pool_name", "namespace"},
		)

		poolScalingEvents = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ducklake_pool_scaling_events_total",
				Help: "Total number of scaling events",
			},
			[]string{"pool_name", "namespace", "direction"},
		)

		// Storage metrics
		s3OperationTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ducklake_s3_operation_total",
				Help: "Total number of S3 operations",
			},
			[]string{"operation", "status"},
		)

		s3OperationDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "ducklake_s3_operation_duration_seconds",
				Help:    "Duration of S3 operations in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation"},
		)

		catalogSize = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ducklake_catalog_size_bytes",
				Help: "Size of DuckDB catalog in bytes",
			},
			[]string{"catalog"},
		)

		// Backup metrics
		backupTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ducklake_backup_total",
				Help: "Total number of backup operations",
			},
			[]string{"catalog", "status"},
		)

		backupDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "ducklake_backup_duration_seconds",
				Help:    "Duration of backup operations in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"catalog"},
		)

		// Register all metrics
		registry.MustRegister(
			tableOperationCounter,
			jobDurationHistogram,
			jobExecutionTotal,
			reconciliationTotal,
			reconciliationDuration,
			reconciliationErrors,
			poolSizeCurrent,
			poolSizeDesired,
			poolPodsIdle,
			poolPodsBusy,
			poolQueueLength,
			poolQueryDuration,
			poolWaitDuration,
			poolPodAcquisition,
			poolPodRelease,
			poolPodsCreated,
			poolPodsDeleted,
			poolScalingEvents,
			s3OperationTotal,
			s3OperationDuration,
			catalogSize,
			backupTotal,
			backupDuration,
		)
	})
}

// RecordTableOperation records a table operation metric
func RecordTableOperation(operation, status string) {
	if tableOperationCounter != nil {
		tableOperationCounter.WithLabelValues(operation, status).Inc()
	}
}

// RecordJobDuration records job duration metric
func RecordJobDuration(jobType, status string, duration float64) {
	if jobDurationHistogram != nil {
		jobDurationHistogram.WithLabelValues(jobType, status).Observe(duration)
	}
}

// RecordJobExecution records job execution metric
func RecordJobExecution(status string, duration float64, operation string) {
	if jobExecutionTotal != nil {
		jobExecutionTotal.WithLabelValues(status).Inc()
	}
	if jobDurationHistogram != nil {
		jobDurationHistogram.WithLabelValues(operation, status).Observe(duration)
	}
}

// RecordReconciliation records reconciliation metrics
func RecordReconciliation(resource, result string, duration float64) {
	if reconciliationTotal != nil {
		reconciliationTotal.WithLabelValues(resource, result).Inc()
	}
	if reconciliationDuration != nil {
		reconciliationDuration.WithLabelValues(resource).Observe(duration)
	}
}

// RecordReconciliationError records reconciliation error metrics
func RecordReconciliationError(resource, errorType string) {
	if reconciliationErrors != nil {
		reconciliationErrors.WithLabelValues(resource, errorType).Inc()
	}
}

// RecordS3Operation records S3 operation metrics
func RecordS3Operation(operation, status string, duration float64) {
	if s3OperationTotal != nil {
		s3OperationTotal.WithLabelValues(operation, status).Inc()
	}
	if s3OperationDuration != nil {
		s3OperationDuration.WithLabelValues(operation).Observe(duration)
	}
}

// UpdateCatalogSize updates catalog size metric
func UpdateCatalogSize(catalog string, sizeBytes float64) {
	if catalogSize != nil {
		catalogSize.WithLabelValues(catalog).Set(sizeBytes)
	}
}

// RecordBackup records backup metrics
func RecordBackup(catalog, status string, duration float64) {
	if backupTotal != nil {
		backupTotal.WithLabelValues(catalog, status).Inc()
	}
	if backupDuration != nil {
		backupDuration.WithLabelValues(catalog).Observe(duration)
	}
}

// Pool metrics functions

// UpdatePoolMetrics updates pool size metrics
func UpdatePoolMetrics(poolName, namespace string, current, desired, idle, busy int32) {
	if poolSizeCurrent != nil {
		poolSizeCurrent.WithLabelValues(poolName, namespace).Set(float64(current))
	}
	if poolSizeDesired != nil {
		poolSizeDesired.WithLabelValues(poolName, namespace).Set(float64(desired))
	}
	if poolPodsIdle != nil {
		poolPodsIdle.WithLabelValues(poolName, namespace).Set(float64(idle))
	}
	if poolPodsBusy != nil {
		poolPodsBusy.WithLabelValues(poolName, namespace).Set(float64(busy))
	}
}

// RecordPoolQueueLength records the current queue length
func RecordPoolQueueLength(poolName, namespace string, length int) {
	if poolQueueLength != nil {
		poolQueueLength.WithLabelValues(poolName, namespace).Set(float64(length))
	}
}

// RecordPodAcquired records a pod acquisition
func RecordPodAcquired(poolName, namespace string) {
	if poolPodAcquisition != nil {
		poolPodAcquisition.WithLabelValues(poolName, namespace).Inc()
	}
}

// RecordPodReleased records a pod release
func RecordPodReleased(poolName, namespace string) {
	if poolPodRelease != nil {
		poolPodRelease.WithLabelValues(poolName, namespace).Inc()
	}
}

// RecordQueryDuration records query execution time
func RecordQueryDuration(poolName, namespace string, seconds float64) {
	if poolQueryDuration != nil {
		poolQueryDuration.WithLabelValues(poolName, namespace).Observe(seconds)
	}
}

// RecordWaitDuration records time waiting for a pod
func RecordWaitDuration(poolName, namespace string, seconds float64) {
	if poolWaitDuration != nil {
		poolWaitDuration.WithLabelValues(poolName, namespace).Observe(seconds)
	}
}

// RecordPodCreated records a pod creation
func RecordPodCreated(poolName, namespace string) {
	if poolPodsCreated != nil {
		poolPodsCreated.WithLabelValues(poolName, namespace).Inc()
	}
}

// RecordPodDeleted records a pod deletion
func RecordPodDeleted(poolName, namespace string) {
	if poolPodsDeleted != nil {
		poolPodsDeleted.WithLabelValues(poolName, namespace).Inc()
	}
}

// RecordScalingEvent records a scaling event
func RecordScalingEvent(poolName, namespace, direction string) {
	if poolScalingEvents != nil {
		poolScalingEvents.WithLabelValues(poolName, namespace, direction).Inc()
	}
}
