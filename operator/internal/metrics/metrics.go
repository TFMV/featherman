package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// Reconciliation metrics
	reconciliationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "featherman_reconciliation_total",
			Help: "Total number of reconciliations per controller",
		},
		[]string{"controller", "result"},
	)

	reconciliationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "featherman_reconciliation_duration_seconds",
			Help:    "Duration of reconciliation in seconds",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
		},
		[]string{"controller"},
	)

	// Job metrics
	jobExecutionTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "featherman_job_execution_total",
			Help: "Total number of DuckDB job executions",
		},
		[]string{"status"},
	)

	jobDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "featherman_job_duration_seconds",
			Help:    "Duration of DuckDB jobs in seconds",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
		},
		[]string{"operation", "status"},
	)

	// Table operation metrics
	tableOperationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "featherman_table_operation_total",
			Help: "Total number of table operations",
		},
		[]string{"operation", "status"},
	)

	// Storage metrics
	s3OperationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "featherman_s3_operation_total",
			Help: "Total number of S3 operations",
		},
		[]string{"operation", "status"},
	)

	s3OperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "featherman_s3_operation_duration_seconds",
			Help:    "Duration of S3 operations in seconds",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
		},
		[]string{"operation"},
	)

	// Catalog metrics
	catalogSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "featherman_catalog_size_bytes",
			Help: "Size of DuckDB catalog files in bytes",
		},
		[]string{"catalog"},
	)

	// Backup metrics
	backupTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "featherman_backup_total",
			Help: "Total number of backup operations",
		},
		[]string{"catalog", "status"},
	)

	backupDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "featherman_backup_duration_seconds",
			Help:    "Duration of backup operations in seconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		},
		[]string{"catalog"},
	)
)

func init() {
	// Register metrics with the controller-runtime metrics registry
	metrics.Registry.MustRegister(
		reconciliationTotal,
		reconciliationDuration,
		jobExecutionTotal,
		jobDuration,
		tableOperationTotal,
		s3OperationTotal,
		s3OperationDuration,
		catalogSize,
		backupTotal,
		backupDuration,
	)
}

// RecordReconciliation records reconciliation metrics
func RecordReconciliation(controller string, success bool, duration float64) {
	result := "success"
	if !success {
		result = "failure"
	}
	reconciliationTotal.WithLabelValues(controller, result).Inc()
	reconciliationDuration.WithLabelValues(controller).Observe(duration)
}

// RecordJobExecution records job execution metrics
func RecordJobExecution(status string, duration float64, operation string) {
	jobExecutionTotal.WithLabelValues(status).Inc()
	jobDuration.WithLabelValues(operation, status).Observe(duration)
}

// RecordJobDuration records job duration metrics
func RecordJobDuration(operation, status string, duration float64) {
	jobDuration.WithLabelValues(operation, status).Observe(duration)
}

// RecordTableOperation records table operation metrics
func RecordTableOperation(operation, status string) {
	tableOperationTotal.WithLabelValues(operation, status).Inc()
}

// RecordS3Operation records S3 operation metrics
func RecordS3Operation(operation string, success bool, duration float64) {
	status := "success"
	if !success {
		status = "failure"
	}
	s3OperationTotal.WithLabelValues(operation, status).Inc()
	s3OperationDuration.WithLabelValues(operation).Observe(duration)
}

// SetCatalogSize sets the current catalog size
func SetCatalogSize(catalog string, sizeBytes float64) {
	catalogSize.WithLabelValues(catalog).Set(sizeBytes)
}

// RecordBackup records backup operation metrics
func RecordBackup(catalog string, success bool, duration float64) {
	status := "success"
	if !success {
		status = "failure"
	}
	backupTotal.WithLabelValues(catalog, status).Inc()
	backupDuration.WithLabelValues(catalog).Observe(duration)
}
