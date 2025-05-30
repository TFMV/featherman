package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// TableOperations tracks table operations (create/update/delete)
	TableOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ducklake_table_operations_total",
			Help: "Number of table operations by type and status",
		},
		[]string{"operation", "status"},
	)

	// JobDuration tracks job execution time
	JobDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ducklake_job_duration_seconds",
			Help:    "Duration of DuckDB jobs",
			Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600},
		},
		[]string{"type", "status"},
	)

	// StorageUsage tracks Parquet file sizes
	StorageUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ducklake_storage_bytes",
			Help: "Storage usage in bytes by table",
		},
		[]string{"table", "type"}, // type can be "parquet" or "catalog"
	)

	// CatalogBackupStatus tracks backup operations
	CatalogBackupStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ducklake_catalog_backup_status",
			Help: "Status of the last backup operation (1 = success, 0 = failure)",
		},
		[]string{"catalog"},
	)

	// TablePartitionCount tracks number of partitions
	TablePartitionCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ducklake_table_partition_count",
			Help: "Number of partitions in a table",
		},
		[]string{"table"},
	)
)

func init() {
	// Register metrics with the controller-runtime metrics registry
	metrics.Registry.MustRegister(
		TableOperations,
		JobDuration,
		StorageUsage,
		CatalogBackupStatus,
		TablePartitionCount,
	)
}

// RecordTableOperation records a table operation metric
func RecordTableOperation(operation, status string) {
	TableOperations.WithLabelValues(operation, status).Inc()
}

// RecordJobDuration records a job duration metric
func RecordJobDuration(jobType, status string, duration float64) {
	JobDuration.WithLabelValues(jobType, status).Observe(duration)
}

// UpdateStorageUsage updates storage usage metrics
func UpdateStorageUsage(table, storageType string, bytes float64) {
	StorageUsage.WithLabelValues(table, storageType).Set(bytes)
}

// UpdateCatalogBackupStatus updates catalog backup status
func UpdateCatalogBackupStatus(catalog string, success bool) {
	value := 0.0
	if success {
		value = 1.0
	}
	CatalogBackupStatus.WithLabelValues(catalog).Set(value)
}

// UpdateTablePartitionCount updates partition count for a table
func UpdateTablePartitionCount(table string, count float64) {
	TablePartitionCount.WithLabelValues(table).Set(count)
}
