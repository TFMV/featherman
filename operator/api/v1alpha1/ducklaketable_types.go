/*
Copyright 2025 Thomas McGeehan.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// TablePhase represents the phase of a DuckLakeTable
type TablePhase string

const (
	// TablePhasePending indicates the table is being created
	TablePhasePending TablePhase = "Pending"
	// TablePhaseSucceeded indicates the table is ready
	TablePhaseSucceeded TablePhase = "Succeeded"
	// TablePhaseFailed indicates the table failed to create
	TablePhaseFailed TablePhase = "Failed"
)

// SQLType represents a SQL data type
type SQLType string

const (
	// SQLTypeInteger represents INTEGER type
	SQLTypeInteger SQLType = "INTEGER"
	// SQLTypeBigInt represents BIGINT type
	SQLTypeBigInt SQLType = "BIGINT"
	// SQLTypeDouble represents DOUBLE type
	SQLTypeDouble SQLType = "DOUBLE"
	// SQLTypeBoolean represents BOOLEAN type
	SQLTypeBoolean SQLType = "BOOLEAN"
	// SQLTypeVarChar represents VARCHAR type
	SQLTypeVarChar SQLType = "VARCHAR"
	// SQLTypeDate represents DATE type
	SQLTypeDate SQLType = "DATE"
	// SQLTypeTimestamp represents TIMESTAMP type
	SQLTypeTimestamp SQLType = "TIMESTAMP"
	// SQLTypeDecimal represents DECIMAL type
	SQLTypeDecimal SQLType = "DECIMAL"
)

// CompressionType represents a Parquet compression type
type CompressionType string

const (
	// CompressionZSTD represents ZSTD compression
	CompressionZSTD CompressionType = "ZSTD"
	// CompressionSnappy represents Snappy compression
	CompressionSnappy CompressionType = "SNAPPY"
)

// TableMode represents the table write mode
type TableMode string

const (
	// TableModeAppend represents append mode
	TableModeAppend TableMode = "append"
	// TableModeOverwrite represents overwrite mode
	TableModeOverwrite TableMode = "overwrite"
)

// ColumnDefinition defines a column in the table
type ColumnDefinition struct {
	// Name is the column name
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=^[a-zA-Z][a-zA-Z0-9_]*$
	Name string `json:"name"`

	// Type is the SQL data type
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=INTEGER;BIGINT;DOUBLE;BOOLEAN;VARCHAR;DATE;TIMESTAMP;DECIMAL
	Type SQLType `json:"type"`

	// Nullable specifies if the column can contain NULL values
	// +optional
	Nullable bool `json:"nullable,omitempty"`

	// Comment provides documentation for the column
	// +optional
	Comment string `json:"comment,omitempty"`
}

// ParquetFormat defines the Parquet file format configuration
type ParquetFormat struct {
	// Compression specifies the compression algorithm
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=ZSTD;SNAPPY
	Compression CompressionType `json:"compression"`

	// Partitioning specifies the partition columns
	// +optional
	Partitioning []string `json:"partitioning,omitempty"`
}

// MaterializationFormat defines the output format for materialization
type MaterializationFormat struct {
	// Type specifies the file format. Currently only "parquet" is supported
	// +kubebuilder:validation:Enum=parquet
	Type string `json:"type"`

	// Compression specifies the compression algorithm
	// +optional
	// +kubebuilder:validation:Enum=ZSTD;SNAPPY
	Compression CompressionType `json:"compression,omitempty"`

	// PartitionBy specifies the partition columns
	// +optional
	PartitionBy []string `json:"partitionBy,omitempty"`
}

// MaterializationDestination defines the object store destination
type MaterializationDestination struct {
	// Bucket is the target bucket for the materialized dataset
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket"`

	// Prefix is the object prefix for the materialized files
	// +optional
	Prefix string `json:"prefix,omitempty"`
}

// MaterializeToSpec configures materialization of query results
type MaterializeToSpec struct {
	// Enabled determines whether materialization is active
	// +kubebuilder:default=false
	Enabled bool `json:"enabled"`

	// Name is the logical name of the materialization
	// +optional
	Name string `json:"name,omitempty"`

	// SQL is the query used to produce the dataset
	// +optional
	SQL string `json:"sql,omitempty"`

	// Schedule is an optional cron expression for refresh
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// Format describes the output file format
	// +optional
	Format MaterializationFormat `json:"format,omitempty"`

	// Destination specifies where the files are written
	// +optional
	Destination MaterializationDestination `json:"destination,omitempty"`
}

// MaterializationStatus tracks the outcome of a materialization run
type MaterializationStatus struct {
	// LastRun indicates the time of the last materialization
	// +optional
	LastRun *metav1.Time `json:"lastRun,omitempty"`

	// Duration is the execution time of the last materialization
	// +optional
	Duration metav1.Duration `json:"duration,omitempty"`

	// OutputPath is the object store prefix of the materialized files
	// +optional
	OutputPath string `json:"outputPath,omitempty"`

	// RowCount is the number of rows produced in the last run
	// +optional
	RowCount int64 `json:"rowCount,omitempty"`
}

// DuckLakeTableSpec defines the desired state of DuckLakeTable
type DuckLakeTableSpec struct {
	// Name is the table name
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=^[a-zA-Z][a-zA-Z0-9_]*$
	Name string `json:"name"`

	// Columns defines the table columns
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Columns []ColumnDefinition `json:"columns"`

	// Format specifies the Parquet format configuration
	// +kubebuilder:validation:Required
	Format ParquetFormat `json:"format"`

	// Location is the object store path for Parquet files
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=^[a-zA-Z0-9-_./]+$
	Location string `json:"location"`

	// TTLDays specifies the data retention period in days
	// +optional
	// +kubebuilder:validation:Minimum=1
	TTLDays *int `json:"ttlDays,omitempty"`

	// Mode specifies the table write mode
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=append;overwrite
	Mode TableMode `json:"mode"`

	// Comment provides documentation for the table
	// +optional
	Comment string `json:"comment,omitempty"`

	// CatalogRef references the DuckLakeCatalog this table belongs to
	// +kubebuilder:validation:Required
	CatalogRef string `json:"catalogRef"`

	// ObjectStore defines the S3-compatible storage configuration
	// If not specified, the configuration from the referenced catalog will be used
	// +optional
	ObjectStore *ObjectStoreSpec `json:"objectStore,omitempty"`

	// MaterializeTo configures optional view materialization
	// +optional
       MaterializeTo *MaterializeToSpec `json:"materializeTo,omitempty"`

       // RestoreVersion, if set, restores the table to a historical version
       // +optional
       RestoreVersion string `json:"restoreVersion,omitempty"`

       // Versioning controls automatic metadata snapshotting
       // +optional
       Versioning *VersioningSpec `json:"versioning,omitempty"`
}

// DuckLakeTableStatus defines the observed state of DuckLakeTable
type DuckLakeTableStatus struct {
	// Phase is the current phase of the table
	// +kubebuilder:validation:Enum=Pending;Succeeded;Failed
	Phase TablePhase `json:"phase,omitempty"`

	// LastAppliedSnapshot is the latest successful snapshot ID
	// +optional
	LastAppliedSnapshot string `json:"lastAppliedSnapshot,omitempty"`

	// BytesWritten is the total bytes written
	// +optional
	BytesWritten int64 `json:"bytesWritten,omitempty"`

	// LastModified is the last modification timestamp
	// +optional
	LastModified *metav1.Time `json:"lastModified,omitempty"`

	// ObservedGeneration is the last generation that was acted on
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the table's state
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Materialization holds information about the last materialization run
	// +optional
       Materialization *MaterializationStatus `json:"materialization,omitempty"`

       // VersionHistory lists recent table versions
       // +optional
       VersionHistory []VersionEntry `json:"versionHistory,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Mode",type="string",JSONPath=".spec.mode"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// DuckLakeTable is the Schema for the ducklaketables API
type DuckLakeTable struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DuckLakeTableSpec   `json:"spec,omitempty"`
	Status DuckLakeTableStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DuckLakeTableList contains a list of DuckLakeTable
type DuckLakeTableList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DuckLakeTable `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DuckLakeTable{}, &DuckLakeTableList{})
}
