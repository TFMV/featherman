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

// ColumnDefinition defines a column in the table
type ColumnDefinition struct {
	// Name of the column
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=[a-zA-Z][a-zA-Z0-9_]*
	Name string `json:"name"`

	// Type is the SQL data type of the column
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=INTEGER;BIGINT;DOUBLE;BOOLEAN;VARCHAR;DATE;TIMESTAMP;DECIMAL
	Type string `json:"type"`

	// Nullable specifies if the column can contain NULL values
	// +optional
	Nullable bool `json:"nullable,omitempty"`

	// Comment provides documentation for the column
	// +optional
	Comment string `json:"comment,omitempty"`
}

// ParquetFormat defines the Parquet file format configuration
type ParquetFormat struct {
	// Compression algorithm to use
	// +kubebuilder:validation:Enum=SNAPPY;ZSTD;NONE
	// +optional
	Compression string `json:"compression,omitempty"`

	// RowGroupSize specifies the size of row groups in bytes
	// +optional
	// +kubebuilder:validation:Minimum=1048576
	RowGroupSize int64 `json:"rowGroupSize,omitempty"`
}

// DuckLakeTableSpec defines the desired state of DuckLakeTable
type DuckLakeTableSpec struct {
	// CatalogRef references the DuckLakeCatalog this table belongs to
	// +kubebuilder:validation:Required
	CatalogRef string `json:"catalogRef"`

	// Name is the name of the table
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=[a-zA-Z][a-zA-Z0-9_]*
	Name string `json:"name"`

	// Columns defines the schema of the table
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Columns []ColumnDefinition `json:"columns"`

	// Partitioning defines the partition columns
	// +optional
	Partitioning []string `json:"partitioning,omitempty"`

	// Format specifies the Parquet format configuration
	// +optional
	Format *ParquetFormat `json:"format,omitempty"`

	// TTLDays specifies the number of days to retain data
	// +optional
	// +kubebuilder:validation:Minimum=1
	TTLDays *int32 `json:"ttlDays,omitempty"`

	// Mode specifies how to handle existing data
	// +kubebuilder:validation:Enum=append;overwrite
	// +optional
	Mode string `json:"mode,omitempty"`

	// Comment provides documentation for the table
	// +optional
	Comment string `json:"comment,omitempty"`
}

// TablePhase represents the current phase of the table
// +kubebuilder:validation:Enum=Pending;Creating;Ready;Failed
type TablePhase string

const (
	// TablePhasePending means the table is being initialized
	TablePhasePending TablePhase = "Pending"
	// TablePhaseCreating means the table is being created
	TablePhaseCreating TablePhase = "Creating"
	// TablePhaseReady means the table is ready for use
	TablePhaseReady TablePhase = "Ready"
	// TablePhaseFailed means the table creation failed
	TablePhaseFailed TablePhase = "Failed"
)

// DuckLakeTableStatus defines the observed state of DuckLakeTable
type DuckLakeTableStatus struct {
	// Phase represents the current phase of the table
	// +optional
	Phase TablePhase `json:"phase,omitempty"`

	// LastAppliedSnapshot is the ID of the last successful snapshot
	// +optional
	LastAppliedSnapshot string `json:"lastAppliedSnapshot,omitempty"`

	// BytesWritten is the total bytes written to this table
	// +optional
	BytesWritten int64 `json:"bytesWritten,omitempty"`

	// LastModified is when the table was last modified
	// +optional
	LastModified *metav1.Time `json:"lastModified,omitempty"`

	// Conditions represent the latest available observations of the table's state
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the last generation that was acted on
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Catalog",type="string",JSONPath=".spec.catalogRef"
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
