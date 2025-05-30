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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ObjectStoreSpec defines the configuration for S3-compatible object storage
type ObjectStoreSpec struct {
	// Endpoint is the S3-compatible endpoint URL
	// +kubebuilder:validation:Required
	Endpoint string `json:"endpoint"`

	// Bucket is the name of the S3 bucket to use
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket"`

	// Region is the S3 region (optional for some providers)
	// +optional
	Region string `json:"region,omitempty"`

	// CredentialsSecret references the secret containing AWS credentials
	// +kubebuilder:validation:Required
	CredentialsSecret corev1.LocalObjectReference `json:"credentialsSecret"`
}

// EncryptionSpec defines the encryption configuration for the catalog
type EncryptionSpec struct {
	// Provider specifies the encryption provider (e.g., "aws-kms")
	// +kubebuilder:validation:Enum=aws-kms;none
	Provider string `json:"provider"`

	// KeyID is the identifier for the encryption key
	// +optional
	KeyID string `json:"keyId,omitempty"`
}

// BackupPolicySpec defines the backup configuration
type BackupPolicySpec struct {
	// Schedule in Cron format
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// RetentionDays specifies how long to keep backups
	// +optional
	// +kubebuilder:validation:Minimum=1
	RetentionDays int32 `json:"retentionDays,omitempty"`
}

// DuckLakeCatalogSpec defines the desired state of DuckLakeCatalog
type DuckLakeCatalogSpec struct {
	// StorageClass specifies the storage class to use for the catalog PVC
	// +kubebuilder:validation:Required
	StorageClass string `json:"storageClass"`

	// Size specifies the size of the catalog PVC
	// +kubebuilder:validation:Required
	Size string `json:"size"`

	// ObjectStore defines the S3-compatible storage configuration
	// +kubebuilder:validation:Required
	ObjectStore ObjectStoreSpec `json:"objectStore"`

	// Encryption defines the encryption configuration
	// +optional
	Encryption *EncryptionSpec `json:"encryption,omitempty"`

	// BackupPolicy defines the backup configuration
	// +optional
	BackupPolicy *BackupPolicySpec `json:"backupPolicy,omitempty"`
}

// CatalogPhase represents the current phase of the catalog
// +kubebuilder:validation:Enum=Pending;Running;Failed;Succeeded
type CatalogPhase string

const (
	// CatalogPhasePending means the catalog is being created
	CatalogPhasePending CatalogPhase = "Pending"
	// CatalogPhaseRunning means the catalog is operational
	CatalogPhaseRunning CatalogPhase = "Running"
	// CatalogPhaseFailed means the catalog has encountered an error
	CatalogPhaseFailed CatalogPhase = "Failed"
	// CatalogPhaseSucceeded means the catalog is ready
	CatalogPhaseSucceeded CatalogPhase = "Succeeded"
)

// DuckLakeCatalogStatus defines the observed state of DuckLakeCatalog
type DuckLakeCatalogStatus struct {
	// Phase represents the current phase of the catalog
	// +optional
	Phase CatalogPhase `json:"phase,omitempty"`

	// LastBackup is the timestamp of the last successful backup
	// +optional
	LastBackup *metav1.Time `json:"lastBackup,omitempty"`

	// Conditions represent the latest available observations of the catalog's state
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the last generation that was acted on
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// DuckLakeCatalog is the Schema for the ducklakecatalogs API
type DuckLakeCatalog struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DuckLakeCatalogSpec   `json:"spec,omitempty"`
	Status DuckLakeCatalogStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DuckLakeCatalogList contains a list of DuckLakeCatalog
type DuckLakeCatalogList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DuckLakeCatalog `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DuckLakeCatalog{}, &DuckLakeCatalogList{})
}
