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

// ObjectStoreSpec defines the configuration for S3-compatible object storage
type ObjectStoreSpec struct {
	// Endpoint is the S3-compatible endpoint URL
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^https?:\/\/.*`
	Endpoint string `json:"endpoint"`

	// Bucket is the name of the S3 bucket to use
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=63
	Bucket string `json:"bucket"`

	// Region is the S3 region (optional for some providers)
	// +optional
	Region string `json:"region,omitempty"`

	// CredentialsSecret references the secret containing AWS credentials
	// +kubebuilder:validation:Required
	CredentialsSecret SecretReference `json:"credentialsSecret"`
}

// SecretReference contains the reference to a secret
type SecretReference struct {
	// Name is the name of the secret
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// AccessKeyField is the field in the secret containing the access key
	// +kubebuilder:default=access-key
	AccessKeyField string `json:"accessKeyField,omitempty"`

	// SecretKeyField is the field in the secret containing the secret key
	// +kubebuilder:default=secret-key
	SecretKeyField string `json:"secretKeyField,omitempty"`
}

// EncryptionSpec defines the encryption configuration for the catalog
type EncryptionSpec struct {
	// KMSKeyID is the AWS KMS key ID for encryption
	// +kubebuilder:validation:Required
	KMSKeyID string `json:"kmsKeyId"`
}

// BackupPolicySpec defines the backup configuration
type BackupPolicySpec struct {
	// Schedule is the cron expression for backups
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^(@(annually|yearly|monthly|weekly|daily|hourly|reboot))|(@every (\d+(ns|us|µs|ms|s|m|h))+)|((((\d+,)+\d+|(\d+(\/|-)\d+)|\d+|\*) ?){5,7})$`
	Schedule string `json:"schedule"`

	// RetentionDays is the number of days to retain backups
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=7
	RetentionDays int `json:"retentionDays,omitempty"`
}

// DuckLakeCatalogSpec defines the desired state of DuckLakeCatalog
type DuckLakeCatalogSpec struct {
	// StorageClass is the storage class to use for the catalog PVC
	// +kubebuilder:validation:Required
	StorageClass string `json:"storageClass"`

	// Size is the size of the catalog PVC
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=^([0-9]+(\.[0-9]+)?)(E|P|T|G|M|K|Ei|Pi|Ti|Gi|Mi|Ki)$
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

	// CatalogPath is the path to the DuckDB catalog file within the PVC
	// +kubebuilder:default=/catalog/catalog.db
	CatalogPath string `json:"catalogPath,omitempty"`
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
