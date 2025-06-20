package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// VersioningSpec controls automatic snapshotting of metadata
// for catalogs and tables
// +kubebuilder:object:generate=true
// +kubebuilder:resource:scope=Namespaced
type VersioningSpec struct {
    // Enabled toggles versioning
    // +kubebuilder:default=false
    Enabled bool `json:"enabled"`

    // Retention defines how long to keep versions
    // +optional
    Retention metav1.Duration `json:"retention,omitempty"`

    // Strategy determines versioning backend implementation
    // +kubebuilder:default=snapshot
    // +optional
    Strategy string `json:"strategy,omitempty"`
}

// VersionEntry represents a stored resource version
type VersionEntry struct {
    // ID is the version identifier
    ID string `json:"id"`
    // Timestamp records when the snapshot was taken
    Timestamp metav1.Time `json:"timestamp"`
}
