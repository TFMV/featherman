package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DuckLakePoolSpec defines the desired state of DuckLakePool
type DuckLakePoolSpec struct {
	// MinSize is the minimum number of warm pods to maintain
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	MinSize int32 `json:"minSize,omitempty"`

	// MaxSize is the maximum number of warm pods to maintain
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10
	MaxSize int32 `json:"maxSize,omitempty"`

	// TargetUtilization is the target utilization ratio (0.0 to 1.0)
	// +kubebuilder:validation:Pattern=^(0?\.[0-9]+|1\.0|1)$
	// +kubebuilder:default="0.8"
	TargetUtilization string `json:"targetUtilization,omitempty"`

	// Template defines the pod template for warm pods
	Template PodTemplate `json:"template"`

	// LifecyclePolicies defines when to recycle pods
	LifecyclePolicies LifecyclePolicies `json:"lifecyclePolicies,omitempty"`

	// ScalingBehavior configures how the pool scales
	ScalingBehavior ScalingBehavior `json:"scalingBehavior,omitempty"`

	// CatalogRef references the DuckLakeCatalog to mount
	CatalogRef CatalogReference `json:"catalogRef"`

	// Metrics configures metrics exposure
	Metrics MetricsConfig `json:"metrics,omitempty"`
}

// PodTemplate defines the template for creating warm pods
type PodTemplate struct {
	// Resources defines resource requirements
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Image is the DuckDB container image
	// +kubebuilder:default="datacatering/duckdb:v1.3.0"
	Image string `json:"image,omitempty"`

	// ImagePullPolicy defines when to pull the image
	// +kubebuilder:default="IfNotPresent"
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// SecurityContext defines security settings
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`

	// NodeSelector defines node selection constraints
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations defines pod tolerations
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Affinity defines pod affinity rules
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
}

// LifecyclePolicies defines when to recycle warm pods
type LifecyclePolicies struct {
	// MaxIdleTime is the maximum time a pod can be idle before termination
	// +kubebuilder:default="5m"
	MaxIdleTime metav1.Duration `json:"maxIdleTime,omitempty"`

	// MaxLifetime is the maximum lifetime of a pod before recycling
	// +kubebuilder:default="1h"
	MaxLifetime metav1.Duration `json:"maxLifetime,omitempty"`

	// MaxQueries is the maximum number of queries before recycling
	// +kubebuilder:default=100
	MaxQueries int32 `json:"maxQueries,omitempty"`
}

// ScalingBehavior configures pool scaling behavior
type ScalingBehavior struct {
	// ScaleUpRate is the maximum number of pods to add per interval
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	ScaleUpRate int32 `json:"scaleUpRate,omitempty"`

	// ScaleDownRate is the maximum number of pods to remove per interval
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	ScaleDownRate int32 `json:"scaleDownRate,omitempty"`

	// ScaleInterval is the interval between scaling evaluations
	// +kubebuilder:default="30s"
	ScaleInterval metav1.Duration `json:"scaleInterval,omitempty"`

	// StabilizationWindow is the time to wait before scaling down
	// +kubebuilder:default="2m"
	StabilizationWindow metav1.Duration `json:"stabilizationWindow,omitempty"`
}

// CatalogReference references a DuckLakeCatalog
type CatalogReference struct {
	// Name is the name of the DuckLakeCatalog
	Name string `json:"name"`

	// ReadOnly specifies if the catalog should be mounted read-only
	// +kubebuilder:default=true
	ReadOnly bool `json:"readOnly,omitempty"`
}

// MetricsConfig configures metrics exposure
type MetricsConfig struct {
	// Enabled specifies if metrics should be exposed
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// Port is the port to expose metrics on
	// +kubebuilder:default=9090
	Port int32 `json:"port,omitempty"`
}

// PodState represents the state of a warm pod
type PodState string

const (
	// PodStateIdle indicates the pod is ready to accept work
	PodStateIdle PodState = "Idle"
	// PodStateAcquiring indicates the pod is being assigned work
	PodStateAcquiring PodState = "Acquiring"
	// PodStateBusy indicates the pod is executing a query
	PodStateBusy PodState = "Busy"
	// PodStateDraining indicates the pod is finishing work before termination
	PodStateDraining PodState = "Draining"
	// PodStateTerminating indicates the pod is being removed
	PodStateTerminating PodState = "Terminating"
)

// PoolPhase represents the phase of the pool
type PoolPhase string

const (
	// PoolPhasePending indicates the pool is being created
	PoolPhasePending PoolPhase = "Pending"
	// PoolPhaseRunning indicates the pool is running normally
	PoolPhaseRunning PoolPhase = "Running"
	// PoolPhaseFailed indicates the pool has failed
	PoolPhaseFailed PoolPhase = "Failed"
	// PoolPhaseTerminating indicates the pool is being deleted
	PoolPhaseTerminating PoolPhase = "Terminating"
)

// PodInfo contains information about a warm pod
type PodInfo struct {
	// Name is the pod name
	Name string `json:"name"`
	// State is the current state
	State PodState `json:"state"`
	// LastUsed is when the pod was last used
	LastUsed metav1.Time `json:"lastUsed,omitempty"`
	// QueryCount is the number of queries executed
	QueryCount int32 `json:"queryCount"`
	// CreatedAt is when the pod was created
	CreatedAt metav1.Time `json:"createdAt"`
}

// DuckLakePoolStatus defines the observed state of DuckLakePool
type DuckLakePoolStatus struct {
	// Phase is the current phase of the pool
	Phase PoolPhase `json:"phase,omitempty"`

	// CurrentSize is the current number of pods
	CurrentSize int32 `json:"currentSize"`

	// DesiredSize is the desired number of pods
	DesiredSize int32 `json:"desiredSize"`

	// IdlePods is the number of idle pods
	IdlePods int32 `json:"idlePods"`

	// BusyPods is the number of busy pods
	BusyPods int32 `json:"busyPods"`

	// QueueLength is the number of pending requests
	QueueLength int32 `json:"queueLength"`

	// Pods contains information about each pod
	Pods []PodInfo `json:"pods,omitempty"`

	// Conditions represent the latest available observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastScaleTime is when the pool was last scaled
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`

	// ObservedGeneration is the last observed generation
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=dlp
//+kubebuilder:printcolumn:name="Min",type=integer,JSONPath=`.spec.minSize`
//+kubebuilder:printcolumn:name="Max",type=integer,JSONPath=`.spec.maxSize`
//+kubebuilder:printcolumn:name="Current",type=integer,JSONPath=`.status.currentSize`
//+kubebuilder:printcolumn:name="Idle",type=integer,JSONPath=`.status.idlePods`
//+kubebuilder:printcolumn:name="Busy",type=integer,JSONPath=`.status.busyPods`
//+kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DuckLakePool is the Schema for the ducklakepools API
type DuckLakePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DuckLakePoolSpec   `json:"spec,omitempty"`
	Status DuckLakePoolStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// DuckLakePoolList contains a list of DuckLakePool
type DuckLakePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DuckLakePool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DuckLakePool{}, &DuckLakePoolList{})
}
