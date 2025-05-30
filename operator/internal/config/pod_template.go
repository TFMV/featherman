package config

import (
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// PodTemplateConfig defines the configuration for DuckDB pods
type PodTemplateConfig struct {
	// Resources defines resource requirements for DuckDB container
	Resources corev1.ResourceRequirements `json:"resources"`

	// InitContainers defines additional init containers
	InitContainers []corev1.Container `json:"initContainers,omitempty"`

	// Env defines additional environment variables
	Env []corev1.EnvVar `json:"env,omitempty"`

	// SecurityContext defines pod security context
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`

	// NodeSelector defines node selection constraints
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations defines pod tolerations
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// ImagePullSecrets defines secrets for pulling container images
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
}

// DefaultPodTemplateConfig returns the default pod template configuration
func DefaultPodTemplateConfig() *PodTemplateConfig {
	return &PodTemplateConfig{
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		},
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot: &[]bool{true}[0],
			RunAsUser:    &[]int64{1000}[0],
			FSGroup:      &[]int64{1000}[0],
		},
	}
}

// LoadFromConfigMap loads pod template configuration from a ConfigMap
func LoadFromConfigMap(data string) (*PodTemplateConfig, error) {
	config := DefaultPodTemplateConfig()
	if data == "" {
		return config, nil
	}

	if err := json.Unmarshal([]byte(data), config); err != nil {
		return nil, err
	}

	return config, nil
}

// ApplyToPodSpec applies the configuration to a PodSpec
func (c *PodTemplateConfig) ApplyToPodSpec(spec *corev1.PodSpec) {
	// Apply resource requirements to DuckDB container
	for i := range spec.Containers {
		if spec.Containers[i].Name == "duckdb" {
			spec.Containers[i].Resources = c.Resources
			break
		}
	}

	// Add custom init containers
	if len(c.InitContainers) > 0 {
		spec.InitContainers = append(spec.InitContainers, c.InitContainers...)
	}

	// Add custom environment variables
	if len(c.Env) > 0 {
		for i := range spec.Containers {
			if spec.Containers[i].Name == "duckdb" {
				spec.Containers[i].Env = append(spec.Containers[i].Env, c.Env...)
				break
			}
		}
	}

	// Apply security context
	if c.SecurityContext != nil {
		spec.SecurityContext = c.SecurityContext
	}

	// Apply node selector
	if len(c.NodeSelector) > 0 {
		spec.NodeSelector = c.NodeSelector
	}

	// Apply tolerations
	if len(c.Tolerations) > 0 {
		spec.Tolerations = c.Tolerations
	}

	// Apply image pull secrets
	if len(c.ImagePullSecrets) > 0 {
		spec.ImagePullSecrets = c.ImagePullSecrets
	}
}
