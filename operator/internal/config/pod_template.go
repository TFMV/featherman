package config

import (
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// PodTemplateConfig defines the configuration for pod templates
type PodTemplateConfig struct {
	// Resources defines resource requirements
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// SecurityContext defines the security context for containers
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`

	// NodeSelector defines node selection constraints
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations defines pod tolerations
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// ImagePullSecrets defines secrets for pulling container images
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Volumes defines additional volumes
	Volumes []corev1.Volume `json:"volumes,omitempty"`

	// VolumeMounts defines additional volume mounts
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`

	// EnvFrom defines environment variables from sources
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`

	// Env defines environment variables
	Env []corev1.EnvVar `json:"env,omitempty"`
}

// DefaultPodTemplateConfig returns a default pod template configuration
func DefaultPodTemplateConfig() *PodTemplateConfig {
	return &PodTemplateConfig{
		Resources: &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsNonRoot: &[]bool{true}[0],
			RunAsUser:    &[]int64{1000}[0],
		},
	}
}

// LoadFromConfigMap loads the configuration from a ConfigMap data string
func LoadFromConfigMap(data string) (*PodTemplateConfig, error) {
	config := &PodTemplateConfig{}
	if err := json.Unmarshal([]byte(data), config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pod template config: %w", err)
	}
	return config, nil
}

// ApplyToPodSpec applies the configuration to a pod spec
func (c *PodTemplateConfig) ApplyToPodSpec(podSpec *corev1.PodSpec) {
	if c.NodeSelector != nil {
		podSpec.NodeSelector = c.NodeSelector
	}

	if len(c.Tolerations) > 0 {
		podSpec.Tolerations = c.Tolerations
	}

	if len(c.ImagePullSecrets) > 0 {
		podSpec.ImagePullSecrets = c.ImagePullSecrets
	}

	if len(c.Volumes) > 0 {
		podSpec.Volumes = append(podSpec.Volumes, c.Volumes...)
	}

	if len(podSpec.Containers) > 0 {
		container := &podSpec.Containers[0]

		if c.Resources != nil {
			container.Resources = *c.Resources
		}

		if c.SecurityContext != nil {
			container.SecurityContext = c.SecurityContext
		}

		if len(c.VolumeMounts) > 0 {
			container.VolumeMounts = append(container.VolumeMounts, c.VolumeMounts...)
		}

		if len(c.EnvFrom) > 0 {
			container.EnvFrom = append(container.EnvFrom, c.EnvFrom...)
		}

		if len(c.Env) > 0 {
			container.Env = append(container.Env, c.Env...)
		}
	}
}

// JobConfig represents a job configuration that can be modified by a pod template
type JobConfig interface {
	SetResources(corev1.ResourceRequirements)
	SetSecurityContext(*corev1.SecurityContext)
	AddVolumes([]corev1.Volume)
	AddVolumeMounts([]corev1.VolumeMount)
	AddEnv([]corev1.EnvVar)
	AddEnvFrom([]corev1.EnvFromSource)
}

// ApplyToJobConfig applies the configuration to a job config
func (c *PodTemplateConfig) ApplyToJobConfig(config JobConfig) error {
	if config == nil {
		return fmt.Errorf("job config cannot be nil")
	}

	if c.Resources != nil {
		config.SetResources(*c.Resources)
	}

	if c.SecurityContext != nil {
		config.SetSecurityContext(c.SecurityContext)
	}

	if len(c.Volumes) > 0 {
		config.AddVolumes(c.Volumes)
	}

	if len(c.VolumeMounts) > 0 {
		config.AddVolumeMounts(c.VolumeMounts)
	}

	if len(c.Env) > 0 {
		config.AddEnv(c.Env)
	}

	if len(c.EnvFrom) > 0 {
		config.AddEnvFrom(c.EnvFrom)
	}

	return nil
}

// DeepCopy creates a deep copy of the PodTemplateConfig
func (c *PodTemplateConfig) DeepCopy() *PodTemplateConfig {
	if c == nil {
		return nil
	}
	out := new(PodTemplateConfig)
	if c.Resources != nil {
		out.Resources = c.Resources.DeepCopy()
	}
	if c.SecurityContext != nil {
		out.SecurityContext = c.SecurityContext.DeepCopy()
	}
	if c.NodeSelector != nil {
		out.NodeSelector = make(map[string]string)
		for k, v := range c.NodeSelector {
			out.NodeSelector[k] = v
		}
	}
	if len(c.Tolerations) > 0 {
		out.Tolerations = make([]corev1.Toleration, len(c.Tolerations))
		for i := range c.Tolerations {
			c.Tolerations[i].DeepCopyInto(&out.Tolerations[i])
		}
	}
	if len(c.ImagePullSecrets) > 0 {
		out.ImagePullSecrets = make([]corev1.LocalObjectReference, len(c.ImagePullSecrets))
		copy(out.ImagePullSecrets, c.ImagePullSecrets)
	}
	if len(c.Volumes) > 0 {
		out.Volumes = make([]corev1.Volume, len(c.Volumes))
		for i := range c.Volumes {
			c.Volumes[i].DeepCopyInto(&out.Volumes[i])
		}
	}
	if len(c.VolumeMounts) > 0 {
		out.VolumeMounts = make([]corev1.VolumeMount, len(c.VolumeMounts))
		for i := range c.VolumeMounts {
			c.VolumeMounts[i].DeepCopyInto(&out.VolumeMounts[i])
		}
	}
	if len(c.EnvFrom) > 0 {
		out.EnvFrom = make([]corev1.EnvFromSource, len(c.EnvFrom))
		for i := range c.EnvFrom {
			c.EnvFrom[i].DeepCopyInto(&out.EnvFrom[i])
		}
	}
	if len(c.Env) > 0 {
		out.Env = make([]corev1.EnvVar, len(c.Env))
		for i := range c.Env {
			c.Env[i].DeepCopyInto(&out.Env[i])
		}
	}
	return out
}
