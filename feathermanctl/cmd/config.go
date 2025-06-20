package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

// Config represents the CLI configuration
type Config struct {
	// Kubernetes configuration
	Namespace  string `yaml:"namespace" mapstructure:"namespace"`
	Kubeconfig string `yaml:"kubeconfig" mapstructure:"kubeconfig"`

	// Featherman configuration
	QueryEndpoint string `yaml:"query_endpoint" mapstructure:"query_endpoint"`
	APIVersion    string `yaml:"api_version" mapstructure:"api_version"`

	// Output configuration
	OutputFormat string `yaml:"output_format" mapstructure:"output_format"`
	ColorOutput  bool   `yaml:"color_output" mapstructure:"color_output"`

	// Logging configuration
	LogLevel  string `yaml:"log_level" mapstructure:"log_level"`
	LogFormat string `yaml:"log_format" mapstructure:"log_format"`
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		Namespace:     "default",
		Kubeconfig:    filepath.Join(os.Getenv("HOME"), ".kube", "config"),
		QueryEndpoint: "http://localhost:8080",
		APIVersion:    "v1alpha1",
		OutputFormat:  "table",
		ColorOutput:   true,
		LogLevel:      "info",
		LogFormat:     "console",
	}
}

// LoadConfig loads configuration from file and environment variables
func LoadConfig() (*Config, error) {
	config := DefaultConfig()

	// Unmarshal viper config into struct
	if err := viper.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return config, nil
}

// SaveConfig saves the configuration to file
func SaveConfig(config *Config, configPath string) error {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Marshal config to YAML
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Write to file
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// ValidateConfig validates the configuration
func ValidateConfig(config *Config) error {
	if config.Namespace == "" {
		return fmt.Errorf("namespace cannot be empty")
	}

	if config.QueryEndpoint == "" {
		return fmt.Errorf("query_endpoint cannot be empty")
	}

	if config.APIVersion == "" {
		return fmt.Errorf("api_version cannot be empty")
	}

	validOutputFormats := map[string]bool{
		"table": true,
		"json":  true,
		"yaml":  true,
		"csv":   true,
	}

	if !validOutputFormats[config.OutputFormat] {
		return fmt.Errorf("invalid output format: %s", config.OutputFormat)
	}

	validLogLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}

	if !validLogLevels[config.LogLevel] {
		return fmt.Errorf("invalid log level: %s", config.LogLevel)
	}

	return nil
}
