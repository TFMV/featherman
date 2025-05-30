package storage

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetS3Config(t *testing.T) {
	tests := []struct {
		name           string
		envVars        map[string]string
		expectError    bool
		validateConfig func(t *testing.T, cfg interface{})
	}{
		{
			name: "default configuration",
			envVars: map[string]string{
				"AWS_REGION": "us-west-2",
			},
			expectError: false,
		},
		{
			name: "custom endpoint configuration",
			envVars: map[string]string{
				"AWS_REGION":  "us-west-2",
				"S3_ENDPOINT": "http://minio:9000",
			},
			expectError: false,
		},
		{
			name: "static credentials configuration",
			envVars: map[string]string{
				"AWS_REGION":            "us-west-2",
				"AWS_ACCESS_KEY_ID":     "test-key",
				"AWS_SECRET_ACCESS_KEY": "test-secret",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Backup current env
			envBackup := make(map[string]string)
			for k := range tt.envVars {
				envBackup[k] = os.Getenv(k)
			}

			// Set test env vars
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}

			// Cleanup
			defer func() {
				for k, v := range envBackup {
					if v == "" {
						os.Unsetenv(k)
					} else {
						os.Setenv(k, v)
					}
				}
			}()

			// Test
			cfg, err := GetS3Config(context.Background())
			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, cfg)

			if tt.validateConfig != nil {
				tt.validateConfig(t, cfg)
			}
		})
	}
}
