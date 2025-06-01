package storage

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// GetS3Config returns the AWS configuration for S3 operations
func GetS3Config(ctx context.Context) (aws.Config, error) {
	// Try to get configuration from environment variables
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1" // Default region
	}

	// Load configuration options
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}

	// Add static credentials if provided via environment variables
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessKey != "" && secretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKey,
			secretKey,
			"",
		)))
	}

	// Load the configuration
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return cfg, nil
}
