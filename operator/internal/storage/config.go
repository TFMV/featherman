package storage

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// GetS3Config returns the AWS configuration for S3 operations
func GetS3Config(ctx context.Context) (aws.Config, error) {
	// Try to get configuration from environment variables
	endpoint := os.Getenv("S3_ENDPOINT")
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

	// If custom endpoint is set, configure S3 client options
	if endpoint != "" {
		opts = append(opts, config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				if service == s3.ServiceID {
					return aws.Endpoint{
						URL:               endpoint,
						SigningRegion:     region,
						HostnameImmutable: true,
						Source:            aws.EndpointSourceCustom,
					}, nil
				}
				// Return EndpointNotFoundError for non-S3 services
				return aws.Endpoint{}, &aws.EndpointNotFoundError{}
			}),
		))
	}

	// Load the configuration
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return cfg, nil
}
