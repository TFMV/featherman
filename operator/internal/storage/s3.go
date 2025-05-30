package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// S3Config represents the configuration for S3 storage
type S3Config struct {
	// Endpoint is the S3-compatible endpoint URL
	Endpoint string
	// Region is the S3 region
	Region string
	// Bucket is the S3 bucket name
	Bucket string
	// AccessKeyID for authentication
	AccessKeyID string
	// SecretAccessKey for authentication
	SecretAccessKey string
}

// S3Client provides operations for S3-compatible storage
type S3Client interface {
	// UploadParquet uploads a Parquet file to S3
	UploadParquet(ctx context.Context, key string, reader io.Reader) error
	// DownloadParquet downloads a Parquet file from S3
	DownloadParquet(ctx context.Context, key string) (io.ReadCloser, error)
	// ListParquetFiles lists Parquet files under a prefix
	ListParquetFiles(ctx context.Context, prefix string) ([]string, error)
	// DeleteParquetFiles deletes Parquet files under a prefix
	DeleteParquetFiles(ctx context.Context, prefix string) error
	// HeadParquet checks if a Parquet file exists and gets its metadata
	HeadParquet(ctx context.Context, key string) (*s3.HeadObjectOutput, error)
}

type s3Client struct {
	client *s3.Client
	bucket string
}

// NewS3Client creates a new S3Client instance
func NewS3Client(ctx context.Context, cfg S3Config) (S3Client, error) {
	logger := log.FromContext(ctx)
	logger.Info("creating S3 client", "endpoint", cfg.Endpoint)
	// TODO: Update to use the new AWS SDK v2
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               cfg.Endpoint,
			SigningRegion:     cfg.Region,
			HostnameImmutable: true,
		}, nil
	})

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg)
	return &s3Client{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}

// UploadParquet implements S3Client
func (c *s3Client) UploadParquet(ctx context.Context, key string, reader io.Reader) error {
	logger := log.FromContext(ctx)
	logger.Info("uploading Parquet file", "key", key)

	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        reader,
		ContentType: aws.String("application/vnd.apache.parquet"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload Parquet file: %w", err)
	}

	return nil
}

// DownloadParquet implements S3Client
func (c *s3Client) DownloadParquet(ctx context.Context, key string) (io.ReadCloser, error) {
	logger := log.FromContext(ctx)
	logger.Info("downloading Parquet file", "key", key)

	output, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download Parquet file: %w", err)
	}

	return output.Body, nil
}

// ListParquetFiles implements S3Client
func (c *s3Client) ListParquetFiles(ctx context.Context, prefix string) ([]string, error) {
	logger := log.FromContext(ctx)
	logger.Info("listing Parquet files", "prefix", prefix)

	var files []string
	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list Parquet files: %w", err)
		}

		for _, obj := range page.Contents {
			files = append(files, *obj.Key)
		}
	}

	return files, nil
}

// DeleteParquetFiles implements S3Client
func (c *s3Client) DeleteParquetFiles(ctx context.Context, prefix string) error {
	logger := log.FromContext(ctx)
	logger.Info("deleting Parquet files", "prefix", prefix)

	files, err := c.ListParquetFiles(ctx, prefix)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil
	}

	var objects []types.ObjectIdentifier
	for _, file := range files {
		objects = append(objects, types.ObjectIdentifier{
			Key: aws.String(file),
		})
	}

	_, err = c.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(c.bucket),
		Delete: &types.Delete{
			Objects: objects,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to delete Parquet files: %w", err)
	}

	return nil
}

// HeadParquet implements S3Client
func (c *s3Client) HeadParquet(ctx context.Context, key string) (*s3.HeadObjectOutput, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info("checking Parquet file", "key", key)

	output, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to head Parquet file: %w", err)
	}

	return output, nil
}

// WaitForParquetConsistency waits for a Parquet file to be available
func (c *s3Client) WaitForParquetConsistency(ctx context.Context, key string, timeout time.Duration) error {
	logger := log.FromContext(ctx)
	logger.Info("waiting for Parquet file consistency", "key", key, "timeout", timeout)

	deadline := time.Now().Add(timeout)
	backoff := time.Second

	for time.Now().Before(deadline) {
		_, err := c.HeadParquet(ctx, key)
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff = time.Duration(float64(backoff) * 1.5)
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
		}
	}

	return fmt.Errorf("timeout waiting for Parquet file consistency")
}

// NewObjectStore creates a new ObjectStore implementation from an S3 client
func NewObjectStore(s3Client *s3.Client) ObjectStore {
	return &s3ObjectStore{
		client: s3Client,
	}
}

// s3ObjectStore implements the ObjectStore interface using AWS S3
type s3ObjectStore struct {
	client *s3.Client
}

func (s *s3ObjectStore) ListBuckets(ctx context.Context) ([]string, error) {
	result, err := s.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	var buckets []string
	for _, bucket := range result.Buckets {
		buckets = append(buckets, *bucket.Name)
	}
	return buckets, nil
}

func (s *s3ObjectStore) CreateBucket(ctx context.Context, bucket string) error {
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	return nil
}

func (s *s3ObjectStore) DeleteBucket(ctx context.Context, bucket string) error {
	_, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}
	return nil
}

func (s *s3ObjectStore) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	var objects []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			objects = append(objects, *obj.Key)
		}
	}

	return objects, nil
}

func (s *s3ObjectStore) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}

	return data, nil
}

func (s *s3ObjectStore) PutObject(ctx context.Context, bucket, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}
	return nil
}

func (s *s3ObjectStore) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}
	return nil
}
