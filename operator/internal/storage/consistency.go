package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
)

const (
	// DefaultTimeout is the default timeout for consistency checks
	DefaultTimeout = 5 * time.Minute

	// DefaultInterval is the default interval between consistency checks
	DefaultInterval = 5 * time.Second

	// DefaultRetries is the default number of retries for consistency checks
	DefaultRetries = 3
)

// ConsistencyChecker handles S3 consistency checks for Parquet files
type ConsistencyChecker struct {
	client   client.Client
	s3Client *s3.Client
	timeout  time.Duration
	interval time.Duration
	retries  int
}

// NewConsistencyChecker creates a new consistency checker
func NewConsistencyChecker(client client.Client, s3Client *s3.Client) *ConsistencyChecker {
	return &ConsistencyChecker{
		client:   client,
		s3Client: s3Client,
		timeout:  DefaultTimeout,
		interval: DefaultInterval,
		retries:  DefaultRetries,
	}
}

// WithTimeout sets the timeout for consistency checks
func (c *ConsistencyChecker) WithTimeout(timeout time.Duration) *ConsistencyChecker {
	c.timeout = timeout
	return c
}

// WithInterval sets the interval between consistency checks
func (c *ConsistencyChecker) WithInterval(interval time.Duration) *ConsistencyChecker {
	c.interval = interval
	return c
}

// WithRetries sets the number of retries for consistency checks
func (c *ConsistencyChecker) WithRetries(retries int) *ConsistencyChecker {
	c.retries = retries
	return c
}

// getObjectStore returns the object store configuration for a table
func (c *ConsistencyChecker) getObjectStore(ctx context.Context, table *ducklakev1alpha1.DuckLakeTable) (*ducklakev1alpha1.ObjectStoreSpec, error) {
	if table.Spec.ObjectStore != nil {
		return table.Spec.ObjectStore, nil
	}

	// Get the catalog
	catalog := &ducklakev1alpha1.DuckLakeCatalog{}
	if err := c.client.Get(ctx, client.ObjectKey{
		Namespace: table.Namespace,
		Name:      table.Spec.CatalogRef,
	}, catalog); err != nil {
		return nil, fmt.Errorf("failed to get catalog: %w", err)
	}

	return &catalog.Spec.ObjectStore, nil
}

// WaitForParquetFile waits for a Parquet file to be available in S3
func (c *ConsistencyChecker) WaitForParquetFile(ctx context.Context, table *ducklakev1alpha1.DuckLakeTable, filename string) error {
	objectStore, err := c.getObjectStore(ctx, table)
	if err != nil {
		return err
	}

	fullPath := path.Join(table.Spec.Location, filename)

	backoff := wait.Backoff{
		Duration: c.interval,
		Factor:   2.0,
		Steps:    c.retries,
	}

	return wait.ExponentialBackoff(backoff, func() (bool, error) {
		exists, err := c.checkFileExists(ctx, objectStore.Bucket, fullPath)
		if err != nil {
			return false, err
		}
		return exists, nil
	})
}

// checkFileExists checks if a file exists in S3
func (c *ConsistencyChecker) checkFileExists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := c.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})

	if err != nil {
		var nsk *types.NoSuchKey
		if ok := errors.As(err, &nsk); ok {
			return false, nil
		}
		return false, fmt.Errorf("failed to check file existence: %w", err)
	}

	return true, nil
}

// WaitForParquetFiles waits for multiple Parquet files to be available in S3
func (c *ConsistencyChecker) WaitForParquetFiles(ctx context.Context, table *ducklakev1alpha1.DuckLakeTable, filenames []string) error {
	for _, filename := range filenames {
		if err := c.WaitForParquetFile(ctx, table, filename); err != nil {
			return fmt.Errorf("failed to wait for file %s: %w", filename, err)
		}
	}
	return nil
}

// ListParquetFiles lists all Parquet files in a table's location
func (c *ConsistencyChecker) ListParquetFiles(ctx context.Context, table *ducklakev1alpha1.DuckLakeTable) ([]types.Object, error) {
	objectStore, err := c.getObjectStore(ctx, table)
	if err != nil {
		return nil, err
	}

	input := &s3.ListObjectsV2Input{
		Bucket: &objectStore.Bucket,
		Prefix: &table.Spec.Location,
	}

	var objects []types.Object
	paginator := s3.NewListObjectsV2Paginator(c.s3Client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		objects = append(objects, page.Contents...)
	}

	return objects, nil
}

// VerifyParquetConsistency verifies the consistency of Parquet files
func (c *ConsistencyChecker) VerifyParquetConsistency(ctx context.Context, table *ducklakev1alpha1.DuckLakeTable) error {
	objectStore, err := c.getObjectStore(ctx, table)
	if err != nil {
		return err
	}

	objects, err := c.ListParquetFiles(ctx, table)
	if err != nil {
		return err
	}

	for _, obj := range objects {
		// Check file size
		if *obj.Size == 0 {
			return fmt.Errorf("found zero-size Parquet file: %s", *obj.Key)
		}

		// Check file extension
		if !strings.HasSuffix(*obj.Key, ".parquet") {
			return fmt.Errorf("found non-Parquet file: %s", *obj.Key)
		}

		// Verify file integrity with HEAD request
		if _, err := c.checkFileExists(ctx, objectStore.Bucket, *obj.Key); err != nil {
			return fmt.Errorf("failed to verify file integrity for %s: %w", *obj.Key, err)
		}
	}

	return nil
}
