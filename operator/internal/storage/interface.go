package storage

import (
	"context"
)

// ObjectStore defines the interface for object storage operations
type ObjectStore interface {
	// ListBuckets lists all buckets
	ListBuckets(ctx context.Context) ([]string, error)
	// CreateBucket creates a new bucket
	CreateBucket(ctx context.Context, bucket string) error
	// DeleteBucket deletes a bucket
	DeleteBucket(ctx context.Context, bucket string) error
	// ListObjects lists objects in a bucket with optional prefix
	ListObjects(ctx context.Context, bucket, prefix string) ([]string, error)
	// GetObject gets an object from a bucket
	GetObject(ctx context.Context, bucket, key string) ([]byte, error)
	// PutObject puts an object in a bucket
	PutObject(ctx context.Context, bucket, key string, data []byte) error
	// DeleteObject deletes an object from a bucket
	DeleteObject(ctx context.Context, bucket, key string) error
}
