package controller

import (
	"context"

	"github.com/TFMV/featherman/operator/internal/storage"
)

// MockS3Client implements the storage.ObjectStore interface for testing
type MockS3Client struct {
	err error
}

// NewMockS3Client creates a new mock S3 client
func NewMockS3Client() *MockS3Client {
	return &MockS3Client{}
}

// SetError sets the error to be returned by the mock client
func (m *MockS3Client) SetError(err error) {
	m.err = err
}

// ListBuckets mocks listing S3 buckets
func (m *MockS3Client) ListBuckets(ctx context.Context) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	return []string{"test-bucket"}, nil
}

// CreateBucket mocks creating an S3 bucket
func (m *MockS3Client) CreateBucket(ctx context.Context, name string) error {
	if m.err != nil {
		return m.err
	}
	return nil
}

// DeleteBucket mocks deleting an S3 bucket
func (m *MockS3Client) DeleteBucket(ctx context.Context, name string) error {
	if m.err != nil {
		return m.err
	}
	return nil
}

// BucketExists mocks checking if an S3 bucket exists
func (m *MockS3Client) BucketExists(ctx context.Context, name string) (bool, error) {
	if m.err != nil {
		return false, m.err
	}
	return true, nil
}

// GetBucketRegion mocks getting the region of an S3 bucket
func (m *MockS3Client) GetBucketRegion(ctx context.Context, name string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return "us-west-2", nil
}

// ListObjects mocks listing objects in a bucket
func (m *MockS3Client) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	return []string{}, nil
}

// GetObject mocks getting an object from a bucket
func (m *MockS3Client) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	if m.err != nil {
		return nil, m.err
	}
	return []byte{}, nil
}

// PutObject mocks putting an object in a bucket
func (m *MockS3Client) PutObject(ctx context.Context, bucket, key string, data []byte) error {
	if m.err != nil {
		return m.err
	}
	return nil
}

// DeleteObject mocks deleting an object from a bucket
func (m *MockS3Client) DeleteObject(ctx context.Context, bucket, key string) error {
	if m.err != nil {
		return m.err
	}
	return nil
}

// Ensure MockS3Client implements storage.ObjectStore
var _ storage.ObjectStore = &MockS3Client{}
