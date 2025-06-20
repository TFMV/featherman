package versioning

import (
    "context"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/TFMV/featherman/operator/internal/storage"

    ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
)

// memoryStore is a simple in-memory ObjectStore for tests
type memoryStore struct {
   objects map[string][]byte
}

 func newMemoryStore() *memoryStore { return &memoryStore{objects: map[string][]byte{}} }

 func (m *memoryStore) ListBuckets(ctx context.Context) ([]string, error) { return []string{"b"}, nil }
 func (m *memoryStore) CreateBucket(ctx context.Context, bucket string) error { return nil }
 func (m *memoryStore) DeleteBucket(ctx context.Context, bucket string) error { return nil }
 func (m *memoryStore) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) { return nil, nil }
 func (m *memoryStore) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
    return m.objects[bucket+"/"+key], nil
 }
 func (m *memoryStore) PutObject(ctx context.Context, bucket, key string, data []byte) error {
    m.objects[bucket+"/"+key] = data
    return nil
 }
func (m *memoryStore) DeleteObject(ctx context.Context, bucket, key string) error { delete(m.objects, bucket+"/"+key); return nil }

var _ storage.ObjectStore = &memoryStore{}

 func TestCatalogSnapshotRoundTrip(t *testing.T) {
    store := newMemoryStore()
    writer := &Writer{Store: store}

    c := &ducklakev1alpha1.DuckLakeCatalog{Spec: ducklakev1alpha1.DuckLakeCatalogSpec{ObjectStore: ducklakev1alpha1.ObjectStoreSpec{Bucket: "b"}}}

    ver, err := writer.SaveCatalog(context.Background(), c, nil)
    assert.NoError(t, err)
    assert.NotEmpty(t, ver)

    spec, err := writer.LoadCatalog(context.Background(), c, ver)
    assert.NoError(t, err)
    assert.Equal(t, c.Spec.ObjectStore.Bucket, spec.ObjectStore.Bucket)
 }
