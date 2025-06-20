package versioning

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
    "github.com/TFMV/featherman/operator/internal/storage"
)

// Writer persists and retrieves resource snapshots.
type Writer struct {
    Store storage.ObjectStore
}

// snapshotRecord is the payload written to object storage.
type snapshotRecord struct {
    Timestamp time.Time       `json:"timestamp"`
    Spec      json.RawMessage `json:"spec"`
    Annotations map[string]string `json:"annotations,omitempty"`
}

// SaveCatalog stores a snapshot of the catalog spec and returns the version id.
func (w *Writer) SaveCatalog(ctx context.Context, c *ducklakev1alpha1.DuckLakeCatalog, annotations map[string]string) (string, error) {
    if w == nil || w.Store == nil {
        return "", fmt.Errorf("nil writer or store")
    }
    version := fmt.Sprintf("v%s", time.Now().UTC().Format("20060102-150405"))
    payload, err := json.Marshal(snapshotRecord{
        Timestamp: time.Now().UTC(),
        Spec:      mustJSON(c.Spec),
        Annotations: annotations,
    })
    if err != nil {
        return "", err
    }
    key := fmt.Sprintf("meta/%s/%s.json", version, c.Name)
    return version, w.Store.PutObject(ctx, c.Spec.ObjectStore.Bucket, key, payload)
}

// LoadCatalog retrieves a catalog spec for the given version.
func (w *Writer) LoadCatalog(ctx context.Context, c *ducklakev1alpha1.DuckLakeCatalog, version string) (*ducklakev1alpha1.DuckLakeCatalogSpec, error) {
    if w == nil || w.Store == nil {
        return nil, fmt.Errorf("nil writer or store")
    }
    key := fmt.Sprintf("meta/%s/%s.json", version, c.Name)
    data, err := w.Store.GetObject(ctx, c.Spec.ObjectStore.Bucket, key)
    if err != nil {
        return nil, err
    }
    var rec snapshotRecord
    if err := json.Unmarshal(data, &rec); err != nil {
        return nil, err
    }
    var spec ducklakev1alpha1.DuckLakeCatalogSpec
    if err := json.Unmarshal(rec.Spec, &spec); err != nil {
        return nil, err
    }
    return &spec, nil
}

// SaveTable stores a snapshot of the table spec and returns the version id.
func (w *Writer) SaveTable(ctx context.Context, t *ducklakev1alpha1.DuckLakeTable, bucket string, annotations map[string]string) (string, error) {
    if w == nil || w.Store == nil {
        return "", fmt.Errorf("nil writer or store")
    }
    version := fmt.Sprintf("v%s", time.Now().UTC().Format("20060102-150405"))
    payload, err := json.Marshal(snapshotRecord{
        Timestamp: time.Now().UTC(),
        Spec:      mustJSON(t.Spec),
        Annotations: annotations,
    })
    if err != nil {
        return "", err
    }
    key := fmt.Sprintf("meta/%s/%s.json", version, t.Name)
    return version, w.Store.PutObject(ctx, bucket, key, payload)
}

// LoadTable retrieves a table spec for the given version.
func (w *Writer) LoadTable(ctx context.Context, t *ducklakev1alpha1.DuckLakeTable, bucket, version string) (*ducklakev1alpha1.DuckLakeTableSpec, error) {
    if w == nil || w.Store == nil {
        return nil, fmt.Errorf("nil writer or store")
    }
    key := fmt.Sprintf("meta/%s/%s.json", version, t.Name)
    data, err := w.Store.GetObject(ctx, bucket, key)
    if err != nil {
        return nil, err
    }
    var rec snapshotRecord
    if err := json.Unmarshal(data, &rec); err != nil {
        return nil, err
    }
    var spec ducklakev1alpha1.DuckLakeTableSpec
    if err := json.Unmarshal(rec.Spec, &spec); err != nil {
        return nil, err
    }
    return &spec, nil
}

func mustJSON(in interface{}) json.RawMessage {
    b, _ := json.Marshal(in)
    return json.RawMessage(b)
}

