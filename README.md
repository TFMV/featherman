# 🪶 Featherman

> ⚠️ **Early Development Notice**: Featherman is currently in early development. While the core design is established, the implementation is actively evolving. We welcome early feedback and contributions!

## What is Featherman?

Featherman is a Kubernetes operator that brings DuckDB's DuckLake functionality to Kubernetes, enabling declarative management of data lakes with the simplicity of DuckDB and the scalability of cloud object storage.

### Key Features

- **Declarative Data Lake Management**: Define your data lake structure using Kubernetes Custom Resources
- **Cloud-Native Storage**: Seamlessly integrate with S3-compatible object stores
- **Simple Yet Powerful**: Leverages DuckDB's simplicity while providing enterprise features
- **Kubernetes-Native**: Fully integrated with Kubernetes ecosystem and tooling

## Architecture

Featherman consists of two main components:

1. **Control Plane**:
   - Kubernetes operator managing the lifecycle of catalogs and tables
   - Handles metadata management and coordination
   - Manages backup and retention policies

2. **Data Plane**:
   - Ephemeral DuckDB jobs for data operations
   - Direct Parquet I/O with object storage
   - Catalog storage on persistent volumes

## Custom Resources

### DuckLakeCatalog

The `DuckLakeCatalog` resource defines a DuckDB catalog and its storage configuration:

```yaml
apiVersion: ducklake.featherman.dev/v1alpha1
kind: DuckLakeCatalog
metadata:
  name: example-catalog
spec:
  storageClass: standard
  size: 10Gi
  objectStore:
    endpoint: s3.amazonaws.com
    bucket: my-data-lake
    region: us-west-2
  encryption:
    provider: aws-kms
    keyId: my-key
  backupPolicy:
    schedule: "0 0 * * *"
    retentionDays: 30
```

### DuckLakeTable

The `DuckLakeTable` resource defines table structure and storage options:

```yaml
apiVersion: ducklake.featherman.dev/v1alpha1
kind: DuckLakeTable
metadata:
  name: example-table
spec:
  catalogRef: example-catalog
  name: users
  columns:
    - name: id
      type: BIGINT
    - name: name
      type: VARCHAR
      nullable: true
  partitioning:
    - created_at
  format:
    compression: ZSTD
  ttlDays: 90
  mode: append
```

## Features

- **Simplified Data Lake Management**: Create and manage data lakes using familiar Kubernetes tooling
- **Cloud Storage Integration**: Native support for S3-compatible object stores
- **Data Lifecycle Management**: Automatic data retention and backup policies
- **Security**:
  - Encryption at rest with KMS integration
  - Kubernetes RBAC integration
  - Secure credential management
- **Operational Excellence**:
  - Monitoring and metrics
  - Backup and restore capabilities
  - Detailed status reporting

## Getting Started

> Coming Soon: Installation and usage instructions will be added as the project matures.

## Development Status

Featherman is in active development. Current focus areas:

- [ ] Core operator implementation
- [ ] Basic catalog and table management
- [ ] S3 integration
- [ ] Initial documentation
- [ ] Testing infrastructure

## Contributing

While we're in early development, we welcome:

- Feature suggestions
- Design feedback
- Use case discussions
- Early testing and feedback

## License

[MIT](LICENSE)
