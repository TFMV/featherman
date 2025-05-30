# Featherman

Featherman brings DuckDB's powerful DuckLake functionality to Kubernetes, enabling declarative management of data lakes with the simplicity of DuckDB and the scalability of cloud object storage.

## Features

- 🦆 **DuckDB-Native**: Leverages DuckDB's simplicity and performance
- 🎯 **Declarative**: Define your data lake structure using Kubernetes CRDs
- ☁️ **Cloud Storage**: Seamless integration with S3-compatible object stores
- 🔒 **Enterprise Ready**: Built-in backup, encryption, and monitoring
- 🚀 **Kubernetes-Native**: Fully integrated with K8s ecosystem

## Quick Start

1. Install Featherman:

```bash
kubectl apply -f https://raw.githubusercontent.com/TFMV/featherman/main/deploy/manifests.yaml
```

2. Create a catalog:

```yaml
apiVersion: ducklake.featherman.dev/v1alpha1
kind: DuckLakeCatalog
metadata:
  name: example
spec:
  storageClass: standard
  size: 10Gi
  objectStore:
    endpoint: s3.amazonaws.com
    bucket: my-data-lake
    region: us-west-2
  backupPolicy:
    schedule: "0 2 * * *"    # Daily at 2 AM
    retentionDays: 7
```

3. Create a table:

```yaml
apiVersion: ducklake.featherman.dev/v1alpha1
kind: DuckLakeTable
metadata:
  name: users
spec:
  catalogRef: example
  name: users
  columns:
    - name: id
      type: INTEGER
    - name: name
      type: VARCHAR
  format:
    compression: ZSTD
    partitioning: ["created_at"]
```

## Architecture

Featherman consists of two main components:

- **Control Plane**: Kubernetes operator managing catalogs and tables
- **Data Plane**: Ephemeral DuckDB jobs for data operations

## License

MIT
