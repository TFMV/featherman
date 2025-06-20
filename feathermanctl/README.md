# feathermanctl

The official command-line interface for [Featherman](https://github.com/TFMV/featherman), a Kubernetes-native data lake platform built on DuckDB.

## Overview

`feathermanctl` provides comprehensive management capabilities for Featherman data lake resources, enabling you to:

- 🚀 **Deploy and manage** data catalogs, tables, and pools
- 🔍 **Execute SQL queries** against your data lake
- 📊 **Monitor status** of lake resources
- ⚙️ **Initialize environments** with proper configuration
- 📋 **Materialize views** to various storage formats

## Installation

### Build from Source

```bash
git clone https://github.com/TFMV/featherman.git
cd featherman/feathermanctl
go build -o feathermanctl .
```

### Homebrew (macOS)

```bash
brew install tfmv/tap/feathermanctl
```

## Quick Start

### 1. Initialize Environment

Set up Featherman in your Kubernetes cluster:

```bash
# Basic initialization
feathermanctl init -n featherman-system

# With MinIO for development
feathermanctl init -n featherman-system --with-minio

# Only install operator
feathermanctl init --operator-only
```

### 2. Deploy Resources

Deploy data lake resources from YAML manifests:

```bash
# Deploy a catalog
feathermanctl deploy catalog.yaml

# Deploy all manifests in a directory
feathermanctl deploy ./manifests/

# Dry run to preview changes
feathermanctl deploy catalog.yaml --dry-run
```

### 3. Query Data

Execute SQL queries against your data lake:

```bash
# Simple query
feathermanctl query --sql "SELECT * FROM my_table LIMIT 10" --catalog my-catalog

# Query from file
feathermanctl query --file query.sql --catalog my-catalog

# Interactive mode
feathermanctl query --interactive --catalog my-catalog
```

### 4. Check Status

Monitor the health of your resources:

```bash
# Show all resources
feathermanctl status --all

# Specific catalog
feathermanctl status --catalog my-catalog

# Watch for changes
feathermanctl status --all --watch
```

## Commands

### Global Flags

```
--config string           config file (default: $HOME/.featherman/config.yaml)
--kubeconfig string       path to kubeconfig file (default: $HOME/.kube/config)
-n, --namespace string    Kubernetes namespace (default: "default")
-o, --output string       output format: table|json|yaml|csv (default: "table")
--log-level string        log level: debug|info|warn|error (default: "info")
--log-format string       log format: console|json (default: "console")
--no-color               disable colored output
--query-endpoint string   Featherman query service endpoint
```

### init

Initialize a Featherman environment in Kubernetes.

```bash
feathermanctl init [flags]
```

**Flags:**
- `--with-minio`: Install MinIO for development environments
- `--operator-only`: Only install the Featherman operator
- `--timeout duration`: Timeout for initialization operations (default: 10m)

**Examples:**
```bash
# Basic initialization
feathermanctl init

# Development setup with MinIO
feathermanctl init --with-minio

# Production setup in custom namespace
feathermanctl init -n production --operator-only
```

### deploy

Deploy Featherman resources from YAML manifest files.

```bash
feathermanctl deploy [manifest-file] [flags]
```

**Flags:**
- `--dry-run`: Preview deployment without applying changes
- `--validate`: Validate manifests before deployment (default: true)
- `--wait`: Wait for resources to be ready

**Examples:**
```bash
# Deploy single manifest
feathermanctl deploy catalog.yaml

# Deploy directory of manifests
feathermanctl deploy ./manifests/

# Dry run deployment
feathermanctl deploy catalog.yaml --dry-run --validate
```

### query

Execute SQL queries against Featherman data lake.

```bash
feathermanctl query [flags]
```

**Flags:**
- `--sql string`: SQL statement to execute
- `--catalog string`: Target catalog name (required)
- `--table string`: Target table name (optional)
- `--file string`: Read SQL query from file
- `--interactive`: Start interactive query session
- `--timeout duration`: Query execution timeout (default: 5m)

**Examples:**
```bash
# Simple query
feathermanctl query --sql "SELECT count(*) FROM users" --catalog analytics

# Query from file
feathermanctl query --file complex_query.sql --catalog analytics

# Interactive session
feathermanctl query --interactive --catalog analytics

# JSON output
feathermanctl query --sql "SELECT * FROM users LIMIT 5" --catalog analytics -o json
```

### status

Show status of Featherman lake resources.

```bash
feathermanctl status [flags]
```

**Flags:**
- `--all`: Show status for all resources
- `--catalog string`: Show status for specific catalog
- `--table string`: Show status for specific table (requires --catalog)
- `--pool string`: Show status for specific pool
- `--watch`: Watch for status changes
- `--watch-interval duration`: Interval for watch updates (default: 5s)

**Examples:**
```bash
# Show all resources
feathermanctl status --all

# Specific catalog
feathermanctl status --catalog my-catalog

# Specific table
feathermanctl status --catalog my-catalog --table my-table

# Watch mode
feathermanctl status --all --watch
```

### materialize

Materialize views and tables to storage.

```bash
feathermanctl materialize [manifest-file] [flags]
```

**Flags:**
- `--source string`: Source table, view, or SQL query
- `--destination string`: Destination path or URI
- `--format string`: Output format: parquet|csv|json|orc (default: "parquet")
- `--compression string`: Compression algorithm: snappy|gzip|lz4|zstd (default: "snappy")
- `--overwrite`: Overwrite existing data
- `--partitions strings`: Partition columns
- `--batch-size int`: Batch size for processing (default: 10000)
- `--timeout duration`: Materialization timeout (default: 30m)

**Examples:**
```bash
# Using manifest file
feathermanctl materialize materialization.yaml

# Command line options
feathermanctl materialize \
  --source "SELECT * FROM analytics.users WHERE active = true" \
  --destination "s3://my-bucket/users/" \
  --format parquet \
  --compression snappy

# With partitioning
feathermanctl materialize \
  --source "analytics.sales" \
  --destination "./output/" \
  --partitions "year,month"
```

### version

Print version information.

```bash
feathermanctl version
```

## Configuration

### Configuration File

feathermanctl uses a YAML configuration file located at `$HOME/.featherman/config.yaml`:

```yaml
# Kubernetes configuration
namespace: default
kubeconfig: ~/.kube/config

# Featherman configuration
query_endpoint: http://localhost:8080
api_version: v1alpha1

# Output configuration
output_format: table
color_output: true

# Logging configuration
log_level: info
log_format: console
```

### Environment Variables

Configuration can be overridden using environment variables with the `FEATHERMAN_` prefix:

```bash
export FEATHERMAN_NAMESPACE=production
export FEATHERMAN_LOG_LEVEL=debug
export FEATHERMAN_QUERY_ENDPOINT=https://featherman-query.example.com
```

## Manifest Examples

### DuckLakeCatalog

```yaml
apiVersion: ducklake.featherman.dev/v1alpha1
kind: DuckLakeCatalog
metadata:
  name: analytics
  namespace: default
spec:
  storage:
    type: s3
    endpoint: http://minio:9000
    bucket: analytics
    credentials:
      secretRef:
        name: s3-credentials
  metastore:
    type: postgres
    connectionString: "postgresql://user:pass@localhost/metastore"
```

### DuckLakeTable

```yaml
apiVersion: ducklake.featherman.dev/v1alpha1
kind: DuckLakeTable
metadata:
  name: users
  namespace: default
spec:
  catalog: analytics
  schema:
    columns:
      - name: id
        type: INTEGER
        nullable: false
      - name: email
        type: VARCHAR
        nullable: false
      - name: created_at
        type: TIMESTAMP
        nullable: false
  storage:
    path: users/
    format: parquet
    partitions:
      - created_at
```

### Materialization

```yaml
apiVersion: ducklake.featherman.dev/v1alpha1
kind: Materialization
metadata:
  name: active-users-export
spec:
  source: "SELECT * FROM analytics.users WHERE active = true"
  destination: "s3://exports/active-users/"
  format: parquet
  compression: snappy
  overwrite: true
  partitions:
    - created_at
  options:
    row_group_size: "100000"
```

## Output Formats

feathermanctl supports multiple output formats:

### Table (default)
```bash
feathermanctl status --all
```

### JSON
```bash
feathermanctl status --all -o json
```

### YAML
```bash
feathermanctl status --all -o yaml
```

### CSV
```bash
feathermanctl status --all -o csv
```

## Authentication

### Kubernetes

feathermanctl uses your kubeconfig for Kubernetes authentication:

```bash
# Use specific kubeconfig
feathermanctl status --all --kubeconfig /path/to/config

# Use specific context
kubectl config use-context my-context
feathermanctl status --all
```

### Service Accounts

For CI/CD environments, use service accounts:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: feathermanctl
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: feathermanctl
rules:
  - apiGroups: ["ducklake.featherman.dev"]
    resources: ["*"]
    verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: feathermanctl
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: feathermanctl
subjects:
  - kind: ServiceAccount
    name: feathermanctl
    namespace: default
```

## Troubleshooting

### Common Issues

**Cannot connect to Kubernetes cluster**
```bash
# Check kubeconfig
kubectl config current-context

# Test connectivity
kubectl get nodes

# Use specific kubeconfig
feathermanctl --kubeconfig /path/to/config status --all
```

**Query service unreachable**
```bash
# Check query endpoint configuration
feathermanctl --query-endpoint http://your-endpoint:8080 query --help

# Port forward for local development
kubectl port-forward svc/featherman-query 8080:8080
```

**Permission denied**
```bash
# Check RBAC permissions
kubectl auth can-i create ducklakecatalogs

# Use different namespace
feathermanctl -n your-namespace status --all
```

### Debug Mode

Enable debug logging for troubleshooting:

```bash
feathermanctl --log-level debug status --all
```

### Verbose Output

Use JSON logging for structured output:

```bash
feathermanctl --log-format json --log-level debug status --all
```

## Integration

### CI/CD

Example GitHub Actions workflow:

```yaml
name: Deploy Data Lake
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup feathermanctl
        run: |
          curl -L https://github.com/TFMV/featherman/releases/latest/download/feathermanctl-linux-amd64 -o feathermanctl
          chmod +x feathermanctl
          sudo mv feathermanctl /usr/local/bin/
      
      - name: Deploy manifests
        run: |
          feathermanctl deploy ./manifests/ --validate
        env:
          KUBECONFIG: ${{ secrets.KUBECONFIG }}
```

### Monitoring

Integrate with monitoring systems:

```bash
# Export metrics in JSON
feathermanctl status --all -o json | jq '.[] | select(.Status != "Ready")'

# Watch for failed resources
feathermanctl status --all --watch --watch-interval 30s
```

## Contributing

See the main [Featherman repository](https://github.com/TFMV/featherman) for contribution guidelines.

## License

Licensed under the MIT License. See [LICENSE](../LICENSE) for details.
