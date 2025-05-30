# Changelog

All notable changes to Featherman will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0-pre] - 2025-05-30

This is the first pre-release of Featherman, a Kubernetes operator for DuckDB-powered data lakes. This release includes the core functionality for managing DuckDB catalogs and tables in Kubernetes.

### Added

#### Core Features

- Initial implementation of the DuckLake operator
- Custom Resource Definitions (CRDs):
  - `DuckLakeCatalog`: Manages DuckDB catalog files and S3 storage configuration
  - `DuckLakeTable`: Defines table schemas and Parquet storage options

#### Backup Management

- Automated backup system for DuckDB catalogs
- Configurable backup scheduling with cron expressions
- S3-compatible storage support for backups
- Retention policy management for automated cleanup
- Backup status tracking in catalog status

#### Storage Integration

- S3-compatible object store integration
- Support for custom endpoints and regions
- Secure credential management through Kubernetes secrets
- PVC-based catalog storage with size validation

#### Security

- Validation webhooks for CRD configuration
- Non-root container execution
- Read-only root filesystem for jobs
- Proper capability management
- Secure AWS credential injection

#### Operational Features

- Leader election for high availability
- Comprehensive health checks:
  - S3 connectivity monitoring
  - Kubernetes API health checks
  - Catalog filesystem access verification
  - Resource pressure monitoring
  - Leader election status
  - Webhook certificate expiration checks
- Detailed metrics for monitoring:
  - Reconciliation success/failure rates
  - Job execution metrics and latencies
  - S3 operation performance
  - Catalog size tracking
  - Backup operation metrics
- Structured logging throughout the operator
- Job-based execution model for reliability
- Resource cleanup on deletion

### Notes

This is a pre-release version intended for early testing and feedback. While core functionality is implemented, the following limitations apply:

- Limited to S3-compatible storage backends
- Basic monitoring and metrics
- Documentation is work in progress
- No upgrade path from this version
- API may change in future releases

### Known Issues

- Backup retention cleanup may be delayed if the operator restarts during execution
- Large catalog files (>10GB) may experience slower backup times
- Table operations do not yet support concurrent modifications

### Installation

```bash
kubectl apply -f https://raw.githubusercontent.com/TFMV/featherman/v0.1.0-pre/deploy/manifests.yaml
```

For detailed installation and usage instructions, please refer to the [README.md](README.md).
