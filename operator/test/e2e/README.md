# E2E Testing Guide

This guide explains how to run End-to-End (E2E) tests for the Featherman Kubernetes operator.

## Prerequisites

The following tools are required:

- Go 1.21 or later
- Docker
- kubectl
- KinD (Kubernetes in Docker)

The setup script will automatically install KinD and kubectl if they're not present.

## Test Environment

The E2E tests run in a KinD cluster with the following configuration:

- Single control-plane node
- Local path provisioner for storage
- Port mappings:
  - 9000: MinIO S3 API
  - 9001: MinIO Console

## Running Tests

### Quick Start

To run the E2E tests:

```bash
make test-e2e
```

This command will:

1. Set up a KinD cluster
2. Create necessary storage classes
3. Run the E2E test suite

### Manual Steps

1. Setup test environment:

```bash
make test-e2e-setup
```

2. Run tests:

```bash
go test ./test/e2e/... -v
```

3. Cleanup:

```bash
make test-e2e-cleanup
```

## Test Structure

The E2E tests are organized as follows:

- `test/e2e/suite_test.go`: Test suite setup and teardown
- `test/e2e/catalog_e2e_test.go`: DuckLakeCatalog tests
- `test/e2e/setup/`: Test environment setup utilities

### Test Cases

1. DuckLakeCatalog Controller Tests:
   - Basic catalog creation and verification
   - PVC creation and specification checks
   - Backup policy updates
   - Status condition monitoring
   - Error handling for invalid configurations
   - Resource cleanup verification

2. MinIO Integration Tests:
   - S3-compatible storage testing
   - Bucket operations
   - Credential management

## Debugging

If tests fail, you can:

1. Check KinD cluster status:

```bash
kind get clusters
kubectl cluster-info --context kind-featherman-e2e
```

2. View pod logs:

```bash
kubectl logs -n <namespace> <pod-name>
```

3. Access MinIO console:
   - URL: <http://localhost:9001>
   - Default credentials:
     - Access Key: minioadmin
     - Secret Key: minioadmin

## Adding New Tests

When adding new tests:

1. Use the Ginkgo BDD framework
2. Follow the existing test patterns
3. Ensure proper cleanup in AfterEach blocks
4. Add appropriate assertions using Gomega
5. Document new test cases in this README
