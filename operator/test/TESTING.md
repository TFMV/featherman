# Testing Featherman

This document describes the testing strategy and setup for the Featherman operator.

## Test Structure

```
operator/
├── internal/          # Unit tests alongside code
│   ├── storage/
│   ├── duckdb/
│   └── sql/
├── test/
│   ├── integration/  # Integration tests
│   └── e2e/         # End-to-end tests
└── Makefile         # Test targets
```

## Running Tests

### Prerequisites

- Go 1.21+
- Docker
- KinD (Kubernetes in Docker)
- MinIO (for S3 testing)

### Test Commands

```bash
# Run all tests
make test

# Run specific test suites
make test-unit
make test-integration
make test-e2e

# Run with coverage
make test-coverage
```

## Test Categories

### Unit Tests

- Located alongside the code they test
- Fast and independent
- No external dependencies
- Run with `go test ./internal/...`

### Integration Tests

- Test component interactions
- Require some external setup (MinIO)
- Run with `go test ./test/integration/...`

### End-to-End Tests

- Full system tests
- Require KinD cluster
- Run with `go test ./test/e2e/...`

## Writing Tests

### Unit Tests

```go
func TestMyFunction(t *testing.T) {
    // Arrange
    // Act
    // Assert
}
```

### Integration Tests

```go
func TestComponentInteraction(t *testing.T) {
    // Setup test environment
    // Run test
    // Verify results
    // Cleanup
}
```

### E2E Tests

```go
var _ = Describe("Feature", func() {
    Context("When something happens", func() {
        It("Should do something", func() {
            // Test implementation
        })
    })
})
```

## Test Coverage

We aim for:

- Unit tests: >80% coverage
- Integration tests: All key workflows
- E2E tests: All user-facing features

## CI/CD Integration

Tests are run in GitHub Actions:

- On pull requests
- On main branch commits
- Nightly for longer tests

## Debugging Tests

### Common Issues

1. MinIO connection failures
2. KinD cluster setup issues
3. Resource cleanup problems

### Solutions

1. Check MinIO logs
2. Verify KinD configuration
3. Use test cleanup handlers

## Contributing

When adding new features:

1. Add unit tests first
2. Add integration tests for components
3. Update E2E tests for user workflows
4. Update test documentation

## Best Practices

1. Use table-driven tests
2. Mock external dependencies
3. Clean up test resources
4. Use meaningful test names
5. Add test documentation
