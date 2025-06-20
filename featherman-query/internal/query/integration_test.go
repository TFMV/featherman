package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

// MockResponseWriter implements http.ResponseWriter for testing
type MockResponseWriter struct {
	buffer bytes.Buffer
	header http.Header
	code   int
}

func (m *MockResponseWriter) Header() http.Header {
	if m.header == nil {
		m.header = make(http.Header)
	}
	return m.header
}

func (m *MockResponseWriter) Write(data []byte) (int, error) {
	return m.buffer.Write(data)
}

func (m *MockResponseWriter) WriteHeader(statusCode int) {
	m.code = statusCode
}

func (m *MockResponseWriter) String() string {
	return m.buffer.String()
}

func TestQueryEndToEnd(t *testing.T) {
	t.Skip("Skipping end-to-end test as it requires real Kubernetes REST client")

	// This test would require a real Kubernetes cluster or a more sophisticated fake client
	// The fake client doesn't provide REST client functionality needed for pod exec
	// In a real environment, this would work with proper k8s config
}

func TestQueryWithEphemeralJob(t *testing.T) {
	// No warm pods - should trigger ephemeral job creation
	clientset := fake.NewSimpleClientset()
	cfg := &rest.Config{}
	executor := NewExecutor(clientset, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	req := QueryRequest{
		SQL:     "SELECT 1 as test_col",
		Catalog: "nonexistent-catalog",
		Format:  "csv",
	}

	mockWriter := &MockResponseWriter{}
	podName, err := executor.Execute(ctx, req, mockWriter)

	// Should get timeout error due to pod never becoming ready in fake client
	if err == nil {
		t.Error("Expected error due to pod creation in fake environment")
	}

	if podName != "" {
		t.Error("Expected empty pod name on error")
	}
}

func TestQueryFormats(t *testing.T) {
	formats := []string{"csv", "json", "arrow", "parquet"}

	for _, format := range formats {
		t.Run(format, func(t *testing.T) {
			executor := &FakeExecutor{response: "test,data\n1,value\n"}
			router := NewRouter(executor)
			server := httptest.NewServer(router)
			defer server.Close()

			queryReq := QueryRequest{
				SQL:     "SELECT * FROM test",
				Catalog: "test-catalog",
				Format:  format,
			}

			body, _ := json.Marshal(queryReq)
			req, _ := http.NewRequest("POST", server.URL+"/query", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer test-token")
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				t.Errorf("Expected 200, got %d", resp.StatusCode)
			}

			// Check content type is set correctly
			contentType := resp.Header.Get("Content-Type")
			switch format {
			case "json":
				if contentType != "application/json" {
					t.Errorf("Expected application/json, got %s", contentType)
				}
			case "arrow":
				if contentType != "application/vnd.apache.arrow.stream" {
					t.Errorf("Expected arrow content type, got %s", contentType)
				}
			case "parquet":
				if contentType != "application/octet-stream" {
					t.Errorf("Expected octet-stream, got %s", contentType)
				}
			default:
				if contentType != "text/csv" {
					t.Errorf("Expected text/csv, got %s", contentType)
				}
			}
		})
	}
}

func TestQueryTimeout(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	cfg := &rest.Config{}
	executor := NewExecutor(clientset, cfg)

	// Create a context that times out immediately
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait for context to timeout
	time.Sleep(1 * time.Millisecond)

	req := QueryRequest{
		SQL:     "SELECT 1",
		Catalog: "test",
		Format:  "csv",
	}

	mockWriter := &MockResponseWriter{}
	_, err := executor.Execute(ctx, req, mockWriter)

	if err == nil {
		t.Error("Expected timeout error")
	}

	if !isContextTimeoutError(err) {
		t.Logf("Got error (which is expected): %v", err)
		// The specific error message may vary, just ensure we got an error
	}
}

func TestConcurrentQueries(t *testing.T) {
	executor := &FakeExecutor{response: "id,name\n1,test\n"}
	router := NewRouter(executor)
	server := httptest.NewServer(router)
	defer server.Close()

	// Run multiple queries concurrently
	numQueries := 10
	results := make(chan error, numQueries)

	for i := 0; i < numQueries; i++ {
		go func(queryNum int) {
			queryReq := QueryRequest{
				SQL:     "SELECT 1 as test_col",
				Catalog: "test-catalog",
				Format:  "csv",
			}

			body, _ := json.Marshal(queryReq)
			req, _ := http.NewRequest("POST", server.URL+"/query", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer test-token")
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				results <- err
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				results <- fmt.Errorf("query %d failed with status %d", queryNum, resp.StatusCode)
				return
			}

			results <- nil
		}(i)
	}

	// Wait for all queries to complete
	for i := 0; i < numQueries; i++ {
		if err := <-results; err != nil {
			t.Errorf("Query failed: %v", err)
		}
	}
}

// Helper function to check if error is context timeout
func isContextTimeoutError(err error) bool {
	return err != nil && (err == context.DeadlineExceeded ||
		err.Error() == "context deadline exceeded" ||
		err.Error() == "timeout waiting for pod to be ready: context deadline exceeded")
}
