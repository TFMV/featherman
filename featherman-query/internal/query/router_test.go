package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// FakeExecutor implements ExecutorInterface for testing
type FakeExecutor struct {
	shouldFail bool
	response   string
	podName    string
}

// Ensure FakeExecutor implements ExecutorInterface
var _ ExecutorInterface = (*FakeExecutor)(nil)

func (f *FakeExecutor) Execute(ctx context.Context, req QueryRequest, w http.ResponseWriter) (string, error) {
	if f.shouldFail {
		return "", fmt.Errorf("fake execution failed")
	}

	response := f.response
	if response == "" {
		response = "id,name\n1,test\n"
	}

	podName := f.podName
	if podName == "" {
		podName = "fake-pod"
	}

	// Set appropriate content type based on format
	switch req.Format {
	case "json":
		w.Header().Set("Content-Type", "application/json")
	case "arrow":
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	case "parquet":
		w.Header().Set("Content-Type", "application/octet-stream")
	default:
		w.Header().Set("Content-Type", "text/csv")
	}

	w.Write([]byte(response))
	return podName, nil
}

func TestRouterQuerySuccess(t *testing.T) {
	executor := &FakeExecutor{response: "id,name\n1,test\n", podName: "test-pod"}
	r := NewRouter(executor)
	ts := httptest.NewServer(r)
	defer ts.Close()

	body, _ := json.Marshal(QueryRequest{SQL: "SELECT * FROM test", Catalog: "test-catalog"})
	req, _ := http.NewRequest("POST", ts.URL+"/query", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer valid-token")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		t.Fatalf("expected 200 got %d", res.StatusCode)
	}

	// Check headers
	if catalog := res.Header.Get("X-Featherman-Catalog"); catalog != "test-catalog" {
		t.Errorf("expected catalog header 'test-catalog', got '%s'", catalog)
	}

	if pod := res.Header.Get("X-Featherman-Pod"); pod != "test-pod" {
		t.Errorf("expected pod header 'test-pod', got '%s'", pod)
	}

	if duration := res.Header.Get("X-Featherman-Duration"); duration == "" {
		t.Error("expected duration header to be set")
	}

	// Check response body
	body, err = io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}

	expected := "id,name\n1,test\n"
	if string(body) != expected {
		t.Errorf("expected response '%s', got '%s'", expected, string(body))
	}
}

func TestRouterQueryFailure(t *testing.T) {
	executor := &FakeExecutor{shouldFail: true}
	r := NewRouter(executor)
	ts := httptest.NewServer(r)
	defer ts.Close()

	body, _ := json.Marshal(QueryRequest{SQL: "SELECT * FROM test", Catalog: "test-catalog"})
	req, _ := http.NewRequest("POST", ts.URL+"/query", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer valid-token")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != 500 {
		t.Fatalf("expected 500 got %d", res.StatusCode)
	}
}

func TestRouterMissingAuth(t *testing.T) {
	executor := &FakeExecutor{}
	r := NewRouter(executor)
	ts := httptest.NewServer(r)
	defer ts.Close()

	body, _ := json.Marshal(QueryRequest{SQL: "SELECT * FROM test", Catalog: "test-catalog"})
	req, _ := http.NewRequest("POST", ts.URL+"/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	// No Authorization header

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != 401 {
		t.Fatalf("expected 401 got %d", res.StatusCode)
	}
}

func TestRouterInvalidMethod(t *testing.T) {
	executor := &FakeExecutor{}
	r := NewRouter(executor)
	ts := httptest.NewServer(r)
	defer ts.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/query", nil)
	req.Header.Set("Authorization", "Bearer valid-token")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != 405 {
		t.Fatalf("expected 405 got %d", res.StatusCode)
	}
}

func TestRouterInvalidJSON(t *testing.T) {
	executor := &FakeExecutor{}
	r := NewRouter(executor)
	ts := httptest.NewServer(r)
	defer ts.Close()

	req, _ := http.NewRequest("POST", ts.URL+"/query", strings.NewReader("invalid json"))
	req.Header.Set("Authorization", "Bearer valid-token")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != 400 {
		t.Fatalf("expected 400 got %d", res.StatusCode)
	}
}

func TestRouterHealthCheck(t *testing.T) {
	executor := &FakeExecutor{}
	r := NewRouter(executor)
	ts := httptest.NewServer(r)
	defer ts.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/healthz", nil)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		t.Fatalf("expected 200 got %d", res.StatusCode)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}

	if string(body) != "ok" {
		t.Errorf("expected 'ok', got '%s'", string(body))
	}
}

func TestRouterDefaultFormat(t *testing.T) {
	executor := &FakeExecutor{}
	r := NewRouter(executor)
	ts := httptest.NewServer(r)
	defer ts.Close()

	// Request without format should default to CSV
	body, _ := json.Marshal(QueryRequest{SQL: "SELECT * FROM test", Catalog: "test-catalog"})
	req, _ := http.NewRequest("POST", ts.URL+"/query", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer valid-token")
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		t.Fatalf("expected 200 got %d", res.StatusCode)
	}
}
