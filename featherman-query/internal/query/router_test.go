package query

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeExecutor struct{}

func (f *fakeExecutor) Execute(ctx context.Context, req QueryRequest, w http.ResponseWriter) (string, error) {
	w.Write([]byte("ok"))
	return "fake-pod", nil
}

func TestRouter(t *testing.T) {
	r := NewRouter(&fakeExecutor{})
	ts := httptest.NewServer(r)
	defer ts.Close()

	body, _ := json.Marshal(QueryRequest{SQL: "select 1", Catalog: "cat"})
	req, _ := http.NewRequest("POST", ts.URL+"/query", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected 200 got %d", res.StatusCode)
	}
}
