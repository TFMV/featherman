package query

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// responseRecorder wraps http.ResponseWriter to capture response before sending
type responseRecorder struct {
	w      http.ResponseWriter
	body   bytes.Buffer
	status int
	header http.Header
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{
		w:      w,
		status: 200, // default status
		header: make(http.Header),
	}
}

func (r *responseRecorder) Header() http.Header {
	return r.header
}

func (r *responseRecorder) Write(data []byte) (int, error) {
	return r.body.Write(data)
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.status = statusCode
}

func (r *responseRecorder) flush() {
	// Copy our headers to the real response writer
	for k, v := range r.header {
		r.w.Header()[k] = v
	}

	// Write status and body
	r.w.WriteHeader(r.status)
	r.body.WriteTo(r.w)
}

// Router provides HTTP routes for querying
func NewRouter(exec ExecutorInterface) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" {
			http.Error(w, "missing bearer token", http.StatusUnauthorized)
			return
		}
		var req QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if req.Format == "" {
			req.Format = "csv"
		}

		// Use response recorder to capture response
		recorder := newResponseRecorder(w)
		start := time.Now()

		podName, err := exec.Execute(r.Context(), req, recorder)
		duration := time.Since(start)

		// Set headers before flushing
		recorder.Header().Set("X-Featherman-Catalog", req.Catalog)
		recorder.Header().Set("X-Featherman-Duration", duration.String())

		if err != nil {
			log.Error().Err(err).Msg("query failed")
			recorder.WriteHeader(http.StatusInternalServerError)
			recorder.body.Reset() // Clear any response data
			recorder.body.WriteString(err.Error())
		} else {
			recorder.Header().Set("X-Featherman-Pod", podName)
		}

		// Flush the recorded response
		recorder.flush()
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})
	return mux
}

// QueryRequest represents an incoming query
type QueryRequest struct {
	SQL     string `json:"sql"`
	Format  string `json:"format"`
	Catalog string `json:"catalog"`
}
