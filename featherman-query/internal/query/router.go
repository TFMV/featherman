package query

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// Router provides HTTP routes for querying
func NewRouter(exec *Executor) http.Handler {
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
		start := time.Now()
		podName, err := exec.Execute(r.Context(), req, w)
		if err != nil {
			log.Error().Err(err).Msg("query failed")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Featherman-Catalog", req.Catalog)
		w.Header().Set("X-Featherman-Pod", podName)
		w.Header().Set("X-Featherman-Duration", time.Since(start).String())
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
