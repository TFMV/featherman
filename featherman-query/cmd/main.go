package main

import (
	"context"
	"net/http"
	"os"

	"github.com/rs/zerolog/log"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	query "github.com/TFMV/featherman/featherman-query/internal/query"
)

func main() {
	ctx := context.Background()

	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load in-cluster config")
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create k8s client")
	}

	executor := query.NewExecutor(clientset, cfg)
	router := query.NewRouter(executor)

	addr := ":8080"
	if v := os.Getenv("PORT"); v != "" {
		addr = ":" + v
	}
	log.Info().Str("addr", addr).Msg("starting featherman-query")
	srv := &http.Server{Addr: addr, Handler: router}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal().Err(err).Msg("server error")
	}
	<-ctx.Done()
}
