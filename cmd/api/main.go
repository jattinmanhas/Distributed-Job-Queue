package main

import (
	"context"
	"log"
	"net/http"

	"github.com/jattin/distributed-job-queue/internal/api"
	"github.com/jattin/distributed-job-queue/internal/config"
	"github.com/jattin/distributed-job-queue/internal/store"
)

func main() {
	cfg := config.Load()

	ctx := context.Background()

	st, err := store.New(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer st.Close()

	if err := st.Migrate(ctx); err != nil {
		log.Fatalf("failed to run migration: %v", err)
	}

	handler := api.NewHandler(st)
	router := api.NewRouter(handler)

	addr := ":" + cfg.ServerPort
	log.Printf("API server starting on port %s", cfg.ServerPort)
	if err := http.ListenAndServe(addr, router); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
