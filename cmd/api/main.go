package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/jattin/distributed-job-queue/internal/api"
	"github.com/jattin/distributed-job-queue/internal/config"
	"github.com/jattin/distributed-job-queue/internal/queue"
	"github.com/jattin/distributed-job-queue/internal/store"
)

// parseLogLevel maps the configured LOG_LEVEL string onto a slog.Level,
// defaulting to info for anything unrecognized.
func parseLogLevel(lvl string) slog.Level {
	switch lvl {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func main() {
	cfg := config.Load()

	// Structured JSON logging as the process-wide default.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))
	slog.SetDefault(logger)

	ctx := context.Background()

	st, err := store.New(ctx, cfg)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	if err := st.Migrate(ctx); err != nil {
		slog.Error("failed to run migration", "error", err)
		os.Exit(1)
	}

	brokers := strings.Split(cfg.KafkaBrokers, ",")
	producer, err := queue.NewProducer(brokers)
	if err != nil {
		slog.Error("failed to create kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	handler := api.NewHandler(st, producer, cfg.KafkaTopic)
	router := api.NewRouter(handler)

	addr := ":" + cfg.ServerPort
	slog.Info("API server starting", "port", cfg.ServerPort)
	if err := http.ListenAndServe(addr, router); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
