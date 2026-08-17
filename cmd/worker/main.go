package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jattin/distributed-job-queue/internal/config"
	"github.com/jattin/distributed-job-queue/internal/queue"
	"github.com/jattin/distributed-job-queue/internal/ratelimit"
	"github.com/jattin/distributed-job-queue/internal/store"
	"github.com/jattin/distributed-job-queue/internal/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const defaultMaxRetries = 3

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

	// Root context cancelled on SIGINT/SIGTERM to drive graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Expose Prometheus metrics on a lightweight HTTP server (worker only).
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		slog.Info("metrics server listening", "port", cfg.MetricsPort)
		if err := http.ListenAndServe(":"+cfg.MetricsPort, mux); err != nil {
			slog.Error("metrics server error", "error", err)
		}
	}()

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

	consumer, err := queue.NewConsumer(brokers, cfg.KafkaGroupID, []string{cfg.KafkaTopic})
	if err != nil {
		slog.Error("failed to create kafka consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	limiter, err := ratelimit.NewLimiter(
		cfg.RedisAddr,
		cfg.RedisPassword,
		cfg.RedisDB,
		cfg.RateLimitWindow,
		cfg.RateLimitMaxRequests,
		cfg.RateLimitRetryMs,
	)
	if err != nil {
		slog.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer limiter.Close()

	hostname, err := os.Hostname()
	if err != nil {
		slog.Warn("could not determine hostname, falling back to 'worker'", "error", err)
		hostname = "worker"
	}

	processor := worker.NewProcessor(st, producer, limiter, defaultMaxRetries, hostname)
	pool := worker.NewPool(cfg.WorkerPoolSize, processor)
	pool.Start(ctx)
	slog.Info("worker pool started",
		"host", hostname, "workers", cfg.WorkerPoolSize,
		"topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID)

	// Run the consumer in the foreground. It returns when ctx is cancelled.
	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- consumer.Start(ctx, pool.JobsChan())
	}()

	consumerExited := false
	select {
	case <-ctx.Done():
		slog.Info("shutdown signal received, draining in-flight jobs")
	case err := <-consumerErr:
		consumerExited = true
		if err != nil && ctx.Err() == nil {
			slog.Error("consumer stopped unexpectedly", "error", err)
		}
		stop()
	}

	// Stop the consumer and wait for its goroutine to fully exit before closing
	// the jobs channel, so it can never send on a closed channel.
	consumer.Close()
	if !consumerExited {
		<-consumerErr
	}

	// Closing the channel lets workers exit once the buffered jobs are drained.
	close(pool.JobsChan())
	pool.Wait()
	slog.Info("all workers drained, worker exiting")
}

