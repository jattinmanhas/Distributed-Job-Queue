package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jattin/distributed-job-queue/internal/config"
	"github.com/jattin/distributed-job-queue/internal/queue"
	"github.com/jattin/distributed-job-queue/internal/store"
	"github.com/jattin/distributed-job-queue/internal/worker"
)

const defaultMaxRetries = 3

func main() {
	cfg := config.Load()

	// Root context cancelled on SIGINT/SIGTERM to drive graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer st.Close()

	if err := st.Migrate(ctx); err != nil {
		log.Fatalf("failed to run migration: %v", err)
	}

	brokers := strings.Split(cfg.KafkaBrokers, ",")

	producer, err := queue.NewProducer(brokers)
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	consumer, err := queue.NewConsumer(brokers, cfg.KafkaGroupID, []string{cfg.KafkaTopic})
	if err != nil {
		log.Fatalf("failed to create kafka consumer: %v", err)
	}
	defer consumer.Close()

	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("could not determine hostname, falling back to 'worker': %v", err)
		hostname = "worker"
	}

	processor := worker.NewProcessor(st, producer, defaultMaxRetries, hostname)
	pool := worker.NewPool(cfg.WorkerPoolSize, processor)
	pool.Start(ctx)
	log.Printf("worker pool started: host=%s workers=%d topic=%s group=%s",
		hostname, cfg.WorkerPoolSize, cfg.KafkaTopic, cfg.KafkaGroupID)

	// Run the consumer in the foreground. It returns when ctx is cancelled.
	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- consumer.Start(ctx, pool.JobsChan())
	}()

	consumerExited := false
	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received, draining in-flight jobs...")
	case err := <-consumerErr:
		consumerExited = true
		if err != nil && ctx.Err() == nil {
			log.Printf("consumer stopped unexpectedly: %v", err)
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
	log.Printf("all workers drained, worker exiting")
}

