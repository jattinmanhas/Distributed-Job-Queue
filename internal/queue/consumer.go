package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/jattin/distributed-job-queue/internal/models"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer wraps a franz-go client configured as part of a consumer group.
//
// Auto-committing is disabled: offsets are committed manually only after a
// message has been successfully handed off to the jobs channel, preserving the
// "commit only after the work is durably accepted" guarantee.
type Consumer struct {
	client *kgo.Client
}

// NewConsumer connects to the given brokers and joins the consumer group for
// the supplied topics.
func NewConsumer(brokers []string, groupID string, topics []string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer client: %w", err)
	}

	if err := client.Ping(context.Background()); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping kafka brokers %v: %w", brokers, err)
	}

	return &Consumer{client: client}, nil
}

// Start polls Kafka in a loop, deserializes each record into a models.Job, and
// pushes it onto jobsChan. The offset for a batch is committed only after every
// record in that batch has been accepted by the channel. It returns when ctx is
// cancelled.
func (c *Consumer) Start(ctx context.Context, jobsChan chan<- models.Job) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			// Context cancellation surfaces here on shutdown; treat it as clean.
			for _, fe := range errs {
				if errors.Is(fe.Err, context.Canceled) {
					return ctx.Err()
				}
				log.Printf("kafka fetch error on topic %s partition %d: %v",
					fe.Topic, fe.Partition, fe.Err)
			}
			continue
		}

		var pushErr error
		fetches.EachRecord(func(rec *kgo.Record) {
			if pushErr != nil {
				return
			}
			var job models.Job
			if err := json.Unmarshal(rec.Value, &job); err != nil {
				// A malformed message can never be processed; log and skip it so
				// it does not block the partition forever.
				log.Printf("error unmarshaling kafka message (topic=%s partition=%d offset=%d): %v",
					rec.Topic, rec.Partition, rec.Offset, err)
				return
			}

			select {
			case jobsChan <- job:
				log.Printf("consumed job_id=%s from topic=%s partition=%d offset=%d",
					job.JobID, rec.Topic, rec.Partition, rec.Offset)
			case <-ctx.Done():
				pushErr = ctx.Err()
			}
		})

		if pushErr != nil {
			return pushErr
		}

		// Commit only after every fetched record was accepted by the channel.
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			log.Printf("error committing kafka offsets: %v", err)
		}
	}
}

// Close leaves the consumer group and shuts down the underlying client.
func (c *Consumer) Close() {
	c.client.Close()
}
