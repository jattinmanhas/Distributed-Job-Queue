package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jattin/distributed-job-queue/internal/models"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer wraps a franz-go client used to publish jobs to Kafka topics.
type Producer struct {
	client *kgo.Client
}

// NewProducer connects to the given Kafka brokers and returns a Producer.
func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer client: %w", err)
	}

	// Verify connectivity early so startup fails fast on a bad broker config.
	if err := client.Ping(context.Background()); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping kafka brokers %v: %w", brokers, err)
	}

	return &Producer{client: client}, nil
}

// Publish serializes the job as JSON and produces it to the given topic. The
// job's JobID is used as the message key so the same job always lands on the
// same partition. It blocks until the broker acknowledges the record.
func (p *Producer) Publish(ctx context.Context, topic string, job models.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job %s: %w", job.JobID, err)
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(job.JobID),
		Value: data,
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce job %s to topic %s: %w", job.JobID, topic, err)
	}
	return nil
}

// Close flushes any buffered records and shuts down the underlying client.
func (p *Producer) Close() {
	p.client.Close()
}
