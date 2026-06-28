package worker

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/jattin/distributed-job-queue/internal/models"
)

// jobStore is the subset of the store the processor depends on.
type jobStore interface {
	GetJobByID(ctx context.Context, jobID string) (models.Job, error)
	UpdateJobStatus(ctx context.Context, jobID string, status string, workerID string) error
	CompleteJob(ctx context.Context, jobID string) error
	FailJob(ctx context.Context, jobID string, errMsg string, attemptCount int) error
	IncrementAttempt(ctx context.Context, jobID string, errMsg string) (int, error)
}

// jobProducer is the subset of the Kafka producer the processor depends on.
type jobProducer interface {
	Publish(ctx context.Context, topic string, job models.Job) error
}

// rateLimiter gates job execution behind a per-service rate limit.
type rateLimiter interface {
	Allow(ctx context.Context, service string) error
}

const (
	statusCompleted = "completed"
	statusRunning   = "running"

	jobsTopic = "jobs"
	dlqTopic  = "jobs.dlq"

	backoffBase = 2 * time.Second
)

// Processor executes individual jobs: it enforces the idempotency guard,
// updates job state in PostgreSQL, and handles failure routing (retry with
// exponential backoff or dead-letter).
type Processor struct {
	store      jobStore
	producer   jobProducer
	limiter    rateLimiter
	maxRetries int
	workerID   string
}

// NewProcessor constructs a Processor.
func NewProcessor(store jobStore, producer jobProducer, limiter rateLimiter, maxRetries int, workerID string) *Processor {
	return &Processor{
		store:      store,
		producer:   producer,
		limiter:    limiter,
		maxRetries: maxRetries,
		workerID:   workerID,
	}
}

// Process runs the full lifecycle for a single job. It never panics: any panic
// in the work itself is recovered and treated as a processing error so the
// worker goroutine survives.
func (p *Processor) Process(ctx context.Context, job models.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic processing job %s: %v", job.JobID, r)
			log.Printf("recovered panic for job_id=%s worker_id=%s: %v", job.JobID, p.workerID, r)
			p.handleFailure(ctx, job, err)
		}
	}()

	// Idempotency guard: a job already completed must never be re-run.
	current, getErr := p.store.GetJobByID(ctx, job.JobID)
	if getErr != nil {
		log.Printf("error loading job_id=%s worker_id=%s: %v", job.JobID, p.workerID, getErr)
		return getErr
	}
	if current.Status == statusCompleted {
		log.Printf("skipping already-completed job_id=%s worker_id=%s (idempotency guard)", job.JobID, p.workerID)
		return nil
	}

	// Carry forward the persisted retry config rather than trusting the message.
	job.AttemptCount = current.AttemptCount
	job.MaxRetries = current.MaxRetries

	if procErr := p.doWork(ctx, job); procErr != nil {
		log.Printf("processing failed for job_id=%s worker_id=%s: %v", job.JobID, p.workerID, procErr)
		p.handleFailure(ctx, job, procErr)
		return procErr
	}

	return nil
}

// doWork performs the running -> work -> completed happy path.
func (p *Processor) doWork(ctx context.Context, job models.Job) error {
	if err := p.limiter.Allow(ctx, "job_processor"); err != nil {
		return fmt.Errorf("rate limiter: %w", err)
	}

	if err := p.store.UpdateJobStatus(ctx, job.JobID, statusRunning, p.workerID); err != nil {
		return fmt.Errorf("mark running: %w", err)
	}

	log.Printf("processing job_id=%s worker_id=%s attempt=%d payload=%s",
		job.JobID, p.workerID, job.AttemptCount, string(job.Payload))

	// Simulate actual work.
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := p.store.CompleteJob(ctx, job.JobID); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}

	log.Printf("completed job_id=%s worker_id=%s", job.JobID, p.workerID)
	return nil
}

// handleFailure increments the attempt count and routes the job to either the
// DLQ (retries exhausted) or back onto the jobs topic with exponential backoff.
func (p *Processor) handleFailure(ctx context.Context, job models.Job, cause error) {
	attemptCount, incErr := p.store.IncrementAttempt(ctx, job.JobID, cause.Error())
	if incErr != nil {
		log.Printf("error incrementing attempt for job_id=%s worker_id=%s: %v", job.JobID, p.workerID, incErr)
		return
	}
	job.AttemptCount = attemptCount

	if attemptCount >= p.maxRetries {
		log.Printf("retries exhausted for job_id=%s worker_id=%s (attempt=%d max=%d), routing to DLQ",
			job.JobID, p.workerID, attemptCount, p.maxRetries)

		if err := p.store.FailJob(ctx, job.JobID, cause.Error(), attemptCount); err != nil {
			log.Printf("error marking job_id=%s failed: %v", job.JobID, err)
		}

		// Publish the authoritative, fully-enriched job (status, last_error,
		// timestamps, worker_id) to the DLQ rather than the sparse in-memory copy.
		enriched, err := p.store.GetJobByID(ctx, job.JobID)
		if err != nil {
			log.Printf("error fetching enriched job_id=%s for DLQ: %v", job.JobID, err)
			// fall back to publishing the partial job
			enriched = job
		}
		if err := p.producer.Publish(ctx, dlqTopic, enriched); err != nil {
			log.Printf("error publishing job_id=%s to DLQ: %v", job.JobID, err)
		}
		return
	}

	// wait = base(2s) * 2^attempt_count + random(0, 1000ms)
	backoff := backoffBase*time.Duration(1<<uint(attemptCount)) +
		time.Duration(rand.Intn(1000))*time.Millisecond
	log.Printf("requeueing job_id=%s worker_id=%s (attempt=%d max=%d) after backoff=%s",
		job.JobID, p.workerID, attemptCount, p.maxRetries, backoff)

	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		log.Printf("backoff interrupted for job_id=%s during shutdown", job.JobID)
		return
	}

	if err := p.producer.Publish(ctx, jobsTopic, job); err != nil {
		log.Printf("error requeueing job_id=%s to jobs topic: %v", job.JobID, err)
	}
}
