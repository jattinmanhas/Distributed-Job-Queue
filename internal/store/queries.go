package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jattin/distributed-job-queue/internal/models"
)

// ErrJobNotFound is returned when a job does not exist in the store.
var ErrJobNotFound = errors.New("job not found")

// InsertJob persists a new job. Only the client-provided fields are written;
// the remaining columns fall back to their database defaults.
func (s *Store) InsertJob(ctx context.Context, job models.Job) error {
	const q = `
		INSERT INTO jobs (job_id, payload)
		VALUES ($1, $2)
	`
	if _, err := s.pool.Exec(ctx, q, job.JobID, job.Payload); err != nil {
		return fmt.Errorf("insert job %s: %w", job.JobID, err)
	}
	return nil
}

// GetJobByID fetches a single job by its ID. It returns ErrJobNotFound when no
// row matches.
func (s *Store) GetJobByID(ctx context.Context, jobID string) (models.Job, error) {
	const q = `
		SELECT job_id, payload, status, attempt_count, max_retries,
		       last_error, worker_id, created_at, updated_at,
		       started_at, completed_at
		FROM jobs
		WHERE job_id = $1
	`
	var job models.Job
	err := s.pool.QueryRow(ctx, q, jobID).Scan(
		&job.JobID,
		&job.Payload,
		&job.Status,
		&job.AttemptCount,
		&job.MaxRetries,
		&job.LastError,
		&job.WorkerID,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.StartedAt,
		&job.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.Job{}, ErrJobNotFound
		}
		return models.Job{}, fmt.Errorf("get job %s: %w", jobID, err)
	}
	return job, nil
}

// UpdateJobStatus sets the job's status and worker_id and refreshes updated_at.
// When the status is "running", started_at is also set (only if not already
// set, so retries keep the original start time).
func (s *Store) UpdateJobStatus(ctx context.Context, jobID string, status string, workerID string) error {
	const q = `
		UPDATE jobs
		SET status = $2,
		    worker_id = $3,
		    started_at = CASE
		        WHEN $2 = 'running' AND started_at IS NULL THEN NOW()
		        ELSE started_at
		    END,
		    updated_at = NOW()
		WHERE job_id = $1
	`
	tag, err := s.pool.Exec(ctx, q, jobID, status, workerID)
	if err != nil {
		return fmt.Errorf("update status for job %s: %w", jobID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotFound
	}
	return nil
}

// CompleteJob marks the job completed and stamps completed_at.
func (s *Store) CompleteJob(ctx context.Context, jobID string) error {
	const q = `
		UPDATE jobs
		SET status = 'completed',
		    completed_at = NOW(),
		    updated_at = NOW()
		WHERE job_id = $1
	`
	tag, err := s.pool.Exec(ctx, q, jobID)
	if err != nil {
		return fmt.Errorf("complete job %s: %w", jobID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotFound
	}
	return nil
}

// FailJob marks the job failed and records the error and attempt count.
func (s *Store) FailJob(ctx context.Context, jobID string, errMsg string, attemptCount int) error {
	const q = `
		UPDATE jobs
		SET status = 'failed',
		    last_error = $2,
		    attempt_count = $3,
		    updated_at = NOW()
		WHERE job_id = $1
	`
	tag, err := s.pool.Exec(ctx, q, jobID, errMsg, attemptCount)
	if err != nil {
		return fmt.Errorf("fail job %s: %w", jobID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotFound
	}
	return nil
}

// IncrementAttempt atomically increments attempt_count, stores the latest
// error, and returns the new attempt_count.
func (s *Store) IncrementAttempt(ctx context.Context, jobID string, errMsg string) (int, error) {
	const q = `
		UPDATE jobs
		SET attempt_count = attempt_count + 1,
		    last_error = $2,
		    updated_at = NOW()
		WHERE job_id = $1
		RETURNING attempt_count
	`
	var attemptCount int
	err := s.pool.QueryRow(ctx, q, jobID, errMsg).Scan(&attemptCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrJobNotFound
		}
		return 0, fmt.Errorf("increment attempt for job %s: %w", jobID, err)
	}
	return attemptCount, nil
}
