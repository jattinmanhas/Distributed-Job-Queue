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
