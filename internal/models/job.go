package models

import (
	"encoding/json"
	"time"
)

// Job represents a unit of work in the distributed job queue.
type Job struct {
	JobID        string     `json:"job_id"`
	Payload      json.RawMessage `json:"payload"`
	Status       string     `json:"status"`
	AttemptCount int        `json:"attempt_count"`
	MaxRetries   int        `json:"max_retries"`
	LastError    *string    `json:"last_error"`
	WorkerID     *string    `json:"worker_id"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	StartedAt    *time.Time `json:"started_at"`
	CompletedAt  *time.Time `json:"completed_at"`
}
