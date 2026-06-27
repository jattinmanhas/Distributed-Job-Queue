package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jattin/distributed-job-queue/internal/models"
	"github.com/jattin/distributed-job-queue/internal/store"
)

// jobStore is the subset of the store needed by the HTTP handlers.
type jobStore interface {
	InsertJob(ctx context.Context, job models.Job) error
	GetJobByID(ctx context.Context, jobID string) (models.Job, error)
}

// Handler holds dependencies for the job API handlers.
type Handler struct {
	store jobStore
}

// NewHandler constructs a Handler with the given store.
func NewHandler(s jobStore) *Handler {
	return &Handler{store: s}
}

type createJobRequest struct {
	JobID   string          `json:"job_id"`
	Payload json.RawMessage `json:"payload"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("error encoding response: %v", err)
	}
}

// JobHandler handles POST /jobs with PostgreSQL-backed idempotency.
func (h *Handler) JobHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("error decoding request body: %v", err)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}

	if req.JobID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id is required"})
		return
	}
	if len(req.Payload) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "payload is required"})
		return
	}

	// Idempotency check: if the job already exists, return it.
	existing, err := h.store.GetJobByID(ctx, req.JobID)
	if err == nil {
		writeJSON(w, http.StatusOK, existing)
		return
	}
	if !errors.Is(err, store.ErrJobNotFound) {
		log.Printf("error checking existing job %s: %v", req.JobID, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	job := models.Job{
		JobID:   req.JobID,
		Payload: req.Payload,
	}
	if err := h.store.InsertJob(ctx, job); err != nil {
		log.Printf("error inserting job %s: %v", req.JobID, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"job_id": req.JobID,
		"status": "pending",
	})
}

// GetJobHandler handles GET /jobs/{id}.
func (h *Handler) GetJobHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	jobID := chi.URLParam(r, "id")
	if jobID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job id is required"})
		return
	}

	job, err := h.store.GetJobByID(ctx, jobID)
	if err != nil {
		if errors.Is(err, store.ErrJobNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
			return
		}
		log.Printf("error fetching job %s: %v", jobID, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	writeJSON(w, http.StatusOK, job)
}
