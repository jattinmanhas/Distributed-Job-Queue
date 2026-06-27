package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// NewRouter wires up all HTTP routes for the API.
func NewRouter(h *Handler) http.Handler {
	r := chi.NewRouter()

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	r.Post("/jobs", h.JobHandler)
	r.Get("/jobs/{id}", h.GetJobHandler)

	return r
}
