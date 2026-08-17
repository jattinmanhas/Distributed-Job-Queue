// Package metrics defines the Prometheus instruments exposed by the worker.
//
// All instruments are registered with the Prometheus default registry via
// promauto, so importing this package is enough to make them available on the
// /metrics endpoint — no manual prometheus.Register calls are required.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// JobsProcessed counts jobs that completed successfully.
	JobsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_processed_total",
		Help: "Total number of jobs successfully completed.",
	})

	// JobsFailed counts jobs that failed and were requeued for retry.
	JobsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_failed_total",
		Help: "Total number of jobs that failed and were requeued.",
	})

	// JobsDLQ counts jobs routed to the dead-letter queue.
	JobsDLQ = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_dlq_total",
		Help: "Total number of jobs routed to the dead-letter queue.",
	})

	// JobDuration observes end-to-end job processing latency in seconds,
	// measured from when the worker picks up a job to when it completes or fails.
	JobDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "job_processing_duration_seconds",
		Help:    "Job processing duration in seconds.",
		Buckets: prometheus.DefBuckets,
	})
)
