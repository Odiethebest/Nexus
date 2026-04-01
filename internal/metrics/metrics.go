// Package metrics defines all Prometheus metrics for Nexus.
// Import this package for its side-effect registration; use the exported
// vars to record observations.
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// MessagesPublished counts events successfully published to the exchange.
	// Labels: type (event type), priority.
	MessagesPublished = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexus_messages_published_total",
			Help: "Total number of events published to the exchange.",
		},
		[]string{"type", "priority"},
	)

	// MessagesProcessed counts delivery attempts by worker channel and outcome.
	// Labels: channel (email|inapp|webhook), status (delivered|failed|duplicate|dlq).
	MessagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexus_messages_processed_total",
			Help: "Total number of messages processed by workers.",
		},
		[]string{"channel", "status"},
	)

	// PublishDuration measures end-to-end publish latency including broker ack.
	PublishDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "nexus_publish_duration_seconds",
		Help:    "Latency of event publish including broker confirm.",
		Buckets: prometheus.DefBuckets,
	})

	// ProcessDuration measures per-message worker processing time.
	// Labels: channel (email|inapp|webhook).
	ProcessDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nexus_worker_process_duration_seconds",
			Help:    "Time spent processing a single message per worker channel.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"channel"},
	)
)

func init() {
	prometheus.MustRegister(
		MessagesPublished,
		MessagesProcessed,
		PublishDuration,
		ProcessDuration,
	)
}
