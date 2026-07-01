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
	// Deprecated: kept for the legacy AMQP path. Kafka path uses
	// StageIngestDuration.
	PublishDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "nexus_publish_duration_seconds",
		Help:    "Latency of event publish including broker confirm (legacy AMQP path).",
		Buckets: prometheus.DefBuckets,
	})

	// ProcessDuration measures per-message worker processing time.
	// Labels: channel (email|inapp|webhook).
	// Deprecated: retained during migration; new dashboards should use
	// StageProcessingDuration.
	ProcessDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nexus_worker_process_duration_seconds",
			Help:    "Time spent processing a single message per worker channel.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"channel"},
	)

	// StageIngestDuration measures how long the producer takes to acknowledge
	// a publish (client submit → broker ack via kgo.Client.Produce callback).
	// This is the "ingestion" leg of the end-to-end tracing set: ingest →
	// processing → delivery.
	StageIngestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nexus_stage_ingest_duration_seconds",
			Help:    "Kafka produce latency (submit → broker ack). Ingest stage of the three-stage trace.",
			Buckets: []float64{0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2},
		},
		[]string{"channel", "priority"},
	)

	// EventsPublished counts every event successfully committed by the Kafka
	// publisher. Distinct from MessagesProcessed so the summary endpoint can
	// report publish-rate and processed-rate separately (previously conflated).
	EventsPublished = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexus_events_published_total",
			Help: "Total events successfully produced to Kafka (post broker ack).",
		},
		[]string{"channel", "priority"},
	)

	// LoadtestStartTotal counts start endpoint outcomes.
	// Labels: status (ok|deny|error).
	LoadtestStartTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexus_loadtest_start_total",
			Help: "Total number of loadtest start attempts by outcome.",
		},
		[]string{"status"},
	)

	// LoadtestUpstreamLatency measures upstream k6 API request latency.
	// Labels: endpoint (start|run|query|other).
	LoadtestUpstreamLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nexus_loadtest_upstream_latency_seconds",
			Help:    "Latency of upstream k6 API calls.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"endpoint"},
	)

	// LoadtestActiveRuns reports current active run count tracked by guard.
	LoadtestActiveRuns = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nexus_loadtest_active_runs",
		Help: "Current number of active loadtest runs.",
	})

	// LoadtestHealthScore publishes the latest computed health score.
	LoadtestHealthScore = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nexus_loadtest_health_score",
		Help: "Latest computed loadtest health score (0-100).",
	})
)

func init() {
	prometheus.MustRegister(
		MessagesPublished,
		MessagesProcessed,
		PublishDuration,
		ProcessDuration,
		StageIngestDuration,
		EventsPublished,
		LoadtestStartTotal,
		LoadtestUpstreamLatency,
		LoadtestActiveRuns,
		LoadtestHealthScore,
	)
}
