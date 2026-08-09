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

	// StageProcessingDuration measures the worker's fetch → commit critical
	// section for a single record (idempotency check + delivery + persist
	// + offset commit). Second leg of the three-stage e2e trace.
	StageProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nexus_stage_processing_duration_seconds",
			Help:    "Per-record worker processing time (idempotency + deliver + persist + commit).",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"channel"},
	)

	// StageDeliveryDuration isolates the dispatch step alone (SMTP send /
	// WebSocket broadcast / outbound HTTP POST), so callers can tell
	// "worker is slow" from "downstream is slow". Third leg of the trace.
	StageDeliveryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nexus_stage_delivery_duration_seconds",
			Help:    "Time spent in the actual channel dispatch call.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		},
		[]string{"channel"},
	)

	// EventE2ELag is the age of an event when a worker starts processing
	// it — computed as (now - x-produced-at). This is the "consumer lag
	// in seconds" figure the resume points at ("lag < 1.5s"). Distinct
	// from ConsumerLagRecords (Kafka-side offset gap gauge).
	EventE2ELag = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nexus_event_e2e_lag_seconds",
			Help:    "Age of the event when the consumer picks it up (now - x-produced-at).",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 1.5, 2, 5, 10, 30},
		},
		[]string{"channel"},
	)

	// ConsumerLagRecords is the classic Kafka offset gap per lane
	// (end offset − committed offset). Wired by internal/kbroker/lag.go
	// in Step 4.
	ConsumerLagRecords = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nexus_consumer_lag_records",
			Help: "End offset minus committed offset for each (channel, priority) consumer group.",
		},
		[]string{"channel", "priority"},
	)

	// DLQMessages approximates the number of records sitting in a
	// dead-letter topic (= end offset of the DLQ topic). Used by the
	// metrics summary to replace the previously hardcoded zero.
	DLQMessages = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nexus_dlq_messages_total",
			Help: "Approximate DLQ backlog per (channel, priority), sampled from end offsets.",
		},
		[]string{"channel", "priority"},
	)

	// CacheHits / CacheMisses track the cache-aside path in front of the
	// notifications store. scope label distinguishes "by_id" (the hot
	// path — one row per message_id, TTL 60s) from "list" (short-TTL
	// query cache for the paged list endpoint). The RUNBOOK's 95%
	// hit-rate figure is scope="by_id" only.
	CacheHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexus_cache_hits_total",
			Help: "Cache-aside hits, labeled by scope (by_id | list).",
		},
		[]string{"scope"},
	)
	CacheMisses = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexus_cache_misses_total",
			Help: "Cache-aside misses, labeled by scope (by_id | list).",
		},
		[]string{"scope"},
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
		ProcessDuration,
		StageIngestDuration,
		EventsPublished,
		StageProcessingDuration,
		StageDeliveryDuration,
		EventE2ELag,
		ConsumerLagRecords,
		DLQMessages,
		CacheHits,
		CacheMisses,
		LoadtestStartTotal,
		LoadtestUpstreamLatency,
		LoadtestActiveRuns,
		LoadtestHealthScore,
	)
}
