package metrics

import (
	"math"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// SummarySnapshot is the response body for GET /api/metrics/summary.
type SummarySnapshot struct {
	PublishRatePerSec      float64        `json:"publish_rate_per_sec"`
	ProcessedRatePerSec    float64        `json:"processed_rate_per_sec"`
	ProcessingLatencyP99MS float64        `json:"processing_latency_p99_ms"`
	// E2ELagP99Seconds is the p99 of (now - x-produced-at) observed by the
	// worker when picking up records. This is the "consumer lag in seconds"
	// figure the resume points at ("lag < 1.5s"). Sourced from
	// nexus_event_e2e_lag_seconds; empty when no events have been
	// processed yet.
	E2ELagP99Seconds       float64        `json:"e2e_lag_p99_seconds"`
	QueueDepth             map[string]int `json:"queue_depth"`
	DeliverySuccessRate    float64        `json:"delivery_success_rate"`
	DLQCount               int            `json:"dlq_count"`
	ActiveWSConnections    int            `json:"active_ws_connections"`
	UptimeSeconds          int            `json:"uptime_seconds"`
}

var startTime = time.Now()

// rateTracker computes a per-second rate from a monotonically increasing counter.
type rateTracker struct {
	mu       sync.Mutex
	prevVal  float64
	prevTime time.Time
	rate     float64
}

func (rt *rateTracker) update(current float64) float64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	now := time.Now()
	if rt.prevTime.IsZero() {
		rt.prevVal = current
		rt.prevTime = now
		return 0
	}
	elapsed := now.Sub(rt.prevTime).Seconds()
	if elapsed >= 1.0 {
		delta := current - rt.prevVal
		if delta >= 0 {
			rt.rate = delta / elapsed
		}
		rt.prevVal = current
		rt.prevTime = now
	}
	return rt.rate
}

// Publish and processed are conceptually distinct — publish counts what the
// producer accepted, processed counts what workers completed. Under
// backpressure they diverge, which is exactly what we want the summary to
// surface.
var (
	publishedRate = &rateTracker{}
	processedRate = &rateTracker{}
)

// fetchWorkerMetrics fetches and parses the Prometheus text exposition from the
// worker's metrics endpoint (METRICS_INTERNAL_URL, default http://localhost:9091/metrics).
func fetchWorkerMetrics() map[string]*dto.MetricFamily {
	url := os.Getenv("METRICS_INTERNAL_URL")
	if url == "" {
		url = "http://localhost:9091/metrics"
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var parser expfmt.TextParser
	mfs, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil && mfs == nil {
		return nil
	}
	return mfs
}

// gatherLocalMetrics returns the same shape as fetchWorkerMetrics but pulled
// from the in-process Prometheus registry. Producer-side counters/gauges
// (publish rate, ingest histogram) live here.
func gatherLocalMetrics() map[string]*dto.MetricFamily {
	fams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return nil
	}
	out := make(map[string]*dto.MetricFamily, len(fams))
	for _, mf := range fams {
		out[mf.GetName()] = mf
	}
	return out
}

// mergeMetricFamilies overlays local values on top of remote ones, keyed by
// metric name. Local wins on collision — the producer's own counters are
// authoritative for the metrics it owns.
func mergeMetricFamilies(remote, local map[string]*dto.MetricFamily) map[string]*dto.MetricFamily {
	if len(remote) == 0 {
		return local
	}
	out := make(map[string]*dto.MetricFamily, len(remote)+len(local))
	for k, v := range remote {
		out[k] = v
	}
	for k, v := range local {
		out[k] = v
	}
	return out
}

// ComputeSummary fetches worker metrics and returns a SummarySnapshot.
// wsCount is the number of currently connected WebSocket clients.
func ComputeSummary(wsCount int) SummarySnapshot {
	remote := fetchWorkerMetrics()
	local := gatherLocalMetrics()
	mfs := mergeMetricFamilies(remote, local)

	var (
		processedTotal float64
		deliveredTotal float64
		publishedTotal float64
	)

	// bucket accumulation for histogram p99 (processing latency)
	procBuckets := make(map[float64]uint64)
	var procHistCount uint64
	// bucket accumulation for e2e lag p99 (seconds)
	lagBuckets := make(map[float64]uint64)
	var lagHistCount uint64

	// per-lane gauges (channel, priority) → value
	queueDepth := map[string]int{}
	dlqTotal := 0

	for name, mf := range mfs {
		switch name {
		case "nexus_messages_processed_total":
			for _, m := range mf.GetMetric() {
				val := m.GetCounter().GetValue()
				processedTotal += val
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "status" && lp.GetValue() == "delivered" {
						deliveredTotal += val
					}
				}
			}

		case "nexus_events_published_total":
			for _, m := range mf.GetMetric() {
				publishedTotal += m.GetCounter().GetValue()
			}

		case "nexus_stage_processing_duration_seconds",
			"nexus_worker_process_duration_seconds":
			// Prefer stage_processing when both are present. If both,
			// stage_processing overwrites via the second iteration (map
			// order isn't guaranteed, so accumulate both — same units).
			for _, m := range mf.GetMetric() {
				h := m.GetHistogram()
				if h == nil {
					continue
				}
				procHistCount += h.GetSampleCount()
				for _, b := range h.GetBucket() {
					procBuckets[b.GetUpperBound()] += b.GetCumulativeCount()
				}
			}

		case "nexus_event_e2e_lag_seconds":
			for _, m := range mf.GetMetric() {
				h := m.GetHistogram()
				if h == nil {
					continue
				}
				lagHistCount += h.GetSampleCount()
				for _, b := range h.GetBucket() {
					lagBuckets[b.GetUpperBound()] += b.GetCumulativeCount()
				}
			}

		case "nexus_consumer_lag_records":
			for _, m := range mf.GetMetric() {
				g := m.GetGauge()
				if g == nil {
					continue
				}
				var channel, priority string
				for _, lp := range m.GetLabel() {
					switch lp.GetName() {
					case "channel":
						channel = lp.GetValue()
					case "priority":
						priority = lp.GetValue()
					}
				}
				if channel != "" && priority != "" {
					queueDepth[channel+"_"+priority] = int(g.GetValue())
				}
			}

		case "nexus_dlq_messages_total":
			for _, m := range mf.GetMetric() {
				g := m.GetGauge()
				if g == nil {
					continue
				}
				dlqTotal += int(g.GetValue())
			}
		}
	}

	if len(queueDepth) == 0 {
		queueDepth = emptyQueueDepth()
	} else {
		// Backfill any missing lane so the response shape is stable for
		// frontend consumers.
		for k := range emptyQueueDepth() {
			if _, ok := queueDepth[k]; !ok {
				queueDepth[k] = 0
			}
		}
	}

	var successRate float64
	if processedTotal > 0 {
		successRate = math.Round(deliveredTotal/processedTotal*10000) / 10000
	}

	return SummarySnapshot{
		PublishRatePerSec:      math.Round(publishedRate.update(publishedTotal)*10) / 10,
		ProcessedRatePerSec:    math.Round(processedRate.update(processedTotal)*10) / 10,
		ProcessingLatencyP99MS: histP99MS(procBuckets, procHistCount),
		E2ELagP99Seconds:       histP99Sec(lagBuckets, lagHistCount),
		QueueDepth:             queueDepth,
		DeliverySuccessRate:    successRate,
		DLQCount:               dlqTotal,
		ActiveWSConnections:    wsCount,
		UptimeSeconds:          int(time.Since(startTime).Seconds()),
	}
}

// histP99MS computes the approximate P99 latency in milliseconds from
// aggregated histogram bucket data.
func histP99MS(buckets map[float64]uint64, totalCount uint64) float64 {
	sec := histP99Sec(buckets, totalCount)
	return math.Round(sec*1000*10) / 10
}

// histP99Sec computes the approximate P99 from bucket data in seconds
// (native histogram unit).
func histP99Sec(buckets map[float64]uint64, totalCount uint64) float64 {
	if totalCount == 0 || len(buckets) == 0 {
		return 0
	}

	target := uint64(math.Ceil(float64(totalCount) * 0.99))

	bounds := make([]float64, 0, len(buckets))
	for b := range buckets {
		bounds = append(bounds, b)
	}
	sort.Float64s(bounds)

	prevBound := 0.0
	prevCount := uint64(0)

	for _, ub := range bounds {
		count := buckets[ub]
		if count >= target {
			if count == prevCount {
				return ub
			}
			frac := float64(target-prevCount) / float64(count-prevCount)
			return prevBound + frac*(ub-prevBound)
		}
		prevBound = ub
		prevCount = count
	}

	if len(bounds) > 0 {
		return bounds[len(bounds)-1]
	}
	return 0
}

func emptyQueueDepth() map[string]int {
	return map[string]int{
		"email_high": 0, "email_normal": 0, "email_low": 0,
		"inapp_high": 0, "inapp_normal": 0, "inapp_low": 0,
		"webhook_high": 0, "webhook_normal": 0, "webhook_low": 0,
	}
}
