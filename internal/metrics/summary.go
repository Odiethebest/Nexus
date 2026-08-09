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
//
// Units differ between the publish and processed figures, deliberately:
// one POST /events is one *event*, which the publisher fans out into one
// *record* per channel. PublishRatePerSec counts events; the processed
// figures count records. At steady state each per-channel processed rate
// should track PublishRatePerSec 1:1, and ProcessedRatePerSec should sit at
// roughly len(channels) × PublishRatePerSec.
type SummarySnapshot struct {
	// PublishRatePerSec is logical events accepted per second, not Kafka
	// records produced per second.
	PublishRatePerSec float64 `json:"publish_rate_per_sec"`

	// ProcessedRatePerSec is records completed per second summed over every
	// channel — one event contributes len(channels) records here.
	ProcessedRatePerSec float64 `json:"processed_rate_per_sec"`

	// ProcessedRatePerSecByChannel breaks the processed rate out per
	// delivery channel, from nexus_messages_processed_total{channel}.
	//
	// Processed — not published — is deliberate. Publish fans one event out
	// to all three channels, so nexus_events_published_total is identical
	// across them by construction and a per-channel publish chart would be
	// three overlapping lines. The processed rate genuinely diverges: the
	// lanes have different pool sizes and very different per-record cost
	// (SMTP vs an in-process WebSocket broadcast vs an outbound HTTP POST).
	// Every known channel is always present, zero-filled, so the response
	// shape is stable for chart consumers.
	ProcessedRatePerSecByChannel map[string]float64 `json:"processed_rate_per_sec_by_channel"`

	ProcessingLatencyP99MS float64 `json:"processing_latency_p99_ms"`

	// E2ELagP99Seconds is the p99 of (now - x-produced-at) observed by the
	// worker when picking up records. This is the "consumer lag in seconds"
	// figure the resume points at ("lag < 1.5s"). Sourced from
	// nexus_event_e2e_lag_seconds; empty when no events have been
	// processed yet.
	E2ELagP99Seconds    float64        `json:"e2e_lag_p99_seconds"`
	QueueDepth          map[string]int `json:"queue_depth"`
	DeliverySuccessRate float64        `json:"delivery_success_rate"`
	DLQCount            int            `json:"dlq_count"`
	ActiveWSConnections int            `json:"active_ws_connections"`
	UptimeSeconds       int            `json:"uptime_seconds"`
}

// channels is the fixed set of delivery channels the summary reports on.
// Mirrors kbroker.Channels; duplicated as plain strings to keep the metrics
// package free of a dependency on the broker package.
var channels = []string{"email", "inapp", "webhook"}

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

// rateTrackerSet is one rateTracker per label value, created on demand. Each
// keeps its own previous sample, so a channel that appears late (its first
// record only just landed) starts from its own baseline instead of
// reporting a spike.
type rateTrackerSet struct {
	mu       sync.Mutex
	trackers map[string]*rateTracker
}

// update advances every tracker named in current and returns the resulting
// per-key rates. Keys absent from current are left untouched rather than
// dropped — a lane that goes briefly quiet keeps its tracker (and therefore
// its baseline) for when traffic resumes.
func (s *rateTrackerSet) update(current map[string]float64) map[string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.trackers == nil {
		s.trackers = make(map[string]*rateTracker, len(current))
	}
	out := make(map[string]float64, len(current))
	for key, value := range current {
		t, ok := s.trackers[key]
		if !ok {
			t = &rateTracker{}
			s.trackers[key] = t
		}
		out[key] = safeFloat(math.Round(t.update(value)*10) / 10)
	}
	return out
}

// Publish and processed are conceptually distinct — publish counts what the
// producer accepted, processed counts what workers completed. Under
// backpressure they diverge, which is exactly what we want the summary to
// surface.
var (
	publishedRate          = &rateTracker{}
	processedRate          = &rateTracker{}
	processedRateByChannel = &rateTrackerSet{}
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
	// cumulative counters per channel, fed to the rate trackers below
	processedByChannel := map[string]float64{}
	publishedByChannel := map[string]float64{}

	for name, mf := range mfs {
		switch name {
		case "nexus_messages_processed_total":
			for _, m := range mf.GetMetric() {
				val := m.GetCounter().GetValue()
				processedTotal += val
				if labelValue(m, "status") == "delivered" {
					deliveredTotal += val
				}
				// Sum across every status, matching the all-status
				// ProcessedRatePerSec scalar: this is records handled per
				// channel, not successes per channel.
				if ch := labelValue(m, "channel"); ch != "" {
					processedByChannel[ch] += val
				}
			}

		case "nexus_events_published_total":
			// This counter is per (channel, priority) record, so summing it
			// yields records, not events. Keep the per-channel split and
			// collapse it below.
			for _, m := range mf.GetMetric() {
				if ch := labelValue(m, "channel"); ch != "" {
					publishedByChannel[ch] += m.GetCounter().GetValue()
				}
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
				channel, priority := labelValue(m, "channel"), labelValue(m, "priority")
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

	// Zero-fill so the chart always receives the same three series, even
	// before a channel has processed its first record.
	byChannel := processedRateByChannel.update(processedByChannel)
	for _, ch := range channels {
		if _, ok := byChannel[ch]; !ok {
			byChannel[ch] = 0
		}
	}

	return SummarySnapshot{
		PublishRatePerSec:            safeFloat(math.Round(publishedRate.update(eventsFromPerChannel(publishedByChannel))*10) / 10),
		ProcessedRatePerSec:          safeFloat(math.Round(processedRate.update(processedTotal)*10) / 10),
		ProcessedRatePerSecByChannel: byChannel,
		ProcessingLatencyP99MS:       safeFloat(histP99MS(procBuckets, procHistCount)),
		E2ELagP99Seconds:             safeFloat(histP99Sec(lagBuckets, lagHistCount)),
		QueueDepth:                   queueDepth,
		DeliverySuccessRate:          safeFloat(successRate),
		DLQCount:                     dlqTotal,
		ActiveWSConnections:          wsCount,
		UptimeSeconds:                int(time.Since(startTime).Seconds()),
	}
}

// eventsFromPerChannel collapses per-channel published-record counters into
// a count of logical events.
//
// Publish fans one event out to every channel, so each channel's counter
// already equals the event count — we take the max rather than sum÷3. Two
// reasons: dividing would hardcode the fan-out width into the metrics layer
// and break the moment fan-out becomes conditional, and the three counters
// are incremented from independent async ack callbacks, so at any instant
// they can differ by the number of in-flight publishes. Max of monotonic
// counters is itself monotonic, so the rate tracker never sees a negative
// delta.
func eventsFromPerChannel(byChannel map[string]float64) float64 {
	var maxSeen float64
	for _, v := range byChannel {
		if v > maxSeen {
			maxSeen = v
		}
	}
	return maxSeen
}

// labelValue returns the value of the named label, or "" if the metric does
// not carry it.
func labelValue(m *dto.Metric, name string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

// histP99MS computes the approximate P99 latency in milliseconds from
// aggregated histogram bucket data.
func histP99MS(buckets map[float64]uint64, totalCount uint64) float64 {
	sec := histP99Sec(buckets, totalCount)
	return math.Round(sec*1000*10) / 10
}

// histP99Sec computes the approximate P99 from bucket data in seconds
// (native histogram unit).
//
// The Prometheus text format includes an explicit +Inf bucket, so a p99 that
// overflows the largest finite bucket would otherwise be reported as +Inf —
// which json.Encoder refuses, failing the whole summary response rather than
// just that field. Only finite bounds are considered; if p99 overflows them
// all, the largest finite bound is returned, meaning "at least this".
func histP99Sec(buckets map[float64]uint64, totalCount uint64) float64 {
	if totalCount == 0 || len(buckets) == 0 {
		return 0
	}

	target := uint64(math.Ceil(float64(totalCount) * 0.99))

	bounds := make([]float64, 0, len(buckets))
	for b := range buckets {
		if math.IsInf(b, 0) || math.IsNaN(b) {
			continue
		}
		bounds = append(bounds, b)
	}
	if len(bounds) == 0 {
		return 0
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

	// p99 sits beyond every finite bucket.
	return bounds[len(bounds)-1]
}

// safeFloat keeps non-encodable values out of the response. A single NaN or
// Inf makes json.Encoder fail the entire body, so the endpoint would return
// 200 with nothing in it — losing every other field too.
func safeFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

func emptyQueueDepth() map[string]int {
	return map[string]int{
		"email_high": 0, "email_normal": 0, "email_low": 0,
		"inapp_high": 0, "inapp_normal": 0, "inapp_low": 0,
		"webhook_high": 0, "webhook_normal": 0, "webhook_low": 0,
	}
}
