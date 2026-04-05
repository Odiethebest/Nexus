package metrics

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// SummarySnapshot is the response body for GET /api/metrics/summary.
type SummarySnapshot struct {
	PublishRatePerSec      float64        `json:"publish_rate_per_sec"`
	ProcessingLatencyP99MS float64        `json:"processing_latency_p99_ms"`
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

var publishRate = &rateTracker{}

// ComputeSummary gathers current Prometheus metric values and returns a SummarySnapshot.
// wsCount is the number of currently connected WebSocket clients.
func ComputeSummary(wsCount int) SummarySnapshot {
	mfs, _ := prometheus.DefaultGatherer.Gather()

	var (
		publishedTotal float64
		deliveredTotal float64
		processedTotal float64
	)

	// bucket accumulation for histogram p99
	bucketCounts := make(map[float64]uint64)
	var histTotalCount uint64

	for _, mf := range mfs {
		switch mf.GetName() {
		case "nexus_messages_published_total":
			for _, m := range mf.GetMetric() {
				publishedTotal += m.GetCounter().GetValue()
			}

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

		case "nexus_worker_process_duration_seconds":
			for _, m := range mf.GetMetric() {
				h := m.GetHistogram()
				if h == nil {
					continue
				}
				histTotalCount += h.GetSampleCount()
				for _, b := range h.GetBucket() {
					bucketCounts[b.GetUpperBound()] += b.GetCumulativeCount()
				}
			}
		}
	}

	var successRate float64
	if processedTotal > 0 {
		successRate = math.Round(deliveredTotal/processedTotal*10000) / 10000
	}

	return SummarySnapshot{
		PublishRatePerSec:      math.Round(publishRate.update(publishedTotal)*10) / 10,
		ProcessingLatencyP99MS: histP99MS(bucketCounts, histTotalCount),
		QueueDepth:             emptyQueueDepth(),
		DeliverySuccessRate:    successRate,
		DLQCount:               0,
		ActiveWSConnections:    wsCount,
		UptimeSeconds:          int(time.Since(startTime).Seconds()),
	}
}

// histP99MS computes the approximate P99 latency in milliseconds from
// aggregated histogram bucket data.
func histP99MS(buckets map[float64]uint64, totalCount uint64) float64 {
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
				return math.Round(ub*100000) / 100 // seconds → ms, 3 decimal places
			}
			frac := float64(target-prevCount) / float64(count-prevCount)
			ms := (prevBound + frac*(ub-prevBound)) * 1000
			return math.Round(ms*10) / 10
		}
		prevBound = ub
		prevCount = count
	}

	// p99 falls in the +Inf bucket — return max explicit upper bound in ms
	if len(bounds) > 0 {
		return math.Round(bounds[len(bounds)-1]*1000*10) / 10
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
