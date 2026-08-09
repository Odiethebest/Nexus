package metrics

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func counterMetric(value float64, labels map[string]string) *dto.Metric {
	m := &dto.Metric{Counter: &dto.Counter{Value: &value}}
	for name, val := range labels {
		m.Label = append(m.Label, &dto.LabelPair{Name: strPtr(name), Value: strPtr(val)})
	}
	return m
}

func strPtr(s string) *string { return &s }

func TestLabelValue(t *testing.T) {
	m := counterMetric(1, map[string]string{"channel": "email", "status": "delivered"})
	if got := labelValue(m, "channel"); got != "email" {
		t.Errorf("channel = %q, want email", got)
	}
	if got := labelValue(m, "missing"); got != "" {
		t.Errorf("absent label = %q, want empty", got)
	}
}

// The per-channel series has to come from a real counter delta, not from
// splitting one aggregate number by fixed ratios (which is what the
// dashboard chart used to do client-side).
func TestRateTrackerSetComputesPerKeyRates(t *testing.T) {
	s := &rateTrackerSet{}

	// First observation seeds each tracker and reports no rate yet.
	first := s.update(map[string]float64{"email": 100, "inapp": 100, "webhook": 100})
	for ch, rate := range first {
		if rate != 0 {
			t.Errorf("%s: first sample rate = %v, want 0 (baseline only)", ch, rate)
		}
	}

	// Backdate the baselines by 10s so the next update has a usable window.
	for _, t := range s.trackers {
		t.prevTime = t.prevTime.Add(-10 * time.Second)
	}

	// Each channel advanced by a different amount — the whole point.
	got := s.update(map[string]float64{"email": 150, "inapp": 300, "webhook": 110})
	want := map[string]float64{"email": 5, "inapp": 20, "webhook": 1}
	for ch, w := range want {
		if got[ch] != w {
			t.Errorf("%s rate = %v, want %v", ch, got[ch], w)
		}
	}
}

func TestRateTrackerSetIgnoresCounterReset(t *testing.T) {
	// A worker restart drops the counter to 0; reporting a negative rate
	// would render as a downward spike in the chart.
	s := &rateTrackerSet{}
	s.update(map[string]float64{"email": 500})
	s.trackers["email"].prevTime = s.trackers["email"].prevTime.Add(-10 * time.Second)
	s.update(map[string]float64{"email": 1000}) // rate 50/s

	s.trackers["email"].prevTime = s.trackers["email"].prevTime.Add(-10 * time.Second)
	got := s.update(map[string]float64{"email": 0}) // restart
	if got["email"] < 0 {
		t.Errorf("rate after counter reset = %v, want the previous rate held, never negative", got["email"])
	}
}

func TestRateTrackerSetKeepsBaselineForQuietKey(t *testing.T) {
	s := &rateTrackerSet{}
	s.update(map[string]float64{"email": 10, "webhook": 10})

	// webhook stops reporting for a cycle, then comes back at the same
	// cumulative value: its rate must be 0, not a spike from a lost baseline.
	s.update(map[string]float64{"email": 10})
	if _, ok := s.trackers["webhook"]; !ok {
		t.Fatal("tracker for a temporarily absent key was dropped")
	}
	s.trackers["webhook"].prevTime = s.trackers["webhook"].prevTime.Add(-10 * time.Second)
	got := s.update(map[string]float64{"email": 10, "webhook": 10})
	if got["webhook"] != 0 {
		t.Errorf("webhook rate = %v, want 0 after returning at an unchanged counter", got["webhook"])
	}
}

// publish_rate_per_sec counts events, not the records fan-out produces.
func TestEventsFromPerChannelCollapsesFanOut(t *testing.T) {
	// Steady state: one event produced one record into each channel.
	if got := eventsFromPerChannel(map[string]float64{"email": 900, "inapp": 900, "webhook": 900}); got != 900 {
		t.Errorf("balanced fan-out = %v, want 900 events (not 2700 records)", got)
	}

	// Mid-publish skew: the three ack callbacks fire independently, so the
	// counters lag each other. Max tracks events accepted; sum÷3 would
	// under-report.
	if got := eventsFromPerChannel(map[string]float64{"email": 902, "inapp": 900, "webhook": 901}); got != 902 {
		t.Errorf("skewed counters = %v, want 902", got)
	}

	if got := eventsFromPerChannel(nil); got != 0 {
		t.Errorf("no data = %v, want 0", got)
	}
}

// The response must always carry all three channels so the chart does not
// have to guess at missing series.
func TestComputeSummaryAlwaysReportsEveryChannel(t *testing.T) {
	t.Setenv("METRICS_INTERNAL_URL", "http://127.0.0.1:1/metrics") // unreachable on purpose

	got := ComputeSummary(0)
	for _, ch := range channels {
		if _, ok := got.ProcessedRatePerSecByChannel[ch]; !ok {
			t.Errorf("channel %q missing from processed_rate_per_sec_by_channel: %v",
				ch, got.ProcessedRatePerSecByChannel)
		}
	}
}

func TestMergeMetricFamiliesLocalWins(t *testing.T) {
	remote := map[string]*dto.MetricFamily{
		"nexus_messages_processed_total": {},
		"nexus_consumer_lag_records":     {}, // stale remote copy
	}
	local := map[string]*dto.MetricFamily{
		"nexus_consumer_lag_records":   {}, // authoritative
		"nexus_events_published_total": {},
	}
	out := mergeMetricFamilies(remote, local)
	if _, ok := out["nexus_messages_processed_total"]; !ok {
		t.Error("remote-only key dropped")
	}
	if _, ok := out["nexus_events_published_total"]; !ok {
		t.Error("local-only key dropped")
	}
	if out["nexus_consumer_lag_records"] != local["nexus_consumer_lag_records"] {
		t.Error("local did not win on collision")
	}
}

func TestHistP99SecInterpolates(t *testing.T) {
	// Simulate a histogram with cumulative counts:
	// bucket ≤0.5s : 90, ≤1s : 99, ≤2s : 100
	buckets := map[float64]uint64{0.5: 90, 1.0: 99, 2.0: 100}
	got := histP99Sec(buckets, 100)
	// p99 target = 99 → falls on the 1.0 bucket boundary. Should be <= 1s.
	if got > 1.0 || got < 0.5 {
		t.Errorf("p99 = %v, want 0.5 ≤ p99 ≤ 1.0", got)
	}
}

func TestHistP99SecReturnsZeroWhenEmpty(t *testing.T) {
	if got := histP99Sec(nil, 0); got != 0 {
		t.Errorf("empty p99 = %v, want 0", got)
	}
}

// Regression: the Prometheus text exposition carries an explicit +Inf
// bucket. A p99 that overflowed every finite bucket used to be reported as
// +Inf, which json.Encoder rejects — blanking the whole summary response,
// not just that one field.
func TestHistP99SecNeverReturnsInfinity(t *testing.T) {
	// 5 of 100 observations exceeded the largest finite bound, so p99 lands
	// in the +Inf bucket.
	buckets := map[float64]uint64{
		1.0:         50,
		5.0:         95,
		math.Inf(1): 100,
	}
	got := histP99Sec(buckets, 100)
	if math.IsInf(got, 0) || math.IsNaN(got) {
		t.Fatalf("p99 = %v, want a finite value", got)
	}
	if got != 5.0 {
		t.Errorf("p99 = %v, want 5 (the largest finite bound: 'at least this')", got)
	}
}

func TestHistP99SecHandlesInfOnlyBuckets(t *testing.T) {
	if got := histP99Sec(map[float64]uint64{math.Inf(1): 10}, 10); got != 0 {
		t.Errorf("p99 = %v, want 0 when no finite bound exists", got)
	}
}

// Whatever the inputs, the response has to survive json.Marshal — one bad
// float costs every other field in the body.
func TestSummarySnapshotIsAlwaysJSONEncodable(t *testing.T) {
	for _, v := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
		if got := safeFloat(v); math.IsInf(got, 0) || math.IsNaN(got) {
			t.Errorf("safeFloat(%v) = %v, still not encodable", v, got)
		}
	}

	t.Setenv("METRICS_INTERNAL_URL", "http://127.0.0.1:1/metrics")
	if _, err := json.Marshal(ComputeSummary(0)); err != nil {
		t.Fatalf("summary is not JSON-encodable: %v", err)
	}
}
