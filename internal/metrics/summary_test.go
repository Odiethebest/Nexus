package metrics

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func TestMergeMetricFamiliesLocalWins(t *testing.T) {
	remote := map[string]*dto.MetricFamily{
		"nexus_messages_processed_total": {},
		"nexus_consumer_lag_records":     {}, // stale remote copy
	}
	local := map[string]*dto.MetricFamily{
		"nexus_consumer_lag_records": {}, // authoritative
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
