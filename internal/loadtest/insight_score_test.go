package loadtest

import (
	"strings"
	"testing"
	"time"
)

func TestBuildSnapshot_InsightPriority(t *testing.T) {
	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		series CoreSeries
		want   string
	}{
		{
			name: "no throughput despite active VUs",
			series: CoreSeries{
				RPS: []MetricPoint{{Timestamp: now, Value: 0}},
				VUs: []MetricPoint{{Timestamp: now, Value: 50}},
			},
			want: "Traffic is not flowing despite active virtual users.",
		},
		{
			name: "error spike",
			series: CoreSeries{
				RPS:          []MetricPoint{{Timestamp: now, Value: 400}},
				P95MS:        []MetricPoint{{Timestamp: now, Value: 80}},
				ErrorRatePct: []MetricPoint{{Timestamp: now, Value: 3.1}},
				VUs:          []MetricPoint{{Timestamp: now, Value: 60}},
			},
			want: "Error rate spike detected.",
		},
		{
			name: "latency degraded",
			series: CoreSeries{
				RPS:          []MetricPoint{{Timestamp: now, Value: 350}},
				P95MS:        []MetricPoint{{Timestamp: now, Value: 360}},
				ErrorRatePct: []MetricPoint{{Timestamp: now, Value: 0.2}},
				VUs:          []MetricPoint{{Timestamp: now, Value: 70}},
			},
			want: "Latency has degraded under load.",
		},
		{
			name: "stable slo",
			series: CoreSeries{
				RPS:          []MetricPoint{{Timestamp: now, Value: 450}},
				P95MS:        []MetricPoint{{Timestamp: now, Value: 95}},
				ErrorRatePct: []MetricPoint{{Timestamp: now, Value: 0.2}},
				VUs:          []MetricPoint{{Timestamp: now, Value: 80}},
			},
			want: "Stable under target SLO.",
		},
		{
			name: "collecting signal",
			series: CoreSeries{
				RPS:          []MetricPoint{{Timestamp: now, Value: 120}},
				P95MS:        []MetricPoint{{Timestamp: now, Value: 220}},
				ErrorRatePct: []MetricPoint{{Timestamp: now, Value: 0.6}},
				VUs:          []MetricPoint{{Timestamp: now, Value: 25}},
			},
			want: "Collecting runtime signal.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildSnapshot(tt.series).Insight
			if got != tt.want {
				t.Fatalf("buildSnapshot insight = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestScoreRun_AbortedRunAddsFailureSignals(t *testing.T) {
	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	series := CoreSeries{
		RPS: []MetricPoint{
			{Timestamp: now, Value: 0},
			{Timestamp: now.Add(5 * time.Second), Value: 0},
		},
		P95MS: []MetricPoint{
			{Timestamp: now, Value: 320},
			{Timestamp: now.Add(5 * time.Second), Value: 410},
		},
		ErrorRatePct: []MetricPoint{
			{Timestamp: now, Value: 3.2},
			{Timestamp: now.Add(5 * time.Second), Value: 4.1},
		},
		VUs: []MetricPoint{
			{Timestamp: now, Value: 120},
			{Timestamp: now.Add(5 * time.Second), Value: 160},
		},
	}
	snapshot := buildSnapshot(series)

	score, signals := scoreRun(TestRun{
		Status: StatusAborted,
		Result: "error",
	}, series, snapshot)

	if score > 50 {
		t.Fatalf("expected heavily penalized score <= 50, got %d", score)
	}
	if !hasSignalContaining(signals, "active VUs but no throughput observed") {
		t.Fatalf("expected throughput failure signal, got %v", signals)
	}
	if !hasSignalContaining(signals, "run ended unsuccessfully") {
		t.Fatalf("expected unsuccessful run signal, got %v", signals)
	}
	if !hasSignalContaining(signals, "run reached terminal state: aborted") {
		t.Fatalf("expected terminal state signal, got %v", signals)
	}
}

func hasSignalContaining(signals []string, needle string) bool {
	for _, signal := range signals {
		if strings.Contains(signal, needle) {
			return true
		}
	}
	return false
}
