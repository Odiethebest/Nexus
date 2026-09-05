package kbroker

import "testing"

// Small pure-function tests. End-to-end publish is exercised in
// internal/integration/pipeline_test.go once Step 7 lands the Redpanda
// testcontainer setup.

func TestNormalizePriority(t *testing.T) {
	cases := map[string]Priority{
		"high":    PriorityHigh,
		"HIGH":    PriorityHigh,
		"normal":  PriorityNormal,
		"":        PriorityNormal,
		"garbage": PriorityNormal,
		"low":     PriorityLow,
		"  low  ": PriorityLow,
	}
	for in, want := range cases {
		if got := normalizePriority(in); got != want {
			t.Errorf("normalizePriority(%q) = %s want %s", in, got, want)
		}
	}
}
