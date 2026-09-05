package main

import (
	"strings"
	"testing"
	"time"
)

func TestSanitizeLoadtestRequestTimeout(t *testing.T) {
	tests := []struct {
		name      string
		requested time.Duration
		want      time.Duration
	}{
		{
			name:      "default when zero",
			requested: 0,
			want:      defaultLoadtestRequestTimeout,
		},
		{
			name:      "default when negative",
			requested: -3 * time.Second,
			want:      defaultLoadtestRequestTimeout,
		},
		{
			name:      "minimum clamp",
			requested: 2 * time.Second,
			want:      minLoadtestRequestTimeout,
		},
		{
			name:      "pass through in range",
			requested: 20 * time.Second,
			want:      20 * time.Second,
		},
		{
			name:      "maximum clamp",
			requested: 90 * time.Second,
			want:      maxLoadtestRequestTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeLoadtestRequestTimeout(tt.requested)
			if got != tt.want {
				t.Fatalf("sanitizeLoadtestRequestTimeout(%s) = %s, want %s", tt.requested, got, tt.want)
			}
		})
	}
}

// setLoadtestEnv fills in every variable initLoadtestService needs to get past
// its earlier gates, so each case below fails or succeeds on the budget value
// alone rather than on a missing prerequisite.
func setLoadtestEnv(t *testing.T, enabled, budget string) {
	t.Helper()
	t.Setenv("LOADTEST_ENABLED", enabled)
	t.Setenv("LOADTEST_ADMIN_KEY", "test-admin-key")
	t.Setenv("K6_API_TOKEN", "test-token")
	t.Setenv("K6_STACK_ID", "test-stack")
	t.Setenv("K6_LOAD_TEST_ID", "1")
	t.Setenv("LOADTEST_BUDGET_VUH_PER_DAY", budget)
}

// A non-finite LOADTEST_BUDGET_VUH_PER_DAY used to disable the budget check
// silently: every comparison against NaN is false, so loadtest.Service neither
// treated the cap as unset nor ever found it exceeded. It is now refused at
// startup instead.
func TestInitLoadtestServiceRejectsNonFiniteBudget(t *testing.T) {
	tests := []struct {
		name   string
		budget string
	}{
		{name: "NaN", budget: "NaN"},
		{name: "positive infinity", budget: "+Inf"},
		{name: "negative infinity", budget: "-Inf"},
		{name: "spelled-out infinity", budget: "infinity"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setLoadtestEnv(t, "true", tt.budget)

			svc, err := initLoadtestService()
			if err == nil {
				t.Fatalf("expected an error for budget %q, got service %v", tt.budget, svc)
			}
			if !strings.Contains(err.Error(), "LOADTEST_BUDGET_VUH_PER_DAY") {
				t.Errorf("error = %v, want it to name LOADTEST_BUDGET_VUH_PER_DAY", err)
			}
		})
	}
}

func TestInitLoadtestServiceAcceptsFiniteBudget(t *testing.T) {
	tests := []struct {
		name   string
		budget string
	}{
		{name: "unset falls back to zero", budget: ""},
		{name: "zero means no budget", budget: "0"},
		{name: "positive cap", budget: "50"},
		{name: "fractional cap", budget: "12.5"},
		// Negative is the documented "no budget" setting, not a malformed
		// value: loadtest.Service treats anything <= 0 as unconfigured.
		{name: "negative means no budget", budget: "-1"},
		// Unparseable input never reaches the check — envconf.Float64 has
		// already substituted the fallback.
		{name: "garbage falls back to zero", budget: "not-a-number"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setLoadtestEnv(t, "true", tt.budget)

			svc, err := initLoadtestService()
			if err != nil {
				t.Fatalf("unexpected error for budget %q: %v", tt.budget, err)
			}
			if svc == nil {
				t.Fatal("expected a service, got nil")
			}
		})
	}
}

// The check sits after the !enabled early return: a deployment with loadtest
// switched off must not fail to boot over a variable it never reads.
func TestInitLoadtestServiceIgnoresBudgetWhenDisabled(t *testing.T) {
	for _, budget := range []string{"NaN", "+Inf", "-Inf"} {
		t.Run("budget="+budget, func(t *testing.T) {
			setLoadtestEnv(t, "false", budget)

			svc, err := initLoadtestService()
			if err != nil {
				t.Fatalf("disabled loadtest must not fail on budget %q, got %v", budget, err)
			}
			if svc == nil {
				t.Fatal("expected a disabled service, got nil")
			}
		})
	}
}
