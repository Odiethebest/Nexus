package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"nexus/internal/loadtest"
)

// toMetricTuples returns nil, never an empty slice, so an empty series
// serialises as JSON null rather than []. These cases pin that distinction,
// which is why every emptiness assertion below is `got == nil` — `len(got) ==
// 0` holds for both nil and [][]any{} and would pin nothing.
func TestToMetricTuples(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		if got := toMetricTuples(nil); got != nil {
			t.Errorf("toMetricTuples(nil) = %#v, want nil", got)
		}
	})

	t.Run("empty slice returns nil not empty slice", func(t *testing.T) {
		if got := toMetricTuples([]loadtest.MetricPoint{}); got != nil {
			t.Errorf("toMetricTuples([]) = %#v, want nil", got)
		}
	})

	t.Run("points become unix-stamped tuples", func(t *testing.T) {
		at := time.Date(2026, 4, 5, 10, 0, 0, 0, time.UTC)
		got := toMetricTuples([]loadtest.MetricPoint{
			{Timestamp: at, Value: 1.5},
			{Timestamp: at.Add(time.Minute), Value: 0},
		})

		if got == nil {
			t.Fatal("got nil, want two tuples")
		}
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
		want := [][]any{
			{at.Unix(), 1.5},
			{at.Add(time.Minute).Unix(), float64(0)},
		}
		// reflect.DeepEqual, not a string comparison of the two printed
		// forms: these are [][]any, and printing collapses the dynamic type
		// of every cell. A value that turned from float64 into a string
		// would render identically and slip through, while the JSON body it
		// feeds would have changed.
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}
		// The timestamp must be seconds, not nanos or a formatted string.
		if got[0][0] != at.Unix() {
			t.Errorf("timestamp = %#v, want %d from .Unix()", got[0][0], at.Unix())
		}
	})
}

func TestStringPtrIfNonEmpty(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantNil bool
		want    string // checked only when wantNil is false
	}{
		{name: "empty is nil", in: "", wantNil: true},
		{name: "spaces are nil", in: "   ", wantNil: true},
		{name: "tab and newline are nil", in: "\t\n", wantNil: true},
		{name: "value passes through", in: "passed", wantNil: false, want: "passed"},
		// TrimSpace decides emptiness only; the string itself is not trimmed.
		{name: "padding is preserved not trimmed", in: "  x  ", wantNil: false, want: "  x  "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stringPtrIfNonEmpty(tt.in)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("got %q, want nil", *got)
				}
				return
			}
			if got == nil {
				t.Fatalf("got nil, want %q", tt.want)
			}
			if *got != tt.want {
				t.Errorf("got %q, want %q", *got, tt.want)
			}
		})
	}
}

func TestClassifyLoadtestStartOutcome(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "unauthorized", err: loadtest.ErrUnauthorized, want: "deny"},
		{name: "disabled", err: loadtest.ErrDisabled, want: "deny"},
		{name: "parallel limit", err: loadtest.ErrParallelLimit, want: "deny"},
		{name: "cooldown", err: loadtest.ErrCooldown, want: "deny"},
		{name: "throttled", err: loadtest.ErrThrottled, want: "deny"},
		{name: "budget exceeded", err: loadtest.ErrBudgetExceeded, want: "deny"},
		{name: "wrapped sentinel still denies", err: fmt.Errorf("start: %w", loadtest.ErrCooldown), want: "deny"},
		// ErrCircuitOpen has its own branch in mapLoadtestError but falls to
		// default here. The asymmetry is deliberate: an open circuit is an
		// upstream fault, not a policy refusal. Pinned so it is not "tidied"
		// into the deny list, which would move it between Prometheus labels.
		{name: "circuit open is an error not a deny", err: loadtest.ErrCircuitOpen, want: "error"},
		{name: "unknown error", err: errors.New("boom"), want: "error"},
		{name: "nil error", err: nil, want: "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyLoadtestStartOutcome(tt.err); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizeLoadtestMode(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "demo", in: "demo", want: loadtestModeDemo},
		{name: "uppercase demo", in: "DEMO", want: loadtestModeDemo},
		{name: "padded demo", in: " demo ", want: loadtestModeDemo},
		{name: "mixed case demo", in: "DeMo", want: loadtestModeDemo},
		{name: "real", in: "real", want: loadtestModeReal},
		{name: "padded uppercase real", in: " REAL ", want: loadtestModeReal},
		// Anything unrecognised falls to real, never demo: an unreadable mode
		// must not silently downgrade a run to the fake one.
		{name: "empty falls to real", in: "", want: loadtestModeReal},
		{name: "whitespace falls to real", in: "   ", want: loadtestModeReal},
		{name: "garbage falls to real", in: "garbage", want: loadtestModeReal},
		{name: "near-miss falls to real", in: "demoo", want: loadtestModeReal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeLoadtestMode(tt.in); got != tt.want {
				t.Errorf("normalizeLoadtestMode(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestActorFromRequest(t *testing.T) {
	// The header name below is "X-Real-Ip", matching the source; Go canonicalises
	// header keys, so the casing here is cosmetic but kept in step with it.
	tests := []struct {
		name       string
		xff        string
		xRealIP    string
		remoteAddr string
		want       string
	}{
		{
			name: "forwarded-for wins over everything",
			xff:  "203.0.113.7", xRealIP: "198.51.100.9", remoteAddr: "192.0.2.1:5555",
			want: "203.0.113.7",
		},
		{
			name: "forwarded-for takes the first hop only",
			xff:  "203.0.113.7, 198.51.100.9, 192.0.2.1", remoteAddr: "192.0.2.1:5555",
			want: "203.0.113.7",
		},
		{
			name: "forwarded-for first hop is trimmed",
			xff:  "  203.0.113.7  , 198.51.100.9", remoteAddr: "192.0.2.1:5555",
			want: "203.0.113.7",
		},
		{
			name: "blank forwarded-for falls through to real-ip",
			xff:  "   ", xRealIP: "198.51.100.9", remoteAddr: "192.0.2.1:5555",
			want: "198.51.100.9",
		},
		{
			name: "real-ip wins over remote addr", xRealIP: "198.51.100.9", remoteAddr: "192.0.2.1:5555",
			want: "198.51.100.9",
		},
		{
			name: "remote addr is stripped of its port", remoteAddr: "192.0.2.1:5555",
			want: "192.0.2.1",
		},
		{
			name: "ipv6 remote addr is stripped of its port", remoteAddr: "[2001:db8::1]:5555",
			want: "2001:db8::1",
		},
		// SplitHostPort fails without a port, and the fallback returns the
		// address as-is rather than an empty string.
		{
			name: "portless remote addr passes through", remoteAddr: "192.0.2.1",
			want: "192.0.2.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/ops/loadtest/start", nil)
			req.RemoteAddr = tt.remoteAddr
			if tt.xff != "" {
				req.Header.Set("X-Forwarded-For", tt.xff)
			}
			if tt.xRealIP != "" {
				req.Header.Set("X-Real-Ip", tt.xRealIP)
			}

			if got := actorFromRequest(req); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
