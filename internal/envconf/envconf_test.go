package envconf

import (
	"math"
	"testing"
)

const key = "NEXUS_ENVCONF_TEST"

func TestString(t *testing.T) {
	tests := []struct {
		name     string
		set      bool
		value    string
		fallback string
		want     string
	}{
		{name: "unset falls back", set: false, fallback: "default", want: "default"},
		{name: "empty falls back", set: true, value: "", fallback: "default", want: "default"},
		{name: "set wins", set: true, value: "postgres://x", fallback: "default", want: "postgres://x"},
		// String deliberately does not trim: a lone space is a value, not an
		// absence. Trimming here would silently swap a blank DSN for the default.
		{name: "single space is a value", set: true, value: " ", fallback: "default", want: " "},
		{name: "surrounding space preserved", set: true, value: "  x  ", fallback: "default", want: "  x  "},
		{name: "empty fallback allowed", set: false, fallback: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv(key, tt.value)
			}
			if got := String(key, tt.fallback); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBool(t *testing.T) {
	tests := []struct {
		name     string
		set      bool
		value    string
		fallback bool
		want     bool
	}{
		{name: "unset falls back true", set: false, fallback: true, want: true},
		{name: "unset falls back false", set: false, fallback: false, want: false},
		{name: "empty falls back", set: true, value: "", fallback: true, want: true},
		{name: "blank falls back", set: true, value: "   ", fallback: true, want: true},
		{name: "one", set: true, value: "1", fallback: false, want: true},
		{name: "zero", set: true, value: "0", fallback: true, want: false},
		{name: "true", set: true, value: "true", fallback: false, want: true},
		{name: "TRUE", set: true, value: "TRUE", fallback: false, want: true},
		{name: "False", set: true, value: "False", fallback: true, want: false},
		{name: "padded true", set: true, value: "  true  ", fallback: false, want: true},
		{name: "garbage falls back", set: true, value: "yes", fallback: true, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv(key, tt.value)
			}
			if got := Bool(key, tt.fallback); got != tt.want {
				t.Errorf("Bool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInt(t *testing.T) {
	tests := []struct {
		name     string
		set      bool
		value    string
		fallback int
		want     int
	}{
		{name: "unset falls back", set: false, fallback: 55, want: 55},
		{name: "empty falls back", set: true, value: "", fallback: 55, want: 55},
		{name: "blank falls back", set: true, value: "  ", fallback: 55, want: 55},
		{name: "parsed", set: true, value: "12", fallback: 55, want: 12},
		{name: "padded parsed", set: true, value: " 12 ", fallback: 55, want: 12},
		{name: "negative", set: true, value: "-3", fallback: 55, want: -3},
		{name: "zero is not fallback", set: true, value: "0", fallback: 55, want: 0},
		{name: "float falls back", set: true, value: "1.5", fallback: 55, want: 55},
		{name: "overflow falls back", set: true, value: "9223372036854775808", fallback: 55, want: 55},
		{name: "garbage falls back", set: true, value: "abc", fallback: 55, want: 55},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv(key, tt.value)
			}
			if got := Int(key, tt.fallback); got != tt.want {
				t.Errorf("Int() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestInt64(t *testing.T) {
	tests := []struct {
		name     string
		set      bool
		value    string
		fallback int64
		want     int64
	}{
		{name: "unset falls back", set: false, fallback: 7, want: 7},
		{name: "empty falls back", set: true, value: "", fallback: 7, want: 7},
		{name: "parsed", set: true, value: "4815162342", fallback: 7, want: 4815162342},
		{name: "padded parsed", set: true, value: "\t42\n", fallback: 7, want: 42},
		{name: "negative", set: true, value: "-42", fallback: 7, want: -42},
		{name: "zero is not fallback", set: true, value: "0", fallback: 7, want: 0},
		{name: "overflow falls back", set: true, value: "9223372036854775808", fallback: 7, want: 7},
		{name: "garbage falls back", set: true, value: "12x", fallback: 7, want: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv(key, tt.value)
			}
			if got := Int64(key, tt.fallback); got != tt.want {
				t.Errorf("Int64() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestFloat64(t *testing.T) {
	tests := []struct {
		name     string
		set      bool
		value    string
		fallback float64
		want     float64
		wantNaN  bool
	}{
		{name: "unset falls back", set: false, fallback: 1.5, want: 1.5},
		{name: "empty falls back", set: true, value: "", fallback: 1.5, want: 1.5},
		{name: "parsed", set: true, value: "2.25", fallback: 1.5, want: 2.25},
		{name: "padded parsed", set: true, value: " 2.25 ", fallback: 1.5, want: 2.25},
		{name: "integer literal", set: true, value: "3", fallback: 1.5, want: 3},
		{name: "zero is not fallback", set: true, value: "0", fallback: 1.5, want: 0},
		{name: "negative", set: true, value: "-0.5", fallback: 1.5, want: -0.5},
		{name: "garbage falls back", set: true, value: "1.5.5", fallback: 1.5, want: 1.5},
		// strconv.ParseFloat accepts NaN and Inf without error, so all four of
		// these come back verbatim rather than as the fallback. That is what
		// the cases below pin, at this layer. What a caller then does with a
		// non-finite value is a separate question — and the four do not all
		// behave the same there.
		//
		// Three of them are dangerous to a caller that compares against them.
		// As LOADTEST_BUDGET_VUH_PER_DAY, NaN / +Inf / "infinity" each leave
		// loadtest.Service's budget check inert: DailyVUHCap <= 0 says no, and
		// used >= cap also says no — every comparison against NaN is false, and
		// nothing is ever >= +Inf. The budget stops applying without saying so.
		//
		// -Inf is not one of them. -Inf <= 0 is true, so it lands in the "no
		// budget configured" branch, which is the documented way to switch the
		// budget off. It is pinned here for the same reason as the others —
		// Float64 must keep returning it verbatim — not because it is unsafe.
		//
		// initLoadtestService rejects every non-finite value that could reach
		// that check. That guard sits at the config boundary on purpose;
		// this package still hands all four values through unchanged.
		{name: "NaN returned verbatim", set: true, value: "NaN", fallback: 1.5, wantNaN: true},
		{name: "+Inf returned verbatim", set: true, value: "+Inf", fallback: 1.5, want: math.Inf(1)},
		{name: "infinity returned verbatim", set: true, value: "infinity", fallback: 1.5, want: math.Inf(1)},
		{name: "-Inf returned verbatim", set: true, value: "-Inf", fallback: 1.5, want: math.Inf(-1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv(key, tt.value)
			}
			got := Float64(key, tt.fallback)
			if tt.wantNaN {
				// NaN != NaN, so this case cannot go through the == below.
				if !math.IsNaN(got) {
					t.Errorf("Float64() = %v, want NaN", got)
				}
				return
			}
			if got != tt.want {
				t.Errorf("Float64() = %v, want %v", got, tt.want)
			}
		})
	}
}
