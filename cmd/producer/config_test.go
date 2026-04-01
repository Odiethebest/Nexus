package main

import (
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
