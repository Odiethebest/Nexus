package httpapi

import (
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"nexus/internal/loadtest"
)

// The two loadtest run modes. normalizeLoadtestMode maps any unrecognised
// value to real.
const (
	loadtestModeDemo = "demo"
	loadtestModeReal = "real"
)

type loadtestStartRequest struct {
	Mode     string `json:"mode,omitempty"`
	Scenario string `json:"scenario,omitempty"`
	Preset   string `json:"preset,omitempty"`
	Note     string `json:"note,omitempty"`
}

type loadtestRunEnvelope struct {
	Mode        string             `json:"mode,omitempty"`
	Run         loadtestRunSummary `json:"run"`
	Series      loadtestSeriesJSON `json:"series"`
	Snapshot    any                `json:"snapshot"`
	HealthScore int                `json:"health_score"`
	Signals     []string           `json:"signals"`
	Warnings    []string           `json:"warnings,omitempty"`
}

type loadtestRunSummary struct {
	ID      int64              `json:"id"`
	Status  loadtest.RunStatus `json:"status"`
	Result  *string            `json:"result"`
	Created time.Time          `json:"created"`
	Ended   *time.Time         `json:"ended"`
}

type loadtestSeriesJSON struct {
	RPS          [][]any `json:"rps"`
	P95MS        [][]any `json:"p95_ms"`
	ErrorRatePct [][]any `json:"error_rate_pct"`
	VUs          [][]any `json:"vus"`
}

func toLoadtestRunEnvelope(in loadtest.RunInsight, mode string) loadtestRunEnvelope {
	return loadtestRunEnvelope{
		Mode: mode,
		Run: loadtestRunSummary{
			ID:      in.Run.ID,
			Status:  in.Run.Status,
			Result:  stringPtrIfNonEmpty(in.Run.Result),
			Created: in.Run.Created,
			Ended:   in.Run.Ended,
		},
		Series: loadtestSeriesJSON{
			RPS:          toMetricTuples(in.Series.RPS),
			P95MS:        toMetricTuples(in.Series.P95MS),
			ErrorRatePct: toMetricTuples(in.Series.ErrorRatePct),
			VUs:          toMetricTuples(in.Series.VUs),
		},
		Snapshot:    in.Snapshot,
		HealthScore: in.HealthScore,
		Signals:     in.Signals,
		Warnings:    in.Warnings,
	}
}

func toMetricTuples(points []loadtest.MetricPoint) [][]any {
	if len(points) == 0 {
		return nil
	}
	out := make([][]any, 0, len(points))
	for _, point := range points {
		out = append(out, []any{
			point.Timestamp.Unix(),
			point.Value,
		})
	}
	return out
}

func stringPtrIfNonEmpty(s string) *string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return &s
}

func classifyLoadtestStartOutcome(err error) string {
	switch {
	case errors.Is(err, loadtest.ErrUnauthorized),
		errors.Is(err, loadtest.ErrDisabled),
		errors.Is(err, loadtest.ErrParallelLimit),
		errors.Is(err, loadtest.ErrCooldown),
		errors.Is(err, loadtest.ErrThrottled),
		errors.Is(err, loadtest.ErrBudgetExceeded):
		return "deny"
	default:
		return "error"
	}
}

func normalizeLoadtestMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case loadtestModeDemo:
		return loadtestModeDemo
	default:
		return loadtestModeReal
	}
}

func actorFromRequest(r *http.Request) string {
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		if idx := strings.Index(xff, ","); idx >= 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return xff
	}

	if xRealIP := strings.TrimSpace(r.Header.Get("X-Real-Ip")); xRealIP != "" {
		return xRealIP
	}

	remote := strings.TrimSpace(r.RemoteAddr)
	if host, _, err := net.SplitHostPort(remote); err == nil && host != "" {
		return host
	}
	return remote
}
