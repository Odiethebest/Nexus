package loadtest

import (
	"context"
	"fmt"
	"math"
	"nexus/internal/metrics"
	"strings"
	"sync"
	"time"
)

// K6API defines the upstream methods used by Service.
type K6API interface {
	StartLoadTest(ctx context.Context, testID int64, req StartOptions) (TestRun, error)
	GetTestRun(ctx context.Context, runID int64) (TestRun, error)
	QueryRangeK6(ctx context.Context, runID int64, metric, query string, stepSeconds int) ([]MetricPoint, error)
}

// ServiceConfig controls loadtest orchestration behavior.
type ServiceConfig struct {
	Enabled      bool
	LoadTestID   int64
	PollInterval time.Duration
	DailyVUHCap  float64
	Now          func() time.Time
}

// Service orchestrates start/poll flows and computes UI-facing insights.
type Service struct {
	cfg    ServiceConfig
	client K6API
	guard  *Guard
	now    func() time.Time

	mu           sync.Mutex
	dailyVUHUsed map[string]float64
	accountedRun map[int64]struct{}
}

// NewService creates a loadtest service with defaults.
func NewService(cfg ServiceConfig, client K6API, guard *Guard) *Service {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 3 * time.Second
	}
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	if guard == nil {
		guard = NewGuard(GuardConfig{})
	}
	return &Service{
		cfg:          cfg,
		client:       client,
		guard:        guard,
		now:          nowFn,
		dailyVUHUsed: make(map[string]float64),
		accountedRun: make(map[int64]struct{}),
	}
}

// Start validates policy, starts a k6 run, and returns UI-friendly metadata.
func (s *Service) Start(
	ctx context.Context,
	adminKey string,
	actor string,
	opts StartOptions,
) (StartResult, error) {
	if !s.cfg.Enabled {
		return StartResult{}, ErrDisabled
	}
	if s.client == nil {
		return StartResult{}, fmt.Errorf("loadtest: client is not configured")
	}
	if s.cfg.LoadTestID <= 0 {
		return StartResult{}, fmt.Errorf("loadtest: invalid load test id")
	}
	if err := s.guard.Authorize(adminKey); err != nil {
		return StartResult{}, err
	}

	now := s.now().UTC()
	if err := s.checkBudget(now); err != nil {
		return StartResult{}, err
	}

	reservation, err := s.guard.ReserveStart(actor, now)
	if err != nil {
		return StartResult{}, err
	}

	run, err := s.client.StartLoadTest(ctx, s.cfg.LoadTestID, opts)
	if err != nil {
		reservation.Cancel()
		return StartResult{}, err
	}

	startedAt := run.Created
	if startedAt.IsZero() {
		startedAt = now
	}
	reservation.Commit(run.ID, startedAt)
	metrics.LoadtestActiveRuns.Set(float64(s.guard.ActiveCount()))

	return StartResult{
		RunID:     run.ID,
		TestID:    run.TestID,
		Status:    run.Status,
		StartedAt: startedAt,
		PollAfter: s.cfg.PollInterval,
	}, nil
}

// SyncRun pulls run state + core metrics and computes insight fields.
func (s *Service) SyncRun(ctx context.Context, runID int64) (RunInsight, error) {
	if !s.cfg.Enabled {
		return RunInsight{}, ErrDisabled
	}
	if s.client == nil {
		return RunInsight{}, fmt.Errorf("loadtest: client is not configured")
	}
	run, err := s.client.GetTestRun(ctx, runID)
	if err != nil {
		return RunInsight{}, err
	}
	if run.ID == 0 {
		run.ID = runID
	}

	series, warnings := s.fetchCoreSeries(ctx, run.ID)
	snapshot := buildSnapshot(series)
	score, signals := scoreRun(run, series, snapshot)
	metrics.LoadtestHealthScore.Set(float64(score))
	if len(warnings) > 0 {
		signals = append(signals, "Some metric queries are temporarily unavailable")
	}

	if run.Status.IsTerminal() {
		s.guard.MarkFinished(run.ID, s.now().UTC())
		metrics.LoadtestActiveRuns.Set(float64(s.guard.ActiveCount()))
		s.accountRunCost(run)
	}

	return RunInsight{
		Run:         run,
		Series:      series,
		Snapshot:    snapshot,
		HealthScore: score,
		Signals:     dedupeStrings(signals),
		Warnings:    warnings,
	}, nil
}

// DailyVUHUsed returns tracked usage for the given day.
func (s *Service) DailyVUHUsed(day time.Time) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dailyVUHUsed[dayKey(day)]
}

func (s *Service) checkBudget(now time.Time) error {
	if s.cfg.DailyVUHCap <= 0 {
		return nil
	}
	used := s.DailyVUHUsed(now)
	if used >= s.cfg.DailyVUHCap {
		return ErrBudgetExceeded
	}
	return nil
}

func (s *Service) accountRunCost(run TestRun) {
	if run.Cost == nil || run.Cost.TotalVUH <= 0 || run.ID <= 0 {
		return
	}

	day := s.now().UTC()
	if run.Ended != nil && !run.Ended.IsZero() {
		day = run.Ended.UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.accountedRun[run.ID]; ok {
		return
	}
	s.accountedRun[run.ID] = struct{}{}
	s.dailyVUHUsed[dayKey(day)] += run.Cost.TotalVUH
}

func (s *Service) fetchCoreSeries(ctx context.Context, runID int64) (CoreSeries, []string) {
	stepSeconds := int(s.cfg.PollInterval.Seconds())
	if stepSeconds <= 0 {
		stepSeconds = 3
	}

	var out CoreSeries
	var warnings []string

	if points, err := s.client.QueryRangeK6(ctx, runID, "http_reqs", "rate", stepSeconds); err == nil {
		out.RPS = points
	} else {
		warnings = append(warnings, "rps: "+trimErr(err))
	}

	if points, err := s.client.QueryRangeK6(ctx, runID, "http_req_duration", "histogram_quantile(0.95)", stepSeconds); err == nil {
		out.P95MS = points
	} else {
		warnings = append(warnings, "p95: "+trimErr(err))
	}

	if points, err := s.client.QueryRangeK6(ctx, runID, "http_req_failed", "rate", stepSeconds); err == nil {
		out.ErrorRatePct = multiplySeries(points, 100)
	} else {
		warnings = append(warnings, "error_rate: "+trimErr(err))
	}

	if points, err := s.client.QueryRangeK6(ctx, runID, "vus", "sum(last by (instance_id))", stepSeconds); err == nil {
		out.VUs = points
	} else {
		warnings = append(warnings, "vus: "+trimErr(err))
	}

	return out, warnings
}

func buildSnapshot(series CoreSeries) InsightSnapshot {
	s := InsightSnapshot{
		RPS:          lastValue(series.RPS),
		P95MS:        lastValue(series.P95MS),
		ErrorRatePct: lastValue(series.ErrorRatePct),
		VUs:          lastValue(series.VUs),
	}

	switch {
	case s.VUs > 0 && s.RPS == 0:
		s.Insight = "Traffic is not flowing despite active virtual users."
	case s.ErrorRatePct > 2:
		s.Insight = "Error rate spike detected."
	case s.P95MS > 300:
		s.Insight = "Latency has degraded under load."
	case s.RPS > 0 && s.ErrorRatePct < 0.5 && s.P95MS > 0 && s.P95MS < 120:
		s.Insight = "Stable under target SLO."
	default:
		s.Insight = "Collecting runtime signal."
	}
	return s
}

func scoreRun(run TestRun, series CoreSeries, snapshot InsightSnapshot) (int, []string) {
	score := 100
	var signals []string

	latencyPenalty := latencyPenalty(snapshot.P95MS)
	errorPenalty := errorPenalty(snapshot.ErrorRatePct)
	volatilityPenalty, stability := volatilityPenalty(series)
	saturationPenalty, saturated := saturationPenalty(series, snapshot)

	score -= latencyPenalty
	score -= errorPenalty
	score -= volatilityPenalty
	score -= saturationPenalty

	if snapshot.VUs > 0 && snapshot.RPS == 0 {
		score -= 20
		signals = append(signals, "active VUs but no throughput observed")
	}
	if run.Status == StatusAborted || strings.EqualFold(run.Result, "error") {
		score -= 20
		signals = append(signals, "run ended unsuccessfully")
	}

	if latencyPenalty > 0 {
		signals = append(signals, "p95 latency is above 120ms")
	} else if snapshot.P95MS > 0 {
		signals = append(signals, "p95 remains below 120ms threshold")
	}

	if errorPenalty > 0 {
		signals = append(signals, "error rate is above 0.5%")
	} else if snapshot.RPS > 0 {
		signals = append(signals, "no sustained error spike detected")
	}

	if saturated {
		signals = append(signals, "saturation pattern detected: VUs up while throughput stalls and latency rises")
	} else if stability >= 0.8 {
		signals = append(signals, "latency and error variance are stable")
	} else if stability > 0 {
		signals = append(signals, "short-window metric variance observed")
	}

	if snapshot.RPS == 0 && !run.Status.IsTerminal() {
		signals = append(signals, "warming up metrics pipeline")
	}

	if run.Status.IsTerminal() {
		signals = append(signals, "run reached terminal state: "+string(run.Status))
	}

	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	return score, signals
}

func dayKey(t time.Time) string {
	return t.UTC().Format("2006-01-02")
}

func multiplySeries(points []MetricPoint, factor float64) []MetricPoint {
	if len(points) == 0 {
		return nil
	}
	out := make([]MetricPoint, 0, len(points))
	for _, p := range points {
		out = append(out, MetricPoint{
			Timestamp: p.Timestamp,
			Value:     p.Value * factor,
		})
	}
	return out
}

func lastValue(points []MetricPoint) float64 {
	if len(points) == 0 {
		return 0
	}
	return points[len(points)-1].Value
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func clampFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func latencyPenalty(p95MS float64) int {
	switch {
	case p95MS <= 0:
		return 0
	case p95MS <= 120:
		return 0
	case p95MS <= 250:
		// 0 -> 20
		return int((p95MS-120)/(250-120)*20 + 0.5)
	case p95MS <= 500:
		// 20 -> 35
		return 20 + int((p95MS-250)/(500-250)*15+0.5)
	default:
		// 35 -> 45
		return clampInt(35+int((p95MS-500)/250*10+0.5), 35, 45)
	}
}

func errorPenalty(errorRatePct float64) int {
	switch {
	case errorRatePct <= 0:
		return 0
	case errorRatePct <= 0.5:
		return 0
	case errorRatePct <= 1.0:
		// 0 -> 10
		return int((errorRatePct-0.5)/(1.0-0.5)*10 + 0.5)
	case errorRatePct <= 3.0:
		// 10 -> 30
		return 10 + int((errorRatePct-1.0)/(3.0-1.0)*20+0.5)
	default:
		// 30 -> 45
		return clampInt(30+int((errorRatePct-3.0)/4.0*15+0.5), 30, 45)
	}
}

func volatilityPenalty(series CoreSeries) (penalty int, stability float64) {
	p95CV := coeffVarFromPoints(series.P95MS, 8)
	errCV := coeffVarFromPoints(series.ErrorRatePct, 8)

	// Normalize CV into [0,1] with independent scales.
	// p95 tends to be less noisy than error-rate; use stricter threshold.
	p95Vol := clampFloat(p95CV/0.35, 0, 1)
	errVol := clampFloat(errCV/1.00, 0, 1)

	weightedVol := (0.65 * p95Vol) + (0.35 * errVol)
	penalty = clampInt(int(weightedVol*20+0.5), 0, 20)
	stability = clampFloat(1-weightedVol, 0, 1)
	return penalty, stability
}

func saturationPenalty(series CoreSeries, snapshot InsightSnapshot) (penalty int, saturated bool) {
	if snapshot.VUs < 20 {
		return 0, false
	}

	vuTrend := trendPct(series.VUs, 6)
	rpsTrend := trendPct(series.RPS, 6)
	p95Trend := trendPct(series.P95MS, 6)
	p95Accel := secondDiff(series.P95MS)

	strong := vuTrend > 0.15 && (rpsTrend < 0.02 || snapshot.RPS <= 0) && (p95Trend > 0.18 || p95Accel > 5)
	mild := vuTrend > 0.08 && rpsTrend < 0.05 && (p95Trend > 0.10 || p95Accel > 2)

	switch {
	case strong:
		return 15, true
	case mild:
		return 8, true
	default:
		return 0, false
	}
}

func trendPct(points []MetricPoint, window int) float64 {
	vals := tailValues(points, window)
	if len(vals) < 2 {
		return 0
	}
	start := vals[0]
	end := vals[len(vals)-1]
	denom := math.Max(math.Abs(start), 1)
	return (end - start) / denom
}

func secondDiff(points []MetricPoint) float64 {
	vals := tailValues(points, 3)
	if len(vals) < 3 {
		return 0
	}
	// Approximate acceleration with discrete second derivative.
	return vals[2] - (2 * vals[1]) + vals[0]
}

func coeffVarFromPoints(points []MetricPoint, window int) float64 {
	vals := tailValues(points, window)
	if len(vals) < 3 {
		return 0
	}
	mean := mean(vals)
	if mean == 0 {
		return 0
	}
	return stddev(vals, mean) / math.Abs(mean)
}

func tailValues(points []MetricPoint, window int) []float64 {
	if len(points) == 0 {
		return nil
	}
	if window <= 0 || window > len(points) {
		window = len(points)
	}
	start := len(points) - window
	out := make([]float64, 0, window)
	for _, p := range points[start:] {
		out = append(out, p.Value)
	}
	return out
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func stddev(values []float64, m float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sq float64
	for _, v := range values {
		d := v - m
		sq += d * d
	}
	return math.Sqrt(sq / float64(len(values)))
}

func dedupeStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, item := range in {
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func trimErr(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) > 120 {
		return msg[:120] + "..."
	}
	return msg
}
