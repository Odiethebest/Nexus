package loadtest

import (
	"context"
	"fmt"
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
	score, signals := scoreRun(run, snapshot)
	if len(warnings) > 0 {
		signals = append(signals, "Some metric queries are temporarily unavailable")
	}

	if run.Status.IsTerminal() {
		s.guard.MarkFinished(run.ID, s.now().UTC())
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

func scoreRun(run TestRun, snapshot InsightSnapshot) (int, []string) {
	score := 100
	var signals []string

	if snapshot.P95MS > 120 {
		score -= minInt(45, int((snapshot.P95MS-120)/8)+1)
		signals = append(signals, "p95 latency is above 120ms")
	}
	if snapshot.ErrorRatePct > 0.5 {
		score -= minInt(45, int((snapshot.ErrorRatePct-0.5)*12)+1)
		signals = append(signals, "error rate is above 0.5%")
	}
	if snapshot.VUs > 0 && snapshot.RPS == 0 {
		score -= 20
		signals = append(signals, "active VUs but no throughput observed")
	}
	if run.Status == StatusAborted || strings.EqualFold(run.Result, "error") {
		score -= 20
		signals = append(signals, "run ended unsuccessfully")
	}

	switch {
	case snapshot.ErrorRatePct < 0.5 && snapshot.RPS > 0:
		signals = append(signals, "no sustained error spike detected")
	case snapshot.RPS == 0 && !run.Status.IsTerminal():
		signals = append(signals, "warming up metrics pipeline")
	}
	if snapshot.P95MS > 0 && snapshot.P95MS <= 120 {
		signals = append(signals, "p95 remains below 120ms threshold")
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
