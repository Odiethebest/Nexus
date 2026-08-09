package loadtest

import (
	"context"
	"errors"
	"fmt"
	"nexus/internal/metrics"
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
	Enabled              bool
	LoadTestID           int64
	PollInterval         time.Duration
	DailyVUHCap          float64
	MaxRunDuration       time.Duration
	StatusRequestTimeout time.Duration
	MetricQueryTimeout   time.Duration
	Now                  func() time.Time
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
	runFirstSeen map[int64]time.Time
}

type runAborter interface {
	AbortTestRun(ctx context.Context, runID int64) error
}

// NewService creates a loadtest service with defaults.
func NewService(cfg ServiceConfig, client K6API, guard *Guard) *Service {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 3 * time.Second
	}
	if cfg.MaxRunDuration < 0 {
		cfg.MaxRunDuration = 0
	}
	if cfg.StatusRequestTimeout < 0 {
		cfg.StatusRequestTimeout = 0
	}
	if cfg.MetricQueryTimeout < 0 {
		cfg.MetricQueryTimeout = 0
	}
	if cfg.StatusRequestTimeout == 0 {
		cfg.StatusRequestTimeout = 4 * time.Second
	}
	if cfg.MetricQueryTimeout == 0 {
		cfg.MetricQueryTimeout = 3 * time.Second
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
		runFirstSeen: make(map[int64]time.Time),
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
	runCtx, cancelRun := withContextTimeout(ctx, s.cfg.StatusRequestTimeout)
	defer cancelRun()
	run, err := s.client.GetTestRun(runCtx, runID)
	if err != nil {
		return RunInsight{}, err
	}
	if run.ID == 0 {
		run.ID = runID
	}
	startedAt := s.resolveRunStartTime(run.ID, run.Created)
	if run.Created.IsZero() && !startedAt.IsZero() {
		run.Created = startedAt
	}
	timeoutWarnings := s.abortIfRunTimedOut(ctx, &run, startedAt)

	series := CoreSeries{}
	warnings := []string(nil)
	if shouldFetchCoreSeries(run.Status) {
		series, warnings = s.fetchCoreSeries(ctx, run.ID)
	}
	if len(timeoutWarnings) > 0 {
		warnings = append(timeoutWarnings, warnings...)
	}
	snapshot := buildSnapshot(series)
	score, signals := scoreRun(run, series, snapshot)
	metrics.LoadtestHealthScore.Set(float64(score))
	if len(warnings) > 0 {
		signals = append(signals, "Some metric queries are temporarily unavailable")
	}

	if run.Status.IsTerminal() {
		s.guard.MarkFinished(run.ID, s.now().UTC())
		s.clearRunTracking(run.ID)
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
	queries := []struct {
		name   string
		metric string
		query  string
		assign func([]MetricPoint)
	}{
		{"rps", "http_reqs", "rate", func(p []MetricPoint) { out.RPS = p }},
		{"p95", "http_req_duration", "histogram_quantile(0.95)", func(p []MetricPoint) { out.P95MS = p }},
		{"error_rate", "http_req_failed", "rate", func(p []MetricPoint) { out.ErrorRatePct = multiplySeries(p, 100) }},
		{"vus", "vus", "sum(last by (instance_id))", func(p []MetricPoint) { out.VUs = p }},
	}

	// Each query owns one slot, so the four run concurrently without a
	// channel, a map, or a second pass to put them back in order. The assign
	// closures touch `out` only in the sequential loop after Wait.
	type result struct {
		points []MetricPoint
		err    error
	}
	results := make([]result, len(queries))

	var wg sync.WaitGroup
	for i, q := range queries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			queryCtx, cancel := withContextTimeout(ctx, s.cfg.MetricQueryTimeout)
			defer cancel()
			points, err := s.client.QueryRangeK6(queryCtx, runID, q.metric, q.query, stepSeconds)
			results[i] = result{points: points, err: err}
		}()
	}
	wg.Wait()

	var warnings []string
	for i, q := range queries {
		if results[i].err != nil {
			warnings = append(warnings, q.name+": "+trimErr(results[i].err))
			continue
		}
		q.assign(results[i].points)
	}
	return out, warnings
}

func (s *Service) abortIfRunTimedOut(ctx context.Context, run *TestRun, startedAt time.Time) []string {
	if run == nil {
		return nil
	}
	if s.cfg.MaxRunDuration <= 0 {
		return nil
	}
	if run.Status.IsTerminal() {
		return nil
	}
	if startedAt.IsZero() {
		return nil
	}
	elapsed := s.now().UTC().Sub(startedAt.UTC())
	if elapsed < s.cfg.MaxRunDuration {
		return nil
	}

	warnings := []string{
		fmt.Sprintf(
			"run exceeded %s, forcing abort for demo responsiveness",
			s.cfg.MaxRunDuration.Truncate(time.Second),
		),
	}

	if aborter, ok := s.client.(runAborter); ok {
		if err := aborter.AbortTestRun(ctx, run.ID); err != nil {
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.StatusCode != 409 {
				warnings = append(warnings, "abort request failed: "+trimErr(err))
			}
		}
	} else {
		warnings = append(warnings, "client does not support abort API")
	}

	now := s.now().UTC()
	run.Status = StatusAborted
	run.Result = "aborted"
	run.Ended = &now
	return warnings
}

func shouldFetchCoreSeries(status RunStatus) bool {
	switch status {
	case StatusRunning, StatusProcessingMetrics, StatusCompleted, StatusAborted:
		return true
	default:
		return false
	}
}

func withContextTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, timeout)
}

func (s *Service) resolveRunStartTime(runID int64, created time.Time) time.Time {
	if !created.IsZero() {
		return created.UTC()
	}
	if startedAt, ok := s.guard.ActiveRunStartedAt(runID); ok && !startedAt.IsZero() {
		return startedAt.UTC()
	}
	if runID <= 0 {
		return time.Time{}
	}

	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if firstSeen, ok := s.runFirstSeen[runID]; ok && !firstSeen.IsZero() {
		return firstSeen
	}
	s.runFirstSeen[runID] = now
	return now
}

func (s *Service) clearRunTracking(runID int64) {
	if runID <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.runFirstSeen, runID)
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
