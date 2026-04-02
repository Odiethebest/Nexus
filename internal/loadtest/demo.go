package loadtest

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"
)

const (
	defaultDemoRunDuration = 55 * time.Second
	defaultDemoPoll        = 2 * time.Second
	defaultDemoRunIDBase   = int64(9000000000000)
)

// DemoServiceConfig controls synthetic run behavior for fast demos.
type DemoServiceConfig struct {
	PollInterval time.Duration
	RunDuration  time.Duration
	RunIDBase    int64
	Now          func() time.Time
}

type demoRun struct {
	ID        int64
	StartedAt time.Time
	Options   StartOptions
}

// DemoService provides deterministic runs that do not depend on cloud scheduling.
type DemoService struct {
	cfg DemoServiceConfig
	now func() time.Time

	mu     sync.Mutex
	nextID int64
	runs   map[int64]demoRun
}

// NewDemoService creates a synthetic loadtest service.
func NewDemoService(cfg DemoServiceConfig) *DemoService {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultDemoPoll
	}
	if cfg.RunDuration <= 0 {
		cfg.RunDuration = defaultDemoRunDuration
	}
	if cfg.RunIDBase <= 0 {
		cfg.RunIDBase = defaultDemoRunIDBase
	}
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &DemoService{
		cfg:    cfg,
		now:    nowFn,
		nextID: cfg.RunIDBase,
		runs:   make(map[int64]demoRun),
	}
}

// Start creates a synthetic run immediately.
func (d *DemoService) Start(_ context.Context, opts StartOptions) (StartResult, error) {
	now := d.now().UTC()
	d.mu.Lock()
	defer d.mu.Unlock()

	d.pruneLocked(now)
	d.nextID++
	runID := d.nextID
	d.runs[runID] = demoRun{
		ID:        runID,
		StartedAt: now,
		Options:   opts,
	}
	return StartResult{
		RunID:     runID,
		TestID:    0,
		Status:    StatusCreated,
		StartedAt: now,
		PollAfter: d.cfg.PollInterval,
	}, nil
}

// HasRun reports whether the run belongs to demo service.
func (d *DemoService) HasRun(runID int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.runs[runID]
	return ok
}

// SyncRun returns synthetic status and metrics for a run.
func (d *DemoService) SyncRun(ctx context.Context, runID int64) (RunInsight, error) {
	if err := ctx.Err(); err != nil {
		return RunInsight{}, err
	}

	now := d.now().UTC()
	d.mu.Lock()
	run, ok := d.runs[runID]
	d.mu.Unlock()
	if !ok {
		return RunInsight{}, &APIError{
			StatusCode: http.StatusNotFound,
			Body:       fmt.Sprintf("demo run %d not found", runID),
		}
	}

	elapsed := now.Sub(run.StartedAt)
	if elapsed < 0 {
		elapsed = 0
	}

	status := demoStatusForElapsed(elapsed, d.cfg.RunDuration)
	model := TestRun{
		ID:      run.ID,
		Status:  status,
		Created: run.StartedAt,
		Note:    run.Options.Note,
		StatusDetails: StatusDetails{
			Type: status,
		},
	}

	if status.IsTerminal() {
		ended := run.StartedAt.Add(d.cfg.RunDuration).UTC()
		if ended.After(now) {
			ended = now
		}
		model.Ended = &ended
		model.Result = "passed"
	}

	series := demoSeries(run.StartedAt, elapsed, d.cfg.PollInterval, d.cfg.RunDuration)
	snapshot := buildSnapshot(series)
	score, signals := scoreRun(model, series, snapshot)

	signals = append([]string{
		"demo mode: synthetic run stream enabled",
		"cloud queue and upstream warmup are bypassed",
	}, signals...)

	warnings := []string(nil)
	if status == StatusCompleted {
		warnings = append(warnings, "demo mode metrics are synthetic and for walkthrough only")
	}

	return RunInsight{
		Run:         model,
		Series:      series,
		Snapshot:    snapshot,
		HealthScore: score,
		Signals:     dedupeStrings(signals),
		Warnings:    warnings,
	}, nil
}

func (d *DemoService) pruneLocked(now time.Time) {
	ttl := 24 * time.Hour
	for runID, run := range d.runs {
		if now.Sub(run.StartedAt) > ttl {
			delete(d.runs, runID)
		}
	}
}

func demoStatusForElapsed(elapsed, runDuration time.Duration) RunStatus {
	switch {
	case elapsed < 4*time.Second:
		return StatusCreated
	case elapsed < 8*time.Second:
		return StatusQueued
	case elapsed < 12*time.Second:
		return StatusInitializing
	case elapsed < 45*time.Second:
		return StatusRunning
	case elapsed < runDuration:
		return StatusProcessingMetrics
	default:
		return StatusCompleted
	}
}

func demoSeries(startedAt time.Time, elapsed, poll, runDuration time.Duration) CoreSeries {
	if poll <= 0 {
		poll = defaultDemoPoll
	}
	if runDuration <= 0 {
		runDuration = defaultDemoRunDuration
	}

	runningStart := 12 * time.Second
	if elapsed <= runningStart {
		return CoreSeries{}
	}

	sampleUntil := elapsed
	runningWindow := 45 * time.Second
	if runningWindow > runDuration {
		runningWindow = runDuration
	}
	if sampleUntil > runningWindow {
		sampleUntil = runningWindow
	}

	var out CoreSeries
	for offset := runningStart; offset <= sampleUntil; offset += poll {
		seconds := offset.Seconds() - runningStart.Seconds()
		ts := startedAt.Add(offset).UTC()
		out.RPS = append(out.RPS, MetricPoint{
			Timestamp: ts,
			Value:     demoRPS(seconds),
		})
		out.P95MS = append(out.P95MS, MetricPoint{
			Timestamp: ts,
			Value:     demoP95MS(seconds),
		})
		out.ErrorRatePct = append(out.ErrorRatePct, MetricPoint{
			Timestamp: ts,
			Value:     demoErrorPct(seconds),
		})
		out.VUs = append(out.VUs, MetricPoint{
			Timestamp: ts,
			Value:     demoVUs(seconds),
		})
	}
	return out
}

func demoRPS(sec float64) float64 {
	switch {
	case sec < 8:
		return 40 + sec*14
	case sec < 24:
		return 152 + math.Sin(sec/2.7)*12
	case sec < 30:
		return 152 - (sec-24)*9
	default:
		return 98 + math.Sin(sec/2.4)*6
	}
}

func demoP95MS(sec float64) float64 {
	switch {
	case sec < 15:
		return 68 + sec*2.2
	case sec < 24:
		return 101 + (sec-15)*1.7
	default:
		return 116 + math.Sin(sec/2.3)*7
	}
}

func demoErrorPct(sec float64) float64 {
	base := 0.14 + math.Max(sec-6, 0)*0.02
	wave := math.Sin(sec/3.1) * 0.09
	value := base + wave
	if value < 0.02 {
		return 0.02
	}
	if value > 1.8 {
		return 1.8
	}
	return value
}

func demoVUs(sec float64) float64 {
	switch {
	case sec < 10:
		return 20 + sec*5
	case sec < 22:
		return 70 + (sec-10)*2.2
	default:
		return 96 + math.Sin(sec/4.2)*4
	}
}
