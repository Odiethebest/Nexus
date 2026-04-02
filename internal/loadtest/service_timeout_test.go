package loadtest

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type timeoutFakeAPI struct {
	run        TestRun
	abortCalls atomic.Int32
	queryCalls atomic.Int32
}

func (f *timeoutFakeAPI) StartLoadTest(context.Context, int64, StartOptions) (TestRun, error) {
	return TestRun{}, nil
}

func (f *timeoutFakeAPI) GetTestRun(context.Context, int64) (TestRun, error) {
	return f.run, nil
}

func (f *timeoutFakeAPI) QueryRangeK6(context.Context, int64, string, string, int) ([]MetricPoint, error) {
	f.queryCalls.Add(1)
	return nil, nil
}

func (f *timeoutFakeAPI) AbortTestRun(context.Context, int64) error {
	f.abortCalls.Add(1)
	return nil
}

func TestService_SyncRun_AbortsWhenExceededMaxDuration(t *testing.T) {
	now := time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC)
	api := &timeoutFakeAPI{
		run: TestRun{
			ID:      9001,
			TestID:  1234,
			Status:  StatusRunning,
			Created: now.Add(-2 * time.Minute),
		},
	}

	svc := NewService(
		ServiceConfig{
			Enabled:        true,
			LoadTestID:     1234,
			PollInterval:   3 * time.Second,
			MaxRunDuration: 60 * time.Second,
			Now:            func() time.Time { return now },
		},
		api,
		NewGuard(GuardConfig{}),
	)

	insight, err := svc.SyncRun(context.Background(), 9001)
	if err != nil {
		t.Fatalf("sync run: %v", err)
	}
	if insight.Run.Status != StatusAborted {
		t.Fatalf("expected status %q, got %q", StatusAborted, insight.Run.Status)
	}
	if api.abortCalls.Load() != 1 {
		t.Fatalf("expected 1 abort call, got %d", api.abortCalls.Load())
	}
	if api.queryCalls.Load() == 0 {
		t.Fatalf("expected metric queries after forced abort")
	}

	found := false
	for _, warning := range insight.Warnings {
		if strings.Contains(warning, "forcing abort") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected timeout warning, got %v", insight.Warnings)
	}
}

func TestService_SyncRun_AbortsWhenCreatedMissingUsingGuardStartTime(t *testing.T) {
	now := time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC)
	api := &timeoutFakeAPI{
		run: TestRun{
			ID:      9010,
			TestID:  1234,
			Status:  StatusRunning,
			Created: time.Time{},
		},
	}

	guard := NewGuard(GuardConfig{})
	reservation, err := guard.ReserveStart("tester", now.Add(-2*time.Minute))
	if err != nil {
		t.Fatalf("reserve start: %v", err)
	}
	reservation.Commit(9010, now.Add(-2*time.Minute))

	svc := NewService(
		ServiceConfig{
			Enabled:        true,
			LoadTestID:     1234,
			PollInterval:   2 * time.Second,
			MaxRunDuration: 55 * time.Second,
			Now:            func() time.Time { return now },
		},
		api,
		guard,
	)

	insight, err := svc.SyncRun(context.Background(), 9010)
	if err != nil {
		t.Fatalf("sync run: %v", err)
	}
	if insight.Run.Status != StatusAborted {
		t.Fatalf("expected status %q, got %q", StatusAborted, insight.Run.Status)
	}
	if insight.Run.Created.IsZero() {
		t.Fatalf("expected created timestamp fallback from guard")
	}
	if api.abortCalls.Load() != 1 {
		t.Fatalf("expected 1 abort call, got %d", api.abortCalls.Load())
	}
}
