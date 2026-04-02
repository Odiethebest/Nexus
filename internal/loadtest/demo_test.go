package loadtest

import (
	"context"
	"testing"
	"time"
)

func TestDemoService_RunLifecycleAndMetrics(t *testing.T) {
	now := time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC)
	svc := NewDemoService(DemoServiceConfig{
		PollInterval: 2 * time.Second,
		RunDuration:  55 * time.Second,
		Now:          func() time.Time { return now },
	})

	started, err := svc.Start(context.Background(), StartOptions{Note: "demo"})
	if err != nil {
		t.Fatalf("start demo run: %v", err)
	}
	if started.Status != StatusCreated {
		t.Fatalf("expected created status, got %q", started.Status)
	}
	if !svc.HasRun(started.RunID) {
		t.Fatalf("expected run to be tracked")
	}

	createdInsight, err := svc.SyncRun(context.Background(), started.RunID)
	if err != nil {
		t.Fatalf("sync created run: %v", err)
	}
	if createdInsight.Run.Status != StatusCreated {
		t.Fatalf("expected created status, got %q", createdInsight.Run.Status)
	}
	if len(createdInsight.Series.RPS) != 0 {
		t.Fatalf("expected no metrics during created phase")
	}

	now = now.Add(20 * time.Second)
	runningInsight, err := svc.SyncRun(context.Background(), started.RunID)
	if err != nil {
		t.Fatalf("sync running run: %v", err)
	}
	if runningInsight.Run.Status != StatusRunning {
		t.Fatalf("expected running status, got %q", runningInsight.Run.Status)
	}
	if len(runningInsight.Series.RPS) == 0 {
		t.Fatalf("expected synthetic metrics while running")
	}
	if runningInsight.Snapshot.RPS <= 0 {
		t.Fatalf("expected positive rps snapshot, got %v", runningInsight.Snapshot.RPS)
	}

	now = now.Add(45 * time.Second)
	doneInsight, err := svc.SyncRun(context.Background(), started.RunID)
	if err != nil {
		t.Fatalf("sync completed run: %v", err)
	}
	if doneInsight.Run.Status != StatusCompleted {
		t.Fatalf("expected completed status, got %q", doneInsight.Run.Status)
	}
	if doneInsight.Run.Ended == nil || doneInsight.Run.Ended.IsZero() {
		t.Fatalf("expected ended timestamp for completed run")
	}
	if doneInsight.Run.Result != "passed" {
		t.Fatalf("expected passed result, got %q", doneInsight.Run.Result)
	}
}
