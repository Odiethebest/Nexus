package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"nexus/internal/loadtest"
)

type fakeK6API struct {
	startFn func(ctx context.Context, testID int64, req loadtest.StartOptions) (loadtest.TestRun, error)
	getFn   func(ctx context.Context, runID int64) (loadtest.TestRun, error)
	queryFn func(ctx context.Context, runID int64, metric, query string, stepSeconds int) ([]loadtest.MetricPoint, error)
}

func (f fakeK6API) StartLoadTest(ctx context.Context, testID int64, req loadtest.StartOptions) (loadtest.TestRun, error) {
	return f.startFn(ctx, testID, req)
}

func (f fakeK6API) GetTestRun(ctx context.Context, runID int64) (loadtest.TestRun, error) {
	return f.getFn(ctx, runID)
}

func (f fakeK6API) QueryRangeK6(
	ctx context.Context,
	runID int64,
	metric, query string,
	stepSeconds int,
) ([]loadtest.MetricPoint, error) {
	return f.queryFn(ctx, runID, metric, query, stepSeconds)
}

func TestHandleLoadtestStart_ContractAndAuth(t *testing.T) {
	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	svc := loadtest.NewService(
		loadtest.ServiceConfig{
			Enabled:      true,
			LoadTestID:   1234,
			PollInterval: 3 * time.Second,
			Now:          func() time.Time { return now },
		},
		fakeK6API{
			startFn: func(_ context.Context, testID int64, req loadtest.StartOptions) (loadtest.TestRun, error) {
				if testID != 1234 {
					t.Fatalf("unexpected test id %d", testID)
				}
				if req.Preset != "quick" || req.Note != "smoke" || req.Scenario != "default" {
					t.Fatalf("unexpected request: %+v", req)
				}
				return loadtest.TestRun{
					ID:      9001,
					TestID:  1234,
					Status:  loadtest.StatusCreated,
					Created: now,
				}, nil
			},
			getFn: func(context.Context, int64) (loadtest.TestRun, error) {
				return loadtest.TestRun{}, nil
			},
			queryFn: func(context.Context, int64, string, string, int) ([]loadtest.MetricPoint, error) {
				return nil, nil
			},
		},
		loadtest.NewGuard(loadtest.GuardConfig{
			AdminKey:    "secret",
			MaxParallel: 1,
		}),
	)

	var latest atomic.Int64
	handler := handleLoadtestStart(svc, &latest)

	req := httptest.NewRequest("POST", "/ops/loadtest/start", strings.NewReader(`{"scenario":"default","preset":"quick","note":"smoke"}`))
	req.Header.Set("X-Admin-Key", "secret")
	req.RemoteAddr = "203.0.113.8:34567"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != 202 {
		t.Fatalf("expected 202, got %d body=%s", rec.Code, rec.Body.String())
	}

	var resp loadtestStartResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.RunID != 9001 || resp.TestID != 1234 {
		t.Fatalf("unexpected ids: %+v", resp)
	}
	if resp.Status != loadtest.StatusCreated {
		t.Fatalf("unexpected status: %s", resp.Status)
	}
	if resp.PollAfterSeconds != 3 {
		t.Fatalf("unexpected poll interval: %d", resp.PollAfterSeconds)
	}
	if got := latest.Load(); got != 9001 {
		t.Fatalf("expected latest run 9001, got %d", got)
	}

	unauthReq := httptest.NewRequest("POST", "/ops/loadtest/start", strings.NewReader(`{}`))
	unauthReq.Header.Set("X-Admin-Key", "wrong")
	unauthRec := httptest.NewRecorder()
	handler.ServeHTTP(unauthRec, unauthReq)
	if unauthRec.Code != 403 {
		t.Fatalf("expected 403 for bad key, got %d body=%s", unauthRec.Code, unauthRec.Body.String())
	}
}

func TestHandleLoadtestStatus_ContractShape(t *testing.T) {
	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	points := []loadtest.MetricPoint{
		{Timestamp: now, Value: 100},
		{Timestamp: now.Add(5 * time.Second), Value: 120},
	}

	svc := loadtest.NewService(
		loadtest.ServiceConfig{
			Enabled:      true,
			LoadTestID:   1234,
			PollInterval: 3 * time.Second,
			Now:          func() time.Time { return now },
		},
		fakeK6API{
			startFn: func(context.Context, int64, loadtest.StartOptions) (loadtest.TestRun, error) {
				return loadtest.TestRun{}, nil
			},
			getFn: func(_ context.Context, runID int64) (loadtest.TestRun, error) {
				return loadtest.TestRun{
					ID:      runID,
					TestID:  1234,
					Status:  loadtest.StatusRunning,
					Created: now,
				}, nil
			},
			queryFn: func(_ context.Context, _ int64, metric, _ string, _ int) ([]loadtest.MetricPoint, error) {
				switch metric {
				case "http_reqs":
					return points, nil
				case "http_req_duration":
					return []loadtest.MetricPoint{
						{Timestamp: now, Value: 70},
						{Timestamp: now.Add(5 * time.Second), Value: 90},
					}, nil
				case "http_req_failed":
					return []loadtest.MetricPoint{
						{Timestamp: now, Value: 0.001},
						{Timestamp: now.Add(5 * time.Second), Value: 0.002},
					}, nil
				case "vus":
					return []loadtest.MetricPoint{
						{Timestamp: now, Value: 50},
						{Timestamp: now.Add(5 * time.Second), Value: 60},
					}, nil
				default:
					return nil, nil
				}
			},
		},
		loadtest.NewGuard(loadtest.GuardConfig{}),
	)

	handler := handleLoadtestStatus(svc)
	req := httptest.NewRequest("GET", "/ops/loadtest/9001", nil)
	req.SetPathValue("run_id", "9001")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	run, ok := payload["run"].(map[string]any)
	if !ok {
		t.Fatalf("missing run object")
	}
	if run["status"] != string(loadtest.StatusRunning) {
		t.Fatalf("unexpected run status: %#v", run["status"])
	}

	series, ok := payload["series"].(map[string]any)
	if !ok {
		t.Fatalf("missing series object")
	}
	rps, ok := series["rps"].([]any)
	if !ok || len(rps) != 2 {
		t.Fatalf("expected rps array with 2 points, got %#v", series["rps"])
	}
	first, ok := rps[0].([]any)
	if !ok || len(first) != 2 {
		t.Fatalf("expected tuple point [ts,value], got %#v", rps[0])
	}

	if _, ok := payload["snapshot"].(map[string]any); !ok {
		t.Fatalf("missing snapshot object")
	}
	if _, ok := payload["health_score"].(float64); !ok {
		t.Fatalf("missing numeric health_score")
	}
}
