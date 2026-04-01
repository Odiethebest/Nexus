package loadtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestService_StartSyncCooldownAndBudget(t *testing.T) {
	var startCalls int32
	var runSeq int64 = 1000

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "missing auth", http.StatusUnauthorized)
			return
		}
		if r.Header.Get("X-Stack-Id") != "42" {
			http.Error(w, "missing stack id", http.StatusUnauthorized)
			return
		}

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/cloud/v6/load_tests/1234/start":
			atomic.AddInt32(&startCalls, 1)
			runID := atomic.AddInt64(&runSeq, 1)
			writeJSON(t, w, map[string]any{
				"id":      runID,
				"test_id": 1234,
				"status":  "created",
				"created": "2026-04-01T12:00:00Z",
			})
			return

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/cloud/v6/test_runs/"):
			idRaw := strings.TrimPrefix(r.URL.Path, "/cloud/v6/test_runs/")
			runID, _ := strconv.ParseInt(idRaw, 10, 64)
			writeJSON(t, w, map[string]any{
				"id":      runID,
				"test_id": 1234,
				"status":  "completed",
				"result":  "passed",
				"created": "2026-04-01T12:00:00Z",
				"ended":   "2026-04-01T12:00:40Z",
				"cost": map[string]any{
					"total_vuh": 30.0,
					"breakdown": map[string]any{
						"base_total_vuh": 30.0,
						"protocol_vuh":   30.0,
						"browser_vuh":    0.0,
						"reduction_rate": 0.0,
					},
				},
				"status_details": map[string]any{
					"type": "completed",
				},
			})
			return

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/cloud/v5/test_runs/") &&
			strings.HasSuffix(r.URL.Path, "/query_range_k6"):
			metric := r.URL.Query().Get("metric")
			switch metric {
			case "http_reqs":
				writeMatrix(t, w, [][]any{
					{1712011200.0, "500"},
					{1712011205.0, "550"},
				})
			case "http_req_duration":
				writeMatrix(t, w, [][]any{
					{1712011200.0, "80"},
					{1712011205.0, "90"},
				})
			case "http_req_failed":
				writeMatrix(t, w, [][]any{
					{1712011200.0, "0.002"},
					{1712011205.0, "0.004"},
				})
			case "vus":
				writeMatrix(t, w, [][]any{
					{1712011200.0, "80"},
					{1712011205.0, "100"},
				})
			default:
				http.Error(w, "unknown metric", http.StatusBadRequest)
			}
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:  server.URL,
		APIToken: "test-token",
		StackID:  "42",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	svc := NewService(
		ServiceConfig{
			Enabled:      true,
			LoadTestID:   1234,
			PollInterval: 3 * time.Second,
			DailyVUHCap:  50,
			Now:          func() time.Time { return now },
		},
		client,
		NewGuard(GuardConfig{
			AdminKey:    "secret",
			MaxParallel: 1,
			Cooldown:    2 * time.Minute,
		}),
	)

	if _, err := svc.Start(context.Background(), "wrong-key", "tester", StartOptions{}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected unauthorized, got %v", err)
	}

	start1, err := svc.Start(context.Background(), "secret", "tester", StartOptions{Note: "run-1"})
	if err != nil {
		t.Fatalf("start run 1: %v", err)
	}
	if start1.RunID == 0 {
		t.Fatalf("expected non-zero run id")
	}
	if start1.PollAfter != 3*time.Second {
		t.Fatalf("unexpected poll interval: %s", start1.PollAfter)
	}

	insight1, err := svc.SyncRun(context.Background(), start1.RunID)
	if err != nil {
		t.Fatalf("sync run 1: %v", err)
	}
	if insight1.Snapshot.RPS <= 0 {
		t.Fatalf("expected rps > 0, got %v", insight1.Snapshot.RPS)
	}
	if insight1.HealthScore <= 0 {
		t.Fatalf("expected health score > 0, got %d", insight1.HealthScore)
	}

	if got := svc.DailyVUHUsed(now); got != 30 {
		t.Fatalf("expected daily VUH 30, got %v", got)
	}

	now = now.Add(1 * time.Minute)
	_, err = svc.Start(context.Background(), "secret", "tester", StartOptions{Note: "cooldown-check"})
	if !errors.Is(err, ErrCooldown) {
		t.Fatalf("expected cooldown error, got %v", err)
	}
	var cooldownErr *CooldownError
	if !errors.As(err, &cooldownErr) || cooldownErr.Remaining <= 0 {
		t.Fatalf("expected cooldown remaining > 0, got %v", err)
	}

	now = now.Add(2 * time.Minute)
	start2, err := svc.Start(context.Background(), "secret", "tester", StartOptions{Note: "run-2"})
	if err != nil {
		t.Fatalf("start run 2: %v", err)
	}
	if _, err := svc.SyncRun(context.Background(), start2.RunID); err != nil {
		t.Fatalf("sync run 2: %v", err)
	}

	if got := svc.DailyVUHUsed(now); got != 60 {
		t.Fatalf("expected daily VUH 60, got %v", got)
	}

	now = now.Add(3 * time.Minute)
	if _, err := svc.Start(context.Background(), "secret", "tester", StartOptions{Note: "budget-check"}); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected budget exceeded, got %v", err)
	}

	if got := atomic.LoadInt32(&startCalls); got != 2 {
		t.Fatalf("expected exactly 2 upstream start calls, got %d", got)
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode json: %v", err)
	}
}

func writeMatrix(t *testing.T, w http.ResponseWriter, values [][]any) {
	t.Helper()
	payload := map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "matrix",
			"result": []map[string]any{
				{
					"metric": map[string]any{"__name__": "dummy"},
					"values": values,
				},
			},
		},
	}
	writeJSON(t, w, payload)
}

func TestClient_DecodeWrappedRunResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/cloud/v6/load_tests/1234/start":
			writeJSON(t, w, map[string]any{
				"value": []map[string]any{
					{
						"id":      8888,
						"test_id": 1234,
						"status":  "created",
						"created": "2026-04-01T12:00:00.553",
					},
				},
			})
		default:
			http.Error(w, fmt.Sprintf("unexpected endpoint %s", r.URL.Path), http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:  server.URL,
		APIToken: "test-token",
		StackID:  "42",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	run, err := client.StartLoadTest(context.Background(), 1234, StartOptions{})
	if err != nil {
		t.Fatalf("start loadtest: %v", err)
	}
	if run.ID != 8888 {
		t.Fatalf("expected wrapped run id 8888, got %d", run.ID)
	}
}
