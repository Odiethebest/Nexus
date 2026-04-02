package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"nexus/internal/loadtest"
)

func TestMapLoadtestError_StatusAndMessage(t *testing.T) {
	until := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		err           error
		wantStatus    int
		wantMessage   string
		wantSubstring string
	}{
		{
			name:        "unauthorized",
			err:         loadtest.ErrUnauthorized,
			wantStatus:  http.StatusForbidden,
			wantMessage: "forbidden",
		},
		{
			name:        "disabled",
			err:         loadtest.ErrDisabled,
			wantStatus:  http.StatusServiceUnavailable,
			wantMessage: "loadtest is disabled",
		},
		{
			name:        "parallel limit",
			err:         loadtest.ErrParallelLimit,
			wantStatus:  http.StatusConflict,
			wantMessage: "loadtest already running",
		},
		{
			name:          "cooldown",
			err:           &loadtest.CooldownError{Remaining: 75 * time.Second},
			wantStatus:    http.StatusTooManyRequests,
			wantSubstring: "retry in",
		},
		{
			name:          "throttled",
			err:           &loadtest.ThrottleError{Remaining: 10 * time.Second},
			wantStatus:    http.StatusTooManyRequests,
			wantSubstring: "retry in",
		},
		{
			name:        "budget exceeded",
			err:         loadtest.ErrBudgetExceeded,
			wantStatus:  http.StatusTooManyRequests,
			wantMessage: loadtest.ErrBudgetExceeded.Error(),
		},
		{
			name:          "circuit open",
			err:           &loadtest.CircuitOpenError{Until: until},
			wantStatus:    http.StatusServiceUnavailable,
			wantSubstring: "retry after",
		},
		{
			name:        "upstream 404",
			err:         &loadtest.APIError{StatusCode: http.StatusNotFound, Body: "missing"},
			wantStatus:  http.StatusNotFound,
			wantMessage: "loadtest run not found",
		},
		{
			name:        "upstream non-404",
			err:         &loadtest.APIError{StatusCode: http.StatusBadGateway, Body: "bad gateway"},
			wantStatus:  http.StatusBadGateway,
			wantMessage: "upstream loadtest API failed",
		},
		{
			name:        "internal fallback",
			err:         errors.New("boom"),
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "internal error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStatus, gotMsg := mapLoadtestError(tt.err)
			if gotStatus != tt.wantStatus {
				t.Fatalf("status = %d, want %d", gotStatus, tt.wantStatus)
			}
			if tt.wantMessage != "" && gotMsg != tt.wantMessage {
				t.Fatalf("message = %q, want %q", gotMsg, tt.wantMessage)
			}
			if tt.wantSubstring != "" && !strings.Contains(gotMsg, tt.wantSubstring) {
				t.Fatalf("message = %q, expected substring %q", gotMsg, tt.wantSubstring)
			}
		})
	}
}

func TestHandleLoadtestStart_StatusBehavior(t *testing.T) {
	t.Run("service unavailable when nil", func(t *testing.T) {
		var latest atomic.Int64
		handler := handleLoadtestStart(nil, nil, &latest)
		req := httptest.NewRequest(http.MethodPost, "/ops/loadtest/start", strings.NewReader(`{}`))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503, got %d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("disabled maps to 503", func(t *testing.T) {
		svc := loadtest.NewService(
			loadtest.ServiceConfig{
				Enabled:      false,
				LoadTestID:   1234,
				PollInterval: 3 * time.Second,
			},
			fakeK6API{
				startFn: func(context.Context, int64, loadtest.StartOptions) (loadtest.TestRun, error) {
					t.Fatalf("start should not be called when service is disabled")
					return loadtest.TestRun{}, nil
				},
				getFn: func(context.Context, int64) (loadtest.TestRun, error) {
					return loadtest.TestRun{}, nil
				},
				queryFn: func(context.Context, int64, string, string, int) ([]loadtest.MetricPoint, error) {
					return nil, nil
				},
			},
			loadtest.NewGuard(loadtest.GuardConfig{}),
		)

		var latest atomic.Int64
		handler := handleLoadtestStart(svc, nil, &latest)
		req := httptest.NewRequest(http.MethodPost, "/ops/loadtest/start", strings.NewReader(`{}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503, got %d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("parallel limit maps to 409", func(t *testing.T) {
		now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
		var seq int64 = 7000
		svc := loadtest.NewService(
			loadtest.ServiceConfig{
				Enabled:      true,
				LoadTestID:   1234,
				PollInterval: 3 * time.Second,
				Now:          func() time.Time { return now },
			},
			fakeK6API{
				startFn: func(_ context.Context, testID int64, _ loadtest.StartOptions) (loadtest.TestRun, error) {
					runID := atomic.AddInt64(&seq, 1)
					return loadtest.TestRun{
						ID:      runID,
						TestID:  testID,
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
		handler := handleLoadtestStart(svc, nil, &latest)

		req1 := httptest.NewRequest(http.MethodPost, "/ops/loadtest/start", strings.NewReader(`{}`))
		req1.Header.Set("X-Admin-Key", "secret")
		rec1 := httptest.NewRecorder()
		handler.ServeHTTP(rec1, req1)
		if rec1.Code != http.StatusAccepted {
			t.Fatalf("first start expected 202, got %d body=%s", rec1.Code, rec1.Body.String())
		}

		req2 := httptest.NewRequest(http.MethodPost, "/ops/loadtest/start", strings.NewReader(`{}`))
		req2.Header.Set("X-Admin-Key", "secret")
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)
		if rec2.Code != http.StatusConflict {
			t.Fatalf("second start expected 409, got %d body=%s", rec2.Code, rec2.Body.String())
		}
	})
}

func TestHandleLoadtestStatus_StatusBehavior(t *testing.T) {
	t.Run("service unavailable when nil", func(t *testing.T) {
		handler := handleLoadtestStatus(nil, nil)
		req := httptest.NewRequest(http.MethodGet, "/ops/loadtest/1", nil)
		req.SetPathValue("run_id", "1")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503, got %d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("invalid run_id returns 400", func(t *testing.T) {
		svc := loadtest.NewService(
			loadtest.ServiceConfig{
				Enabled:      true,
				LoadTestID:   1234,
				PollInterval: 3 * time.Second,
			},
			fakeK6API{
				startFn: func(context.Context, int64, loadtest.StartOptions) (loadtest.TestRun, error) {
					return loadtest.TestRun{}, nil
				},
				getFn: func(context.Context, int64) (loadtest.TestRun, error) {
					t.Fatalf("get should not be called for invalid run_id")
					return loadtest.TestRun{}, nil
				},
				queryFn: func(context.Context, int64, string, string, int) ([]loadtest.MetricPoint, error) {
					return nil, nil
				},
			},
			loadtest.NewGuard(loadtest.GuardConfig{}),
		)

		handler := handleLoadtestStatus(svc, nil)
		req := httptest.NewRequest(http.MethodGet, "/ops/loadtest/not-a-number", nil)
		req.SetPathValue("run_id", "not-a-number")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("upstream 404 maps to 404", func(t *testing.T) {
		now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
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
				getFn: func(context.Context, int64) (loadtest.TestRun, error) {
					return loadtest.TestRun{}, &loadtest.APIError{StatusCode: http.StatusNotFound, Body: "missing"}
				},
				queryFn: func(context.Context, int64, string, string, int) ([]loadtest.MetricPoint, error) {
					return nil, nil
				},
			},
			loadtest.NewGuard(loadtest.GuardConfig{}),
		)

		handler := handleLoadtestStatus(svc, nil)
		req := httptest.NewRequest(http.MethodGet, "/ops/loadtest/9001", nil)
		req.SetPathValue("run_id", "9001")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("upstream non-404 maps to 502", func(t *testing.T) {
		now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
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
				getFn: func(context.Context, int64) (loadtest.TestRun, error) {
					return loadtest.TestRun{}, &loadtest.APIError{StatusCode: http.StatusBadGateway, Body: "bad gateway"}
				},
				queryFn: func(context.Context, int64, string, string, int) ([]loadtest.MetricPoint, error) {
					return nil, nil
				},
			},
			loadtest.NewGuard(loadtest.GuardConfig{}),
		)

		handler := handleLoadtestStatus(svc, nil)
		req := httptest.NewRequest(http.MethodGet, "/ops/loadtest/9002", nil)
		req.SetPathValue("run_id", "9002")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadGateway {
			t.Fatalf("expected 502, got %d body=%s", rec.Code, rec.Body.String())
		}
	})
}
