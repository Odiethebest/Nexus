package loadtest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestClient_RetryWithJitter_SucceedsAfterTransientFailures(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n <= 2 {
			http.Error(w, "temporary upstream failure", http.StatusBadGateway)
			return
		}
		writeRunJSON(t, w, map[string]any{
			"id":      7001,
			"test_id": 1234,
			"status":  "created",
			"created": "2026-04-01T12:00:00Z",
		})
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:                        server.URL,
		APIToken:                       "token",
		StackID:                        "42",
		RetryMaxAttempts:               3,
		RetryBaseDelay:                 5 * time.Millisecond,
		RetryMaxDelay:                  20 * time.Millisecond,
		CircuitBreakerFailureThreshold: 10,
		CircuitBreakerOpenDuration:     50 * time.Millisecond,
		RandSeed:                       1,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	run, err := client.StartLoadTest(context.Background(), 1234, StartOptions{Note: "retry"})
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if run.ID != 7001 {
		t.Fatalf("unexpected run id: %d", run.ID)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("expected 3 calls (2 retries), got %d", got)
	}
}

func TestClient_CircuitBreaker_OpensAfterConsecutiveFailures(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Error(w, "upstream down", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:                        server.URL,
		APIToken:                       "token",
		StackID:                        "42",
		RetryMaxAttempts:               0,
		CircuitBreakerFailureThreshold: 1,
		CircuitBreakerOpenDuration:     60 * time.Millisecond,
		RandSeed:                       2,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = client.StartLoadTest(context.Background(), 1234, StartOptions{})
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError on first failure, got %v", err)
	}

	_, err = client.StartLoadTest(context.Background(), 1234, StartOptions{})
	var openErr *CircuitOpenError
	if !errors.As(err, &openErr) {
		t.Fatalf("expected CircuitOpenError on second call, got %v", err)
	}
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected error to unwrap to ErrCircuitOpen, got %v", err)
	}

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected breaker to block network call, calls=%d", got)
	}

	time.Sleep(80 * time.Millisecond)
	_, err = client.StartLoadTest(context.Background(), 1234, StartOptions{})
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError after breaker reopen attempt, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected one more network call after breaker window, calls=%d", got)
	}
}

func writeRunJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode json: %v", err)
	}
}
