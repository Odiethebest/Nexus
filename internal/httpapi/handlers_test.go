package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// These tests cover only the branches that return before any external
// dependency is touched. handleListNotifications and handleMetricsSummary have
// no such branch — every path dereferences the cache or the hub — so they are
// deliberately absent rather than exercised with a nil dependency.

type fakePublisher struct {
	msgID string
	err   error
}

func (f fakePublisher) Publish(_ context.Context, _, _ string, _ map[string]any) (string, error) {
	return f.msgID, f.err
}

func TestHandlePublish(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		pub      fakePublisher
		wantCode int
		wantBody string // substring; empty means not checked
	}{
		{name: "malformed json", body: "{", pub: fakePublisher{msgID: "x"}, wantCode: http.StatusBadRequest},
		{name: "empty body", body: "", pub: fakePublisher{msgID: "x"}, wantCode: http.StatusBadRequest},
		{name: "missing type", body: `{"priority":"high"}`, pub: fakePublisher{msgID: "x"}, wantCode: http.StatusBadRequest},
		{name: "missing priority", body: `{"type":"payment.completed"}`, pub: fakePublisher{msgID: "x"}, wantCode: http.StatusBadRequest},
		{
			name:     "accepted",
			body:     `{"type":"payment.completed","priority":"high"}`,
			pub:      fakePublisher{msgID: "abc-123"},
			wantCode: http.StatusAccepted,
			wantBody: `"message_id":"abc-123"`,
		},
		{
			name:     "publisher failure",
			body:     `{"type":"payment.completed","priority":"high"}`,
			pub:      fakePublisher{err: errors.New("broker down")},
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(tt.body))
			rr := httptest.NewRecorder()
			handlePublish(tt.pub)(rr, req)

			if rr.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d (body %q)", rr.Code, tt.wantCode, rr.Body.String())
			}
			if tt.wantBody != "" && !strings.Contains(rr.Body.String(), tt.wantBody) {
				t.Errorf("body = %q, want it to contain %q", rr.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandleGetNotificationRejectsBlankID(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{name: "empty", id: ""},
		{name: "single space", id: " "},
		{name: "tab", id: "\t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/notifications/x", nil)
			req.SetPathValue("message_id", tt.id)
			rr := httptest.NewRecorder()
			// The cache is nil on purpose: a blank id must never reach it.
			handleGetNotification(nil)(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rr.Code)
			}
		})
	}
}

func TestHandleReplay(t *testing.T) {
	// The queue names that reach the Replayer below are not DLQ topics, so
	// Replayer.Replay returns an error before it touches any field — which is
	// what lets these cases run against a nil *replay.Replayer without
	// panicking. That safety is not local to this file: it rests on the
	// statement order in internal/replay/kafka.go, where NormalizeDLQTopic and
	// PrimaryFromDLQ run first and the receiver's cfg is only read afterwards.
	// Reordering those would turn every 500 case here into a nil-pointer panic.
	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{name: "malformed json", body: "{", wantCode: http.StatusBadRequest},
		{name: "empty body", body: "", wantCode: http.StatusBadRequest},
		{name: "missing queue", body: `{"max":10}`, wantCode: http.StatusBadRequest},
		// Whitespace is NOT blank here: handleReplay tests body.Queue == ""
		// with no TrimSpace, while handleGetNotification tests
		// strings.TrimSpace(id) == "". That asymmetry is existing behaviour, so
		// "   " gets through the guard and reaches the Replayer — hence 500,
		// not 400. Adding a TrimSpace to match would be a behaviour change.
		{name: "whitespace queue is not treated as blank", body: `{"queue":"   "}`, wantCode: http.StatusInternalServerError},
		// Out-of-range Max is silently clamped to 100, not rejected. What is
		// observable from here is only that it is not a 400 — the clamped value
		// itself is not visible with a nil Replayer. Reintroducing a 400 for
		// these inputs would break the two cases below.
		{name: "max zero is clamped not rejected", body: `{"queue":"not-a-dlq-topic","max":0}`, wantCode: http.StatusInternalServerError},
		{name: "max over limit is clamped not rejected", body: `{"queue":"not-a-dlq-topic","max":5000}`, wantCode: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/dlq/replay", strings.NewReader(tt.body))
			rr := httptest.NewRecorder()
			handleReplay(nil)(rr, req)

			if rr.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d (body %q)", rr.Code, tt.wantCode, rr.Body.String())
			}
		})
	}
}

func TestHandleClearNotificationsRejectsMalformedBody(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "truncated object", body: "{"},
		{name: "not json", body: "definitely not json"},
		{name: "wrong type for field", body: `{"before_unix_ms":"yesterday"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/notifications/clear", strings.NewReader(tt.body))
			rr := httptest.NewRecorder()
			// The store is nil on purpose: a malformed body must never reach it.
			handleClearNotifications(nil)(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rr.Code)
			}
		})
	}
}
