package loadtest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClient_StartLoadTest_RequestAndResponseMapping(t *testing.T) {
	var gotMethod string
	var gotPath string
	var gotAuth string
	var gotStackID string
	var gotContentType string
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotStackID = r.Header.Get("X-Stack-Id")
		gotContentType = r.Header.Get("Content-Type")

		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"id":         91001,
			"test_id":    1234,
			"project_id": 88,
			"status":     "created",
			"created":    "2026-04-01T12:00:00Z",
			"note":       "smoke",
			"status_details": map[string]any{
				"type": "created",
				"extra": map[string]any{
					"message": "queued",
				},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:  server.URL,
		APIToken: "token",
		StackID:  "42",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	run, err := client.StartLoadTest(context.Background(), 1234, StartOptions{
		Scenario: "default",
		Preset:   "quick",
		Note:     "smoke",
	})
	if err != nil {
		t.Fatalf("start load test: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Fatalf("expected method POST, got %s", gotMethod)
	}
	if gotPath != "/cloud/v6/load_tests/1234/start" {
		t.Fatalf("unexpected path: %s", gotPath)
	}
	if gotAuth != "Bearer token" {
		t.Fatalf("unexpected Authorization header: %q", gotAuth)
	}
	if gotStackID != "42" {
		t.Fatalf("unexpected X-Stack-Id: %q", gotStackID)
	}
	if !strings.Contains(gotContentType, "application/json") {
		t.Fatalf("expected JSON content-type, got %q", gotContentType)
	}

	if len(gotBody) != 1 {
		t.Fatalf("expected conservative payload with only note, got %+v", gotBody)
	}
	if gotBody["note"] != "smoke" {
		t.Fatalf("expected note=smoke, got %+v", gotBody)
	}

	if run.ID != 91001 || run.TestID != 1234 {
		t.Fatalf("unexpected run ids: %+v", run)
	}
	if run.Status != StatusCreated {
		t.Fatalf("unexpected status: %s", run.Status)
	}
	if run.Note != "smoke" {
		t.Fatalf("unexpected note: %q", run.Note)
	}
	if run.StatusDetails.Type != StatusCreated {
		t.Fatalf("unexpected status_details.type: %s", run.StatusDetails.Type)
	}
	if run.StatusDetails.Extra == nil || run.StatusDetails.Extra.Message != "queued" {
		t.Fatalf("unexpected status_details.extra: %+v", run.StatusDetails.Extra)
	}
}

func TestClient_GetTestRun_StatusDetailsFallbackToStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"id":             92001,
			"test_id":        1234,
			"status":         "running",
			"created":        "2026-04-01T12:00:00Z",
			"status_details": map[string]any{},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:  server.URL,
		APIToken: "token",
		StackID:  "42",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	run, err := client.GetTestRun(context.Background(), 92001)
	if err != nil {
		t.Fatalf("get test run: %v", err)
	}
	if run.Status != StatusRunning {
		t.Fatalf("unexpected status: %s", run.Status)
	}
	if run.StatusDetails.Type != StatusRunning {
		t.Fatalf("expected status_details.type fallback to %s, got %s", StatusRunning, run.StatusDetails.Type)
	}
}

func TestClient_QueryRangeK6_MergesAndSortsSeries(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "matrix",
				"result": []map[string]any{
					{
						"metric": map[string]any{"instance": "a"},
						"values": [][]any{
							{1712011205.0, "200"},
							{1712011200.0, "100.5"},
						},
					},
					{
						"metric": map[string]any{"instance": "b"},
						"values": [][]any{
							{1712011200.0, "50"},
							{1712011210.0, 75},
						},
					},
				},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:  server.URL,
		APIToken: "token",
		StackID:  "42",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	points, err := client.QueryRangeK6(context.Background(), 93001, "http_reqs", "rate", 3)
	if err != nil {
		t.Fatalf("query range: %v", err)
	}

	wantPath := "/cloud/v5/test_runs/93001/query_range_k6(metric=%27http_reqs%27,query=%27rate%27,step=3)"
	if gotPath != wantPath {
		t.Fatalf("unexpected request path: got %q want %q", gotPath, wantPath)
	}

	if len(points) != 3 {
		t.Fatalf("expected 3 merged points, got %d", len(points))
	}
	if points[0].Timestamp.Unix() != 1712011200 || points[0].Value != 150.5 {
		t.Fatalf("unexpected first point: %+v", points[0])
	}
	if points[1].Timestamp.Unix() != 1712011205 || points[1].Value != 200 {
		t.Fatalf("unexpected second point: %+v", points[1])
	}
	if points[2].Timestamp.Unix() != 1712011210 || points[2].Value != 75 {
		t.Fatalf("unexpected third point: %+v", points[2])
	}
}
