package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseAllowedOriginsNormalizesAndSkipsInvalid(t *testing.T) {
	allowed := parseAllowedOrigins("https://app.example.com, http://LOCALHOST:3000, bad-value, https://app.example.com:443")

	if len(allowed) != 2 {
		t.Fatalf("expected 2 valid origins, got %d", len(allowed))
	}

	if _, ok := allowed["https://app.example.com:443"]; !ok {
		t.Fatalf("expected normalized https origin in allow-list")
	}
	if _, ok := allowed["http://localhost:3000"]; !ok {
		t.Fatalf("expected normalized localhost origin in allow-list")
	}
}

func TestIsRequestOriginAllowedSameOrigin(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://api.example.com/health", nil)
	req.Host = "api.example.com"
	req.Header.Set("Origin", "http://api.example.com")

	if !isRequestOriginAllowed(req, nil) {
		t.Fatalf("expected same-origin request to be allowed")
	}

	proxied := httptest.NewRequest(http.MethodGet, "http://internal/health", nil)
	proxied.Host = "api.example.com"
	proxied.Header.Set("X-Forwarded-Proto", "https")
	proxied.Header.Set("Origin", "https://api.example.com")

	if !isRequestOriginAllowed(proxied, nil) {
		t.Fatalf("expected same-origin request behind proxy to be allowed")
	}
}

func TestIsRequestOriginAllowedAllowList(t *testing.T) {
	allowed := parseAllowedOrigins("https://app.example.com")

	req := httptest.NewRequest(http.MethodGet, "http://api.example.com/ops/loadtest/latest", nil)
	req.Host = "api.example.com"
	req.Header.Set("Origin", "https://app.example.com")

	if !isRequestOriginAllowed(req, allowed) {
		t.Fatalf("expected trusted cross-origin request to be allowed")
	}

	req.Header.Set("Origin", "https://evil.example.com")
	if isRequestOriginAllowed(req, allowed) {
		t.Fatalf("expected untrusted cross-origin request to be denied")
	}
}

func TestWithCORSPreflightTrustedOrigin(t *testing.T) {
	allowed := parseAllowedOrigins("https://app.example.com")
	called := false
	h := withCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}), allowed)

	req := httptest.NewRequest(http.MethodOptions, "http://api.example.com/ops/loadtest/start", nil)
	req.Host = "api.example.com"
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for preflight, got %d", rr.Code)
	}
	if called {
		t.Fatalf("expected preflight request to be handled by middleware")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Fatalf("unexpected Access-Control-Allow-Origin: %q", got)
	}
}

func TestWithCORSRejectsUntrustedOrigin(t *testing.T) {
	allowed := parseAllowedOrigins("https://app.example.com")
	called := false
	h := withCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}), allowed)

	req := httptest.NewRequest(http.MethodGet, "http://api.example.com/ops/loadtest/latest", nil)
	req.Host = "api.example.com"
	req.Header.Set("Origin", "https://evil.example.com")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for untrusted origin, got %d", rr.Code)
	}
	if called {
		t.Fatalf("expected middleware to block handler invocation")
	}
}
