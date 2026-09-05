package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// requestWithOrigin is duplicated from cmd/producer/cors_test.go: the router
// tests left behind there need it too, and the two files are now in different
// packages, so neither can borrow the other's copy.
func requestWithOrigin(method, path, origin string) *http.Request {
	req := httptest.NewRequest(method, "http://api.example.com"+path, nil)
	req.Host = "api.example.com"
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	return req
}

func TestLoadAllowedOriginsPrefersCORSEnvOverDeprecatedName(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "https://new.example.com")
	t.Setenv("LOADTEST_ALLOWED_ORIGINS", "https://old.example.com")

	allowed := LoadAllowedOrigins()
	if _, ok := allowed["https://new.example.com:443"]; !ok {
		t.Errorf("CORS_ALLOWED_ORIGINS should win, got %v", allowed)
	}
	if _, ok := allowed["https://old.example.com:443"]; ok {
		t.Errorf("deprecated name should be ignored when the new one is set, got %v", allowed)
	}
}

func TestLoadAllowedOriginsFallsBackToDeprecatedName(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "")
	t.Setenv("LOADTEST_ALLOWED_ORIGINS", "https://old.example.com")

	allowed := LoadAllowedOrigins()
	if _, ok := allowed["https://old.example.com:443"]; !ok {
		t.Errorf("existing deployments on the old name must keep working, got %v", allowed)
	}
}

func TestLoadAllowedOriginsUnsetAllowsAll(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "")
	t.Setenv("LOADTEST_ALLOWED_ORIGINS", "")

	// Asked through AllowsAll rather than by reading the map: that is the
	// path main.go takes to decide whether to log the trust-everything warning.
	if !AllowsAll(LoadAllowedOrigins()) {
		t.Error("unset config should keep the zero-config allow-all demo behaviour")
	}
}

// WebSocket handshakes bypass CORS entirely, so the upgrader has to be
// handed the same policy or /ws stays open when the REST API is locked.
func TestOriginCheckerMatchesHTTPPolicy(t *testing.T) {
	check := OriginChecker(parseAllowedOrigins("https://app.example.com"))

	if !check(requestWithOrigin(http.MethodGet, "/ws", "https://app.example.com")) {
		t.Error("trusted origin should be allowed to upgrade")
	}
	if check(requestWithOrigin(http.MethodGet, "/ws", "https://evil.example.com")) {
		t.Error("untrusted origin must not be allowed to upgrade")
	}
	if !check(requestWithOrigin(http.MethodGet, "/ws", "")) {
		t.Error("non-browser client without Origin should be allowed")
	}
}

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

func TestParseAllowedOrigins_AllowAllWhenEmpty(t *testing.T) {
	allowed := parseAllowedOrigins("")

	if len(allowed) != 1 {
		t.Fatalf("expected only wildcard marker, got %d entries", len(allowed))
	}
	if !AllowsAll(allowed) {
		t.Fatalf("expected wildcard allow-all marker for empty config")
	}
}

func TestParseAllowedOrigins_AllowAllMarker(t *testing.T) {
	allowed := parseAllowedOrigins("*, https://app.example.com")

	if !AllowsAll(allowed) {
		t.Fatalf("expected wildcard allow-all marker to be present")
	}
	if _, ok := allowed["https://app.example.com:443"]; !ok {
		t.Fatalf("expected explicit trusted origin to remain present")
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

func TestIsRequestOriginAllowedAllowAll(t *testing.T) {
	for _, raw := range []string{"*", ""} {
		t.Run("raw="+raw, func(t *testing.T) {
			allowed := parseAllowedOrigins(raw)

			req := httptest.NewRequest(http.MethodGet, "http://api.example.com/ops/loadtest/latest", nil)
			req.Host = "api.example.com"
			req.Header.Set("Origin", "https://anywhere.example.com")

			if !isRequestOriginAllowed(req, allowed) {
				t.Fatalf("expected wildcard allow-all to trust any origin")
			}
		})
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
