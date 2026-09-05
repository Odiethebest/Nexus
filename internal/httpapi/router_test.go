package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"nexus/internal/hub"
	"nexus/internal/notifcache"
	"nexus/internal/store"
)

// This file is the mechanical check on the HTTP surface: every route below is
// probed through the router NewRouter actually builds, using that route's own
// early-return response as evidence that it is registered and wired to the
// handler it is supposed to be wired to.
//
// Why not probe with a wrong method and assert 405: that assertion is
// vacuously true here. "GET /" is a catch-all prefix pattern, so an
// unregistered POST path also answers 405 and an unregistered GET path answers
// 200 with "nexus producer" rather than 404. A 405 therefore proves nothing
// about registration. That is also why the /health case asserts an empty body
// — a 200 alone would not distinguish /health from the catch-all.
//
// All thirteen routes NewRouter registers are probed here. The three that once
// had no early-return branch are reached instead by giving the router enough
// of a dependency to answer: a real hub for /ws and /api/metrics/summary, and
// a cache primed with a list entry for /notifications.
//
// What this file still does NOT cover, stated plainly rather than papered over:
//   - Swapping two routes whose handlers answer with the same status and the
//     same body would not be caught. Every probe below asserts something only
//     its own handler produces — a status, a body fragment, or an empty body —
//     so the pairs that exist today are distinguishable, but nothing enforces
//     that a future route keeps that property.
//   - The probes prove a route is registered and reaches the intended handler.
//     They do not exercise the handler beyond its first branch.

type stubPublisher struct{}

func (stubPublisher) Publish(_ context.Context, _, _ string, _ map[string]any) (string, error) {
	return "stub-msg-id", nil
}

// newProbeRouter builds the real router with the least dependency each route
// needs to answer. Store and Replayer stay nil — every probe that reaches them
// returns first — while Hub and Cache are real because /ws,
// /api/metrics/summary and /notifications have no early return.
//
// The Cache is handed a nil *store.Store on purpose. notifcache.Cache only
// reaches its store on a cache miss: ListNotifications returns from the Redis
// hit before it touches c.store. The probe below primes that key so the hit
// path is the one taken. That makes this test depend on a property of another
// package — if notifcache ever consults the store before serving a hit, the
// /notifications probe turns from a 200 into a nil-pointer panic.
func newProbeRouter(t *testing.T) http.Handler {
	t.Helper()

	// Without this the /api/metrics/summary probe is not hermetic. That handler
	// reaches metrics.ComputeSummary, which merges in a scrape of
	// METRICS_INTERNAL_URL and falls back to http://localhost:9091/metrics when
	// the variable is unset — which is the worker's real metrics port. On a
	// machine running the worker locally this unit test would scrape it, and a
	// host that accepts the connection without answering costs the 2s client
	// timeout in metrics/summary.go. Port 1 is reserved and never listening, so
	// the scrape fails immediately. The assertion is unaffected either way:
	// publish_rate_per_sec comes from the producer's own registry.
	t.Setenv("METRICS_INTERNAL_URL", "http://127.0.0.1:1/metrics")

	return NewRouter(Deps{
		Publisher:      stubPublisher{},
		Hub:            hub.New(),
		Cache:          newProbeCache(t),
		LatestRun:      &atomic.Int64{},
		AllowedOrigins: parseAllowedOrigins(""),
	})
}

// newProbeCache returns a cache primed with the list entry the /notifications
// probe expects, backed by miniredis and a nil store (see above).
func newProbeCache(t *testing.T) *notifcache.Cache {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	cached, err := json.Marshal([]store.Notification{{
		MessageID: "probe-msg-id",
		Channel:   "inapp",
		EventType: "probe.event",
		Status:    "delivered",
		Priority:  "normal",
	}})
	if err != nil {
		t.Fatalf("marshal cached notifications: %v", err)
	}
	// Key format and limit both come from the handler under test:
	// handleListNotifications calls ListNotifications(ctx, 50), which reads
	// "cache:notif:list:v2:50". A typo here misses the cache and the nil store
	// panics instead of failing an assertion.
	mr.Set("cache:notif:list:v2:50", string(cached))

	return notifcache.New(rdb, nil)
}

// probe is one request and what its answer must look like. The struct is named
// and the table is a function so that TestProbesDetectSwappedRoutes can run the
// same probes against a deliberately miswired router.
type probe struct {
	name     string
	method   string
	path     string
	body     string
	wantCode int
	wantBody string // substring; empty means not checked
	emptyOK  bool   // assert the body is empty instead
}

// runProbe returns "" when the probe holds, or a description of what went
// wrong. It deliberately does not take *testing.T: a caller has to be able to
// ask whether a probe fails without that failing the test.
func runProbe(h http.Handler, p probe) string {
	var req *http.Request
	if p.body == "" {
		req = httptest.NewRequest(p.method, p.path, nil)
	} else {
		req = httptest.NewRequest(p.method, p.path, strings.NewReader(p.body))
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != p.wantCode {
		return fmt.Sprintf("%s: status = %d, want %d (body %q)", p.name, rr.Code, p.wantCode, rr.Body.String())
	}
	if p.emptyOK && rr.Body.Len() != 0 {
		return fmt.Sprintf("%s: body = %q, want empty", p.name, rr.Body.String())
	}
	if p.wantBody != "" && !strings.Contains(rr.Body.String(), p.wantBody) {
		return fmt.Sprintf("%s: body = %q, want it to contain %q", p.name, rr.Body.String(), p.wantBody)
	}
	return ""
}

// runProbes returns a description for every probe that did not hold.
func runProbes(h http.Handler, probes []probe) []string {
	var failures []string
	for _, p := range probes {
		if msg := runProbe(h, p); msg != "" {
			failures = append(failures, msg)
		}
	}
	return failures
}

// probesForPaths selects table entries by request path, so the mutation test
// below names the routes it miswires instead of duplicating their expectations.
func probesForPaths(paths ...string) []probe {
	wanted := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		wanted[p] = struct{}{}
	}
	var out []probe
	for _, p := range probeTable() {
		if _, ok := wanted[p.path]; ok {
			out = append(out, p)
		}
	}
	return out
}

func probeTable() []probe {
	return []probe{
		{
			name: "POST /events reaches the publish handler", method: http.MethodPost,
			path: "/events", body: "{}", wantCode: http.StatusBadRequest,
			wantBody: "type and priority are required",
		},
		{
			name: "POST /dlq/replay reaches the replay handler", method: http.MethodPost,
			path: "/dlq/replay", body: "{}", wantCode: http.StatusBadRequest,
			wantBody: "queue is required",
		},
		{
			name: "POST /notifications/clear reaches the clear handler", method: http.MethodPost,
			path: "/notifications/clear", body: "not json", wantCode: http.StatusBadRequest,
			wantBody: "invalid request body",
		},
		{
			name: "GET /notifications/{message_id} reaches the by-id handler", method: http.MethodGet,
			path: "/notifications/%20", wantCode: http.StatusBadRequest,
		},
		{
			name: "POST /ops/loadtest/start reaches the start handler", method: http.MethodPost,
			path: "/ops/loadtest/start", body: "{}", wantCode: http.StatusServiceUnavailable,
		},
		{
			name: "GET /ops/loadtest/{run_id} reaches the status handler", method: http.MethodGet,
			path: "/ops/loadtest/0", wantCode: http.StatusBadRequest, wantBody: "invalid run_id",
		},
		{
			name: "GET /ops/loadtest/latest reaches the latest handler", method: http.MethodGet,
			path: "/ops/loadtest/latest", wantCode: http.StatusNotFound, wantBody: "no loadtest run recorded",
		},
		{
			// An empty body is what separates /health from the "GET /"
			// catch-all, which answers 200 with "nexus producer".
			name: "GET /health is its own route, not the catch-all", method: http.MethodGet,
			path: "/health", wantCode: http.StatusOK, emptyOK: true,
		},
		{
			// Asserting "# HELP" would be vacuous: the Go runtime collector is
			// always registered, so that string appears even if nexus's own
			// metrics were never linked in. nexus/internal/metrics registers
			// through an init() side effect, which the compiler cannot prove is
			// reachable — drop the last import of that package and the binary
			// still builds, /metrics still answers 200, and every nexus_* series
			// silently disappears. Naming one of them is what catches that.
			//
			// It has to be a plain Gauge: a CounterVec like
			// nexus_messages_processed_total does not appear until some label
			// combination has been used, so it would fail here for a reason
			// that has nothing to do with registration.
			name: "GET /metrics exposes nexus's own collectors", method: http.MethodGet,
			path: "/metrics", wantCode: http.StatusOK, wantBody: "nexus_loadtest_active_runs",
		},
		{
			// gorilla refuses a handshake that carries no upgrade headers, so a
			// plain GET is enough to prove /ws reaches the hub rather than the
			// "GET /" catch-all, which would answer 200.
			name: "GET /ws reaches the websocket upgrader", method: http.MethodGet,
			path: "/ws", wantCode: http.StatusBadRequest, wantBody: "Bad Request",
		},
		{
			name: "GET /api/metrics/summary reaches the summary handler", method: http.MethodGet,
			path: "/api/metrics/summary", wantCode: http.StatusOK, wantBody: `"publish_rate_per_sec"`,
		},
		{
			// Served from the primed cache entry; see newProbeRouter.
			name: "GET /notifications reaches the list handler", method: http.MethodGet,
			path: "/notifications", wantCode: http.StatusOK, wantBody: `"probe-msg-id"`,
		},
	}
}

func TestRouterRegistersEveryProbeableRoute(t *testing.T) {
	for _, p := range probeTable() {
		t.Run(p.name, func(t *testing.T) {
			if msg := runProbe(newProbeRouter(t), p); msg != "" {
				t.Error(msg)
			}
		})
	}
}

// TestRouterCatchAllStillAnswers pins the behaviour the probes above are
// written around: an unregistered GET path is absorbed by "GET /", so absence
// of a route does not show up as a 404.
func TestRouterCatchAllStillAnswers(t *testing.T) {
	h := newProbeRouter(t)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/totally-unregistered", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 from the catch-all", rr.Code)
	}
	if got := rr.Body.String(); got != "nexus producer" {
		t.Errorf("body = %q, want %q", got, "nexus producer")
	}
}

// The tests below exercise the handler NewRouter actually returns. The
// isolated withCORS tests in internal/httpapi/cors_test.go all passed while
// the server was serving an unconditional Access-Control-Allow-Origin: * — the
// allow-list was built in main and then never attached. Asserting on the composed
// router is what closes that gap.
//
// /health is used as the probe because it touches none of the Deps fields,
// so the rest can stay nil.

func newTestRouter(originsEnv string) http.Handler {
	return NewRouter(Deps{AllowedOrigins: parseAllowedOrigins(originsEnv)})
}

func TestRouterEnforcesAllowListOnEveryRoute(t *testing.T) {
	h := newTestRouter("https://app.example.com")

	t.Run("trusted origin is echoed, never wildcarded", func(t *testing.T) {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, requestWithOrigin(http.MethodGet, "/health", "https://app.example.com"))

		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rr.Code)
		}
		if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
			t.Errorf("Access-Control-Allow-Origin = %q, want the exact origin", got)
		}
		if got := rr.Header().Get("Vary"); !strings.Contains(got, "Origin") {
			t.Errorf("Vary = %q, want it to include Origin (responses differ per origin)", got)
		}
	})

	t.Run("untrusted origin is refused", func(t *testing.T) {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, requestWithOrigin(http.MethodGet, "/health", "https://evil.example.com"))

		if rr.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", rr.Code)
		}
		if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("Access-Control-Allow-Origin = %q, want empty for an untrusted origin", got)
		}
	})

	t.Run("no Origin header passes through", func(t *testing.T) {
		// curl, the in-repo loadgen, Prometheus scrapes, server-to-server.
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, requestWithOrigin(http.MethodGet, "/health", ""))

		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 — non-browser clients must not be blocked", rr.Code)
		}
	})

	t.Run("preflight advertises the admin-key header", func(t *testing.T) {
		// POST /ops/loadtest/start reads X-Admin-Key. The old allow-all
		// middleware only advertised Content-Type, so that preflight failed.
		req := requestWithOrigin(http.MethodOptions, "/ops/loadtest/start", "https://app.example.com")
		req.Header.Set("Access-Control-Request-Method", "POST")
		req.Header.Set("Access-Control-Request-Headers", "X-Admin-Key")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)

		if rr.Code != http.StatusNoContent {
			t.Fatalf("preflight status = %d, want 204", rr.Code)
		}
		if got := rr.Header().Get("Access-Control-Allow-Headers"); !strings.Contains(got, "X-Admin-Key") {
			t.Errorf("Access-Control-Allow-Headers = %q, want it to include X-Admin-Key", got)
		}
	})
}

func TestRouterDefaultsToAllowAll(t *testing.T) {
	// Zero-config demo behaviour must survive the rewiring.
	h := newTestRouter("")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, requestWithOrigin(http.MethodGet, "/health", "https://anywhere.example.com"))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 with no allow-list configured", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://anywhere.example.com" {
		t.Errorf("Access-Control-Allow-Origin = %q, want the caller's origin echoed", got)
	}
}

// TestProbesDetectSwappedRoutes is the check on the checks. The probe table
// above is only worth having if a miswired router makes it fail, and nothing
// else in the repository enforces that: someone can weaken an assertion and
// every test still passes. So this test miswires a router on purpose and
// requires the probes to notice.
//
// Read the direction carefully — this test passing means the probes have
// teeth. If it ever starts failing, the probe table has lost its ability to
// tell a correct router from an incorrect one, and that is the defect.
//
// Each case registers only the pair it crosses rather than a copy of all
// thirteen routes; a copy would drift as NewRouter changes.
//
// It asserts that *every* selected probe fails, not merely that some do.
// "At least one failure" would stay true after deleting one of the two
// wantBody assertions, which is exactly the erosion this test exists to catch.
func TestProbesDetectSwappedRoutes(t *testing.T) {
	tests := []struct {
		name  string
		paths []string
		build func(*testing.T) http.Handler
	}{
		{
			// Detected on status alone: a list handler reached through the
			// {message_id} route answers 200, and a by-id handler reached
			// without one answers 400.
			name:  "notifications list and by-id crossed",
			paths: []string{"/notifications", "/notifications/%20"},
			build: func(t *testing.T) http.Handler {
				c := newProbeCache(t)
				mux := http.NewServeMux()
				mux.HandleFunc("GET /notifications/{message_id}", handleListNotifications(c))
				mux.HandleFunc("GET /notifications", handleGetNotification(c))
				return mux
			},
		},
		{
			// Detected on body alone. Both handlers answer 400 for an empty
			// JSON object, so wantCode cannot tell them apart; only the
			// wantBody strings can. This is the case that proves the body
			// assertions are load-bearing rather than decorative.
			name:  "events and dlq replay crossed",
			paths: []string{"/events", "/dlq/replay"},
			build: func(*testing.T) http.Handler {
				mux := http.NewServeMux()
				mux.HandleFunc("POST /events", handleReplay(nil))
				mux.HandleFunc("POST /dlq/replay", handlePublish(stubPublisher{}))
				return mux
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probes := probesForPaths(tt.paths...)
			if len(probes) != len(tt.paths) {
				t.Fatalf("selected %d probes for %v, want %d — the table no longer covers these paths",
					len(probes), tt.paths, len(tt.paths))
			}

			failures := runProbes(tt.build(t), probes)
			if len(failures) != len(probes) {
				t.Errorf("miswired router produced %d/%d probe failures; every probe should have caught it.\ngot: %v",
					len(failures), len(probes), failures)
			}
		})
	}
}
