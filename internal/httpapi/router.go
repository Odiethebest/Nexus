package httpapi

import (
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"nexus/internal/hub"
	"nexus/internal/loadtest"
	"nexus/internal/notifcache"
	"nexus/internal/replay"
	"nexus/internal/store"
)

// Deps is everything the HTTP surface needs. Grouped into a struct so
// NewRouter can be exercised in tests without standing up Kafka, PostgreSQL
// and Redis first.
type Deps struct {
	Publisher      EventPublisher
	Cache          *notifcache.Cache
	Store          *store.Store
	Replayer       *replay.Replayer
	Hub            *hub.Hub
	Loadtest       *loadtest.Service
	DemoLoadtest   *loadtest.DemoService
	LatestRun      *atomic.Int64
	AllowedOrigins map[string]struct{}
}

// NewRouter registers every route and returns it already wrapped in the
// origin policy. The wrap lives here, not at the call site, because the
// previous arrangement — build an allow-list in main, then wrap with an
// unrelated allow-all middleware — is exactly how the policy came to be
// silently bypassed.
func NewRouter(d Deps) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", handlePublish(d.Publisher))
	mux.HandleFunc("GET /notifications/{message_id}", handleGetNotification(d.Cache))
	mux.HandleFunc("GET /notifications", handleListNotifications(d.Cache))
	mux.HandleFunc("POST /notifications/clear", handleClearNotifications(d.Store))
	mux.HandleFunc("POST /dlq/replay", handleReplay(d.Replayer))
	mux.HandleFunc("POST /ops/loadtest/start", handleLoadtestStart(d.Loadtest, d.DemoLoadtest, d.LatestRun))
	mux.HandleFunc("GET /ops/loadtest/{run_id}", handleLoadtestStatus(d.Loadtest, d.DemoLoadtest))
	mux.HandleFunc("GET /ops/loadtest/latest", handleLoadtestLatest(d.Loadtest, d.DemoLoadtest, d.LatestRun))
	mux.HandleFunc("GET /ws", d.Hub.ServeWS)
	mux.HandleFunc("GET /api/metrics/summary", handleMetricsSummary(d.Hub))
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("nexus producer"))
	})

	return withCORS(mux, d.AllowedOrigins)
}
