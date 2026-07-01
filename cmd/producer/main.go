package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"nexus/internal/envutil"
	"nexus/internal/grpcserver"
	"nexus/internal/hub"
	"nexus/internal/kbroker"
	"nexus/internal/loadtest"
	"nexus/internal/metrics"
	"nexus/internal/notifcache"
	"nexus/internal/replay"
	"nexus/internal/store"
)

// eventPublisher is the local narrowing of what handlers need from a
// publisher. Both broker.Publisher (AMQP) and kbroker.Publisher (Kafka)
// satisfy it, so the USE_KAFKA feature flag can flip between them without
// touching handler code.
type eventPublisher interface {
	Publish(ctx context.Context, eventType, priority string, payload map[string]any) (string, error)
}

const (
	defaultLoadtestRequestTimeout = 20 * time.Second
	minLoadtestRequestTimeout     = 5 * time.Second
	maxLoadtestRequestTimeout     = 30 * time.Second
	defaultLoadtestStatusTimeout  = 4 * time.Second
	defaultLoadtestQueryTimeout   = 3 * time.Second
	corsAllowAllMarker            = "*"
	loadtestModeDemo              = "demo"
	loadtestModeReal              = "real"
)

func main() {
	if path, err := envutil.LoadDotEnvIfPresent(); err != nil {
		slog.Warn("failed to auto-load .env", "err", err)
	} else if path != "" {
		slog.Info("loaded environment file", "path", path)
	}

	pgDSN := getenv("POSTGRES_DSN", "postgres://nexus:nexus@localhost:5432/nexus?sslmode=disable")
	redisURL := getenv("REDIS_URL", "redis://localhost:6379")
	listenAddr := getenv("LISTEN_ADDR", ":"+getenv("PORT", "8080"))
	grpcAddr := getenv("GRPC_ADDR", ":50051")

	kcfg, err := kbroker.LoadConfig()
	if err != nil {
		slog.Error("failed to load kafka config", "err", err)
		os.Exit(1)
	}

	st, err := store.New(pgDSN)
	if err != nil {
		slog.Error("failed to connect to store", "err", err)
		os.Exit(1)
	}
	if err := st.Migrate(context.Background()); err != nil {
		slog.Error("migration failed", "err", err)
		os.Exit(1)
	}

	redisOpt, err := redis.ParseURL(redisURL)
	if err != nil {
		slog.Error("invalid redis URL", "err", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(redisOpt)
	defer rdb.Close()
	notifCache := notifcache.New(rdb, st)

	allowedOrigins := parseAllowedOrigins(os.Getenv("LOADTEST_ALLOWED_ORIGINS"))
	slog.Info("trusted request origins configured", "count", len(allowedOrigins))
	// WebSocket accepts all origins — browser clients connect from localhost:3000 in dev
	// and from arbitrary origins in production (behind TLS). CORS for HTTP routes is
	// handled separately by corsMiddleware.
	wsHub := hub.New()

	// Root context so background samplers (lag reader) stop when SIGTERM fires.
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	// Best-effort auto-create topics. Failures are logged but not fatal —
	// operators may pre-create topics on managed clusters where the app
	// lacks admin rights.
	ctxEnsure, cancelEnsure := context.WithTimeout(context.Background(), 10*time.Second)
	if err := kbroker.EnsureTopics(ctxEnsure, kcfg); err != nil {
		slog.Warn("kafka: EnsureTopics failed (continuing)", "err", err)
	}
	cancelEnsure()

	kafkaPub, err := kbroker.NewPublisher(kcfg)
	if err != nil {
		slog.Error("failed to create kafka publisher", "err", err)
		os.Exit(1)
	}
	var pub eventPublisher = kafkaPub
	slog.Info("publisher backend", "impl", "kafka", "brokers", kcfg.Brokers, "partitions", kcfg.TopicPartitions)

	// Lag reader pushes consumer-lag + DLQ gauges into Prometheus every
	// 3s. Runs on the producer so the summary handler can read them
	// from the local registry without cross-service scraping.
	var lagReader *kbroker.LagReader
	if lr, err := kbroker.NewLagReader(kcfg, slog.Default()); err != nil {
		slog.Warn("kafka lag reader disabled", "err", err)
	} else {
		lagReader = lr
		go lagReader.Run(rootCtx, 3*time.Second)
	}

	replayer := replay.New(kcfg, slog.Default())
	loadtestSvc, err := initLoadtestService()
	if err != nil {
		slog.Error("failed to initialize loadtest service", "err", err)
		os.Exit(1)
	}
	loadtestEnabled := getenvBool("LOADTEST_ENABLED", false)
	demoRunDuration := time.Duration(getenvInt("LOADTEST_DEMO_RUN_SECONDS", 55)) * time.Second
	demoLoadtestSvc := loadtest.NewDemoService(loadtest.DemoServiceConfig{
		PollInterval: time.Duration(getenvInt("LOADTEST_POLL_INTERVAL_SECONDS", 2)) * time.Second,
		RunDuration:  demoRunDuration,
	})
	slog.Info("loadtest endpoints configured", "enabled", loadtestEnabled)
	var latestLoadtestRun atomic.Int64

	// ── HTTP server ───────────────────────────────────────────────────────
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", handlePublish(pub))
	mux.HandleFunc("GET /notifications/{message_id}", handleGetNotification(notifCache))
	mux.HandleFunc("GET /notifications", handleListNotifications(notifCache))
	mux.HandleFunc("POST /notifications/clear", handleClearNotifications(st))
	mux.HandleFunc("POST /dlq/replay", handleReplay(replayer))
	mux.HandleFunc("POST /ops/loadtest/start", handleLoadtestStart(loadtestSvc, demoLoadtestSvc, &latestLoadtestRun))
	mux.HandleFunc("GET /ops/loadtest/{run_id}", handleLoadtestStatus(loadtestSvc, demoLoadtestSvc))
	mux.HandleFunc("GET /ops/loadtest/latest", handleLoadtestLatest(loadtestSvc, demoLoadtestSvc, &latestLoadtestRun))
	mux.HandleFunc("GET /ws", wsHub.ServeWS)
	mux.HandleFunc("GET /api/metrics/summary", handleMetricsSummary(wsHub))
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("nexus producer"))
	})

	srv := &http.Server{
		Addr:         listenAddr,
		Handler:      corsMiddleware(mux),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		slog.Info("producer HTTP listening", "addr", listenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "err", err)
			os.Exit(1)
		}
	}()

	// ── gRPC server ───────────────────────────────────────────────────────
	grpcSrv, grpcLis, err := grpcserver.Listen(grpcAddr, pub)
	if err != nil {
		slog.Error("failed to start gRPC server", "err", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("producer gRPC listening", "addr", grpcAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			slog.Error("gRPC server error", "err", err)
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	grpcSrv.GracefulStop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	if kafkaPub != nil {
		if err := kafkaPub.Close(ctx); err != nil {
			slog.Warn("kafka publisher close", "err", err)
		}
	}
	rootCancel()
	if lagReader != nil {
		lagReader.Close()
	}
	slog.Info("producer shut down")
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

type publishRequest struct {
	Type     string         `json:"type"`
	Priority string         `json:"priority"`
	Payload  map[string]any `json:"payload"`
}

type loadtestStartRequest struct {
	Mode     string `json:"mode,omitempty"`
	Scenario string `json:"scenario,omitempty"`
	Preset   string `json:"preset,omitempty"`
	Note     string `json:"note,omitempty"`
}

type loadtestStartResponse struct {
	Mode             string             `json:"mode,omitempty"`
	RunID            int64              `json:"run_id"`
	TestID           int64              `json:"test_id"`
	Status           loadtest.RunStatus `json:"status"`
	StartedAt        time.Time          `json:"started_at"`
	PollAfterSeconds int                `json:"poll_after_seconds"`
}

type loadtestRunEnvelope struct {
	Mode        string             `json:"mode,omitempty"`
	Run         loadtestRunSummary `json:"run"`
	Series      loadtestSeriesJSON `json:"series"`
	Snapshot    any                `json:"snapshot"`
	HealthScore int                `json:"health_score"`
	Signals     []string           `json:"signals"`
	Warnings    []string           `json:"warnings,omitempty"`
}

type loadtestRunSummary struct {
	ID      int64              `json:"id"`
	Status  loadtest.RunStatus `json:"status"`
	Result  *string            `json:"result"`
	Created time.Time          `json:"created"`
	Ended   *time.Time         `json:"ended"`
}

type loadtestSeriesJSON struct {
	RPS          [][]any `json:"rps"`
	P95MS        [][]any `json:"p95_ms"`
	ErrorRatePct [][]any `json:"error_rate_pct"`
	VUs          [][]any `json:"vus"`
}

func handleMetricsSummary(h *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snapshot := metrics.ComputeSummary(h.Count())
		writeJSON(w, http.StatusOK, snapshot)
	}
}

func handlePublish(pub eventPublisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req publishRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if req.Type == "" || req.Priority == "" {
			http.Error(w, "type and priority are required", http.StatusBadRequest)
			return
		}

		msgID, err := pub.Publish(r.Context(), req.Type, req.Priority, req.Payload)
		if err != nil {
			slog.Error("publish failed", "err", err)
			http.Error(w, "publish failed", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"message_id": msgID})
	}
}

func handleListNotifications(c *notifcache.Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		notifications, err := c.ListNotifications(r.Context(), 50)
		if err != nil {
			slog.Error("list notifications failed", "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(notifications)
	}
}

// handleGetNotification is the cache-aside hot path — the one the RUNBOOK
// points at for the 95% by_id hit-rate figure. Repeat lookups of the same
// message_id within TTL come back from Redis; the first read fills the
// cache from PostgreSQL.
func handleGetNotification(c *notifcache.Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("message_id")
		if strings.TrimSpace(id) == "" {
			http.Error(w, "message_id is required", http.StatusBadRequest)
			return
		}
		rows, err := c.GetByMessageID(r.Context(), id)
		if err != nil {
			slog.Error("get notification failed", "msg_id", id, "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if len(rows) == 0 {
			writeJSONError(w, http.StatusNotFound, "notification not found")
			return
		}
		writeJSON(w, http.StatusOK, rows)
	}
}

func handleClearNotifications(st *store.Store) http.HandlerFunc {
	type clearRequest struct {
		BeforeUnixMS int64 `json:"before_unix_ms"`
	}

	type clearResponse struct {
		Cleared      int64 `json:"cleared"`
		BeforeUnixMS int64 `json:"before_unix_ms"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		var req clearRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		cutoff := time.Now().UTC()
		if req.BeforeUnixMS > 0 {
			cutoff = time.UnixMilli(req.BeforeUnixMS).UTC()
		}

		cleared, err := st.ClearNotificationsBefore(r.Context(), cutoff)
		if err != nil {
			slog.Error("clear notifications failed", "before_unix_ms", cutoff.UnixMilli(), "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		writeJSON(w, http.StatusOK, clearResponse{
			Cleared:      cleared,
			BeforeUnixMS: cutoff.UnixMilli(),
		})
	}
}

func handleReplay(r *replay.Replayer) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			Queue string `json:"queue"`
			Max   int    `json:"max"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if body.Queue == "" {
			http.Error(w, "queue is required", http.StatusBadRequest)
			return
		}
		if body.Max <= 0 || body.Max > 1000 {
			body.Max = 100
		}

		n, err := r.Replay(req.Context(), body.Queue, body.Max)
		if err != nil {
			slog.Error("replay failed", "queue", body.Queue, "err", err)
			http.Error(w, "replay failed", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"replayed": n})
	}
}

func handleLoadtestStart(svc *loadtest.Service, demo *loadtest.DemoService, latest *atomic.Int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req loadtestStartRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		opts := loadtest.StartOptions{
			Scenario: req.Scenario,
			Preset:   req.Preset,
			Note:     req.Note,
		}
		mode := normalizeLoadtestMode(req.Mode)

		var (
			started loadtest.StartResult
			err     error
		)

		switch mode {
		case loadtestModeDemo:
			if demo == nil {
				writeJSONError(w, http.StatusServiceUnavailable, "demo loadtest service unavailable")
				return
			}
			started, err = demo.Start(r.Context(), opts)
		default:
			if svc == nil {
				writeJSONError(w, http.StatusServiceUnavailable, "loadtest service unavailable")
				return
			}
			started, err = svc.Start(
				r.Context(),
				r.Header.Get("X-Admin-Key"),
				actorFromRequest(r),
				opts,
			)
		}
		if err != nil {
			metrics.LoadtestStartTotal.WithLabelValues(classifyLoadtestStartOutcome(err)).Inc()
			status, message := mapLoadtestError(err)
			slog.Warn("loadtest start failed", "mode", mode, "status", status, "err", err)
			writeJSONError(w, status, message)
			return
		}
		metrics.LoadtestStartTotal.WithLabelValues("ok").Inc()

		latest.Store(started.RunID)

		pollAfterSeconds := int(started.PollAfter.Seconds())
		if pollAfterSeconds <= 0 {
			pollAfterSeconds = 3
		}

		writeJSON(w, http.StatusAccepted, loadtestStartResponse{
			Mode:             mode,
			RunID:            started.RunID,
			TestID:           started.TestID,
			Status:           started.Status,
			StartedAt:        started.StartedAt,
			PollAfterSeconds: pollAfterSeconds,
		})
	}
}

func handleLoadtestStatus(svc *loadtest.Service, demo *loadtest.DemoService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		runID, err := strconv.ParseInt(r.PathValue("run_id"), 10, 64)
		if err != nil || runID <= 0 {
			writeJSONError(w, http.StatusBadRequest, "invalid run_id")
			return
		}

		modeQuery := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("mode")))
		forceDemo := modeQuery == loadtestModeDemo
		forceReal := modeQuery == loadtestModeReal

		mode := loadtestModeReal
		var insight loadtest.RunInsight
		switch {
		case forceDemo:
			if demo == nil {
				writeJSONError(w, http.StatusServiceUnavailable, "demo loadtest service unavailable")
				return
			}
			mode = loadtestModeDemo
			insight, err = demo.SyncRun(r.Context(), runID)
		case forceReal:
			if svc == nil {
				writeJSONError(w, http.StatusServiceUnavailable, "loadtest service unavailable")
				return
			}
			insight, err = svc.SyncRun(r.Context(), runID)
		case demo != nil && demo.HasRun(runID):
			mode = loadtestModeDemo
			insight, err = demo.SyncRun(r.Context(), runID)
		default:
			if svc == nil {
				writeJSONError(w, http.StatusServiceUnavailable, "loadtest service unavailable")
				return
			}
			insight, err = svc.SyncRun(r.Context(), runID)
		}
		if err != nil {
			status, message := mapLoadtestError(err)
			slog.Warn("loadtest status fetch failed", "run_id", runID, "mode", mode, "status", status, "err", err)
			writeJSONError(w, status, message)
			return
		}

		writeJSON(w, http.StatusOK, toLoadtestRunEnvelope(insight, mode))
	}
}

func handleLoadtestLatest(svc *loadtest.Service, demo *loadtest.DemoService, latest *atomic.Int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		runID := latest.Load()
		if runID <= 0 {
			writeJSONError(w, http.StatusNotFound, "no loadtest run recorded")
			return
		}

		mode := loadtestModeReal
		var (
			insight loadtest.RunInsight
			err     error
		)
		if demo != nil && demo.HasRun(runID) {
			mode = loadtestModeDemo
			insight, err = demo.SyncRun(r.Context(), runID)
		} else {
			if svc == nil {
				writeJSONError(w, http.StatusServiceUnavailable, "loadtest service unavailable")
				return
			}
			insight, err = svc.SyncRun(r.Context(), runID)
		}
		if err != nil {
			status, message := mapLoadtestError(err)
			slog.Warn("loadtest latest fetch failed", "run_id", runID, "mode", mode, "status", status, "err", err)
			writeJSONError(w, status, message)
			return
		}
		writeJSON(w, http.StatusOK, toLoadtestRunEnvelope(insight, mode))
	}
}

func toLoadtestRunEnvelope(in loadtest.RunInsight, mode string) loadtestRunEnvelope {
	return loadtestRunEnvelope{
		Mode: mode,
		Run: loadtestRunSummary{
			ID:      in.Run.ID,
			Status:  in.Run.Status,
			Result:  stringPtrIfNonEmpty(in.Run.Result),
			Created: in.Run.Created,
			Ended:   in.Run.Ended,
		},
		Series: loadtestSeriesJSON{
			RPS:          toMetricTuples(in.Series.RPS),
			P95MS:        toMetricTuples(in.Series.P95MS),
			ErrorRatePct: toMetricTuples(in.Series.ErrorRatePct),
			VUs:          toMetricTuples(in.Series.VUs),
		},
		Snapshot:    in.Snapshot,
		HealthScore: in.HealthScore,
		Signals:     in.Signals,
		Warnings:    in.Warnings,
	}
}

func toMetricTuples(points []loadtest.MetricPoint) [][]any {
	if len(points) == 0 {
		return nil
	}
	out := make([][]any, 0, len(points))
	for _, point := range points {
		out = append(out, []any{
			point.Timestamp.Unix(),
			point.Value,
		})
	}
	return out
}

func stringPtrIfNonEmpty(s string) *string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return &s
}

func mapLoadtestError(err error) (int, string) {
	switch {
	case errors.Is(err, loadtest.ErrUnauthorized):
		return http.StatusForbidden, "forbidden"
	case errors.Is(err, loadtest.ErrDisabled):
		return http.StatusServiceUnavailable, "loadtest is disabled"
	case errors.Is(err, loadtest.ErrParallelLimit):
		return http.StatusConflict, "loadtest already running"
	case errors.Is(err, loadtest.ErrCooldown):
		return http.StatusTooManyRequests, err.Error()
	case errors.Is(err, loadtest.ErrThrottled):
		return http.StatusTooManyRequests, err.Error()
	case errors.Is(err, loadtest.ErrBudgetExceeded):
		return http.StatusTooManyRequests, err.Error()
	case errors.Is(err, loadtest.ErrCircuitOpen):
		return http.StatusServiceUnavailable, err.Error()
	default:
		var apiErr *loadtest.APIError
		if errors.As(err, &apiErr) {
			if apiErr.StatusCode == http.StatusNotFound {
				return http.StatusNotFound, "loadtest run not found"
			}
			return http.StatusBadGateway, "upstream loadtest API failed"
		}
		return http.StatusInternalServerError, "internal error"
	}
}

func classifyLoadtestStartOutcome(err error) string {
	switch {
	case errors.Is(err, loadtest.ErrUnauthorized),
		errors.Is(err, loadtest.ErrDisabled),
		errors.Is(err, loadtest.ErrParallelLimit),
		errors.Is(err, loadtest.ErrCooldown),
		errors.Is(err, loadtest.ErrThrottled),
		errors.Is(err, loadtest.ErrBudgetExceeded):
		return "deny"
	default:
		return "error"
	}
}

func normalizeLoadtestMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case loadtestModeDemo:
		return loadtestModeDemo
	default:
		return loadtestModeReal
	}
}

func actorFromRequest(r *http.Request) string {
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		if idx := strings.Index(xff, ","); idx >= 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return xff
	}

	if xRealIP := strings.TrimSpace(r.Header.Get("X-Real-Ip")); xRealIP != "" {
		return xRealIP
	}

	remote := strings.TrimSpace(r.RemoteAddr)
	if host, _, err := net.SplitHostPort(remote); err == nil && host != "" {
		return host
	}
	return remote
}

func initLoadtestService() (*loadtest.Service, error) {
	enabled := getenvBool("LOADTEST_ENABLED", false)
	pollInterval := time.Duration(getenvInt("LOADTEST_POLL_INTERVAL_SECONDS", 2)) * time.Second
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	statusTimeout := time.Duration(getenvInt("LOADTEST_STATUS_TIMEOUT_SECONDS", int(defaultLoadtestStatusTimeout.Seconds()))) * time.Second
	queryTimeout := time.Duration(getenvInt("LOADTEST_QUERY_TIMEOUT_SECONDS", int(defaultLoadtestQueryTimeout.Seconds()))) * time.Second

	serviceCfg := loadtest.ServiceConfig{
		Enabled:              enabled,
		LoadTestID:           getenvInt64("K6_LOAD_TEST_ID", 0),
		PollInterval:         pollInterval,
		DailyVUHCap:          getenvFloat64("LOADTEST_BUDGET_VUH_PER_DAY", 0),
		MaxRunDuration:       time.Duration(getenvInt("LOADTEST_MAX_RUN_SECONDS", 55)) * time.Second,
		StatusRequestTimeout: statusTimeout,
		MetricQueryTimeout:   queryTimeout,
	}

	guardCfg := loadtest.GuardConfig{
		AdminKey:         os.Getenv("LOADTEST_ADMIN_KEY"),
		MaxParallel:      getenvInt("LOADTEST_MAX_PARALLEL", 1),
		Cooldown:         time.Duration(getenvInt("LOADTEST_COOLDOWN_SECONDS", 300)) * time.Second,
		MinStartInterval: time.Duration(getenvInt("LOADTEST_MIN_START_INTERVAL_SECONDS", 0)) * time.Second,
	}

	guard := loadtest.NewGuard(guardCfg)
	if !enabled {
		return loadtest.NewService(serviceCfg, nil, guard), nil
	}
	if strings.TrimSpace(guardCfg.AdminKey) == "" {
		return nil, fmt.Errorf("LOADTEST_ADMIN_KEY must be set when loadtest is enabled")
	}

	timeoutRaw := time.Duration(getenvInt("LOADTEST_REQUEST_TIMEOUT_SECONDS", int(defaultLoadtestRequestTimeout.Seconds()))) * time.Second
	timeout := sanitizeLoadtestRequestTimeout(timeoutRaw)
	if timeout != timeoutRaw {
		slog.Warn("adjusted loadtest request timeout", "requested", timeoutRaw.String(), "effective", timeout.String())
	}

	client, err := loadtest.NewClient(loadtest.ClientConfig{
		BaseURL:  getenv("K6_API_BASE", "https://api.k6.io"),
		APIToken: os.Getenv("K6_API_TOKEN"),
		StackID:  os.Getenv("K6_STACK_ID"),
		HTTPClient: &http.Client{
			Timeout: timeout,
		},
		RetryMaxAttempts:               getenvInt("LOADTEST_UPSTREAM_RETRY_MAX", 2),
		RetryBaseDelay:                 time.Duration(getenvInt("LOADTEST_UPSTREAM_RETRY_BASE_MS", 250)) * time.Millisecond,
		RetryMaxDelay:                  time.Duration(getenvInt("LOADTEST_UPSTREAM_RETRY_MAX_MS", 2000)) * time.Millisecond,
		CircuitBreakerFailureThreshold: getenvInt("LOADTEST_CIRCUIT_BREAKER_THRESHOLD", 5),
		CircuitBreakerOpenDuration:     time.Duration(getenvInt("LOADTEST_CIRCUIT_BREAKER_OPEN_SECONDS", 30)) * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("loadtest client: %w", err)
	}

	if serviceCfg.LoadTestID <= 0 {
		return nil, fmt.Errorf("K6_LOAD_TEST_ID must be > 0 when loadtest is enabled")
	}

	return loadtest.NewService(serviceCfg, client, guard), nil
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withCORS(next http.Handler, allowedOrigins map[string]struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}
		if !isRequestOriginAllowed(r, allowedOrigins) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}

		w.Header().Add("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Admin-Key")
		w.Header().Set("Access-Control-Max-Age", "600")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func parseAllowedOrigins(raw string) map[string]struct{} {
	allowed := make(map[string]struct{})
	if strings.TrimSpace(raw) == "" {
		// Zero-config demo mode: if no explicit allow-list is configured,
		// trust all origins. Set LOADTEST_ALLOWED_ORIGINS to a comma-separated
		// list in stricter environments.
		allowed[corsAllowAllMarker] = struct{}{}
		return allowed
	}

	for _, token := range strings.Split(raw, ",") {
		origin := strings.TrimSpace(token)
		if origin == "" {
			continue
		}
		if origin == corsAllowAllMarker {
			allowed[corsAllowAllMarker] = struct{}{}
			continue
		}
		key, _, _, _, ok := normalizeOrigin(origin)
		if !ok {
			slog.Warn("ignoring invalid allowed origin", "origin", origin)
			continue
		}
		allowed[key] = struct{}{}
	}
	return allowed
}

func isRequestOriginAllowed(r *http.Request, allowedOrigins map[string]struct{}) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	if _, ok := allowedOrigins[corsAllowAllMarker]; ok {
		return true
	}

	originKey, originScheme, originHost, originPort, ok := normalizeOrigin(origin)
	if !ok {
		return false
	}

	reqScheme := requestScheme(r)
	reqHost, reqPort, ok := normalizeHostPort(r.Host, reqScheme)
	if ok &&
		originScheme == reqScheme &&
		originHost == reqHost &&
		originPort == reqPort {
		return true
	}

	_, ok = allowedOrigins[originKey]
	return ok
}

func normalizeOrigin(raw string) (key, scheme, host, port string, ok bool) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", "", "", "", false
	}

	scheme = strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", "", "", "", false
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", "", "", false
	}
	if path := strings.TrimSpace(parsed.Path); path != "" && path != "/" {
		return "", "", "", "", false
	}

	host = strings.ToLower(parsed.Hostname())
	if host == "" {
		return "", "", "", "", false
	}
	port = parsed.Port()
	if port == "" {
		port = defaultPortForScheme(scheme)
	}
	if port == "" {
		return "", "", "", "", false
	}

	key = fmt.Sprintf("%s://%s:%s", scheme, host, port)
	return key, scheme, host, port, true
}

func normalizeHostPort(hostport, scheme string) (host, port string, ok bool) {
	hostURL, err := url.Parse("http://" + strings.TrimSpace(hostport))
	if err != nil {
		return "", "", false
	}
	host = strings.ToLower(hostURL.Hostname())
	if host == "" {
		return "", "", false
	}
	port = hostURL.Port()
	if port == "" {
		port = defaultPortForScheme(scheme)
	}
	if port == "" {
		return "", "", false
	}
	return host, port, true
}

func requestScheme(r *http.Request) string {
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); forwarded != "" {
		if idx := strings.Index(forwarded, ","); idx >= 0 {
			forwarded = strings.TrimSpace(forwarded[:idx])
		}
		switch strings.ToLower(forwarded) {
		case "http", "https":
			return strings.ToLower(forwarded)
		}
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func defaultPortForScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "https":
		return "443"
	case "http":
		return "80"
	default:
		return ""
	}
}

func sanitizeLoadtestRequestTimeout(requested time.Duration) time.Duration {
	if requested <= 0 {
		return defaultLoadtestRequestTimeout
	}
	if requested < minLoadtestRequestTimeout {
		return minLoadtestRequestTimeout
	}
	if requested > maxLoadtestRequestTimeout {
		return maxLoadtestRequestTimeout
	}
	return requested
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("failed to write JSON response", "err", err)
	}
}

func writeJSONError(w http.ResponseWriter, statusCode int, msg string) {
	writeJSON(w, statusCode, map[string]string{"error": msg})
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return v
}

func getenvInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func getenvInt64(key string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return v
}

func getenvFloat64(key string, fallback float64) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fallback
	}
	return v
}
