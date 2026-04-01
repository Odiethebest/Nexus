package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"nexus/internal/broker"
	"nexus/internal/grpcserver"
	"nexus/internal/hub"
	"nexus/internal/loadtest"
	_ "nexus/internal/metrics" // register Prometheus collectors
	"nexus/internal/replay"
	"nexus/internal/store"
	nexusweb "nexus/web"
)

func main() {
	amqpURL := getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
	pgDSN := getenv("POSTGRES_DSN", "postgres://nexus:nexus@localhost:5432/nexus?sslmode=disable")
	listenAddr := getenv("LISTEN_ADDR", ":"+getenv("PORT", "8080"))
	grpcAddr := getenv("GRPC_ADDR", ":50051")

	conn, err := broker.New(amqpURL)
	if err != nil {
		slog.Error("failed to connect to broker", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	st, err := store.New(pgDSN)
	if err != nil {
		slog.Error("failed to connect to store", "err", err)
		os.Exit(1)
	}
	if err := st.Migrate(context.Background()); err != nil {
		slog.Error("migration failed", "err", err)
		os.Exit(1)
	}

	wsHub := hub.New()
	pub, err := broker.NewPublisher(conn)
	if err != nil {
		slog.Error("failed to create publisher", "err", err)
		os.Exit(1)
	}

	replayer := replay.New(conn)
	loadtestSvc, err := initLoadtestService()
	if err != nil {
		slog.Error("failed to initialize loadtest service", "err", err)
		os.Exit(1)
	}
	loadtestEnabled := getenvBool("LOADTEST_ENABLED", false)
	slog.Info("loadtest endpoints configured", "enabled", loadtestEnabled)
	var latestLoadtestRun atomic.Int64

	// ── HTTP server ───────────────────────────────────────────────────────
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", handlePublish(pub))
	mux.HandleFunc("GET /notifications", handleListNotifications(st))
	mux.HandleFunc("POST /dlq/replay", handleReplay(replayer))
	mux.HandleFunc("POST /ops/loadtest/start", handleLoadtestStart(loadtestSvc, &latestLoadtestRun))
	mux.HandleFunc("GET /ops/loadtest/{run_id}", handleLoadtestStatus(loadtestSvc))
	mux.HandleFunc("GET /ops/loadtest/latest", handleLoadtestLatest(loadtestSvc, &latestLoadtestRun))
	mux.HandleFunc("GET /ws", wsHub.ServeWS)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// ── Static frontend ───────────────────────────────────────────────────
	staticFS, _ := fs.Sub(nexusweb.FS, "dist")
	mux.Handle("GET /assets/", http.FileServerFS(staticFS))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		index, _ := nexusweb.FS.ReadFile("dist/index.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(index)
	})

	srv := &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
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
	slog.Info("producer shut down")
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

type publishRequest struct {
	Type     string         `json:"type"`
	Priority string         `json:"priority"`
	Payload  map[string]any `json:"payload"`
}

func handlePublish(pub *broker.Publisher) http.HandlerFunc {
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

func handleListNotifications(st *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		notifications, err := st.ListNotifications(r.Context(), 50)
		if err != nil {
			slog.Error("list notifications failed", "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(notifications)
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

func handleLoadtestStart(svc *loadtest.Service, latest *atomic.Int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if svc == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "loadtest service unavailable")
			return
		}

		var req loadtest.StartOptions
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		started, err := svc.Start(
			r.Context(),
			r.Header.Get("X-Admin-Key"),
			actorFromRequest(r),
			req,
		)
		if err != nil {
			status, message := mapLoadtestError(err)
			slog.Warn("loadtest start failed", "status", status, "err", err)
			writeJSONError(w, status, message)
			return
		}

		latest.Store(started.RunID)

		pollAfterSeconds := int(started.PollAfter.Seconds())
		if pollAfterSeconds <= 0 {
			pollAfterSeconds = 3
		}

		writeJSON(w, http.StatusAccepted, map[string]any{
			"run_id":             started.RunID,
			"test_id":            started.TestID,
			"status":             started.Status,
			"started_at":         started.StartedAt,
			"poll_after_seconds": pollAfterSeconds,
		})
	}
}

func handleLoadtestStatus(svc *loadtest.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if svc == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "loadtest service unavailable")
			return
		}

		runID, err := strconv.ParseInt(r.PathValue("run_id"), 10, 64)
		if err != nil || runID <= 0 {
			writeJSONError(w, http.StatusBadRequest, "invalid run_id")
			return
		}

		insight, err := svc.SyncRun(r.Context(), runID)
		if err != nil {
			status, message := mapLoadtestError(err)
			slog.Warn("loadtest status fetch failed", "run_id", runID, "status", status, "err", err)
			writeJSONError(w, status, message)
			return
		}

		writeJSON(w, http.StatusOK, insight)
	}
}

func handleLoadtestLatest(svc *loadtest.Service, latest *atomic.Int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if svc == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "loadtest service unavailable")
			return
		}
		runID := latest.Load()
		if runID <= 0 {
			writeJSONError(w, http.StatusNotFound, "no loadtest run recorded")
			return
		}

		insight, err := svc.SyncRun(r.Context(), runID)
		if err != nil {
			status, message := mapLoadtestError(err)
			slog.Warn("loadtest latest fetch failed", "run_id", runID, "status", status, "err", err)
			writeJSONError(w, status, message)
			return
		}
		writeJSON(w, http.StatusOK, insight)
	}
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
	pollInterval := time.Duration(getenvInt("LOADTEST_POLL_INTERVAL_SECONDS", 3)) * time.Second
	if pollInterval <= 0 {
		pollInterval = 3 * time.Second
	}

	serviceCfg := loadtest.ServiceConfig{
		Enabled:      enabled,
		LoadTestID:   getenvInt64("K6_LOAD_TEST_ID", 0),
		PollInterval: pollInterval,
		DailyVUHCap:  getenvFloat64("LOADTEST_BUDGET_VUH_PER_DAY", 0),
	}

	guardCfg := loadtest.GuardConfig{
		AdminKey:    os.Getenv("LOADTEST_ADMIN_KEY"),
		MaxParallel: getenvInt("LOADTEST_MAX_PARALLEL", 1),
		Cooldown:    time.Duration(getenvInt("LOADTEST_COOLDOWN_SECONDS", 300)) * time.Second,
	}

	guard := loadtest.NewGuard(guardCfg)
	if !enabled {
		return loadtest.NewService(serviceCfg, nil, guard), nil
	}
	if strings.TrimSpace(guardCfg.AdminKey) == "" {
		return nil, fmt.Errorf("LOADTEST_ADMIN_KEY must be set when loadtest is enabled")
	}

	timeout := time.Duration(getenvInt("LOADTEST_REQUEST_TIMEOUT_SECONDS", 20)) * time.Second
	if timeout <= 0 {
		timeout = 20 * time.Second
	}

	client, err := loadtest.NewClient(loadtest.ClientConfig{
		BaseURL:  getenv("K6_API_BASE", "https://api.k6.io"),
		APIToken: os.Getenv("K6_API_TOKEN"),
		StackID:  os.Getenv("K6_STACK_ID"),
		HTTPClient: &http.Client{
			Timeout: timeout,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("loadtest client: %w", err)
	}

	if serviceCfg.LoadTestID <= 0 {
		return nil, fmt.Errorf("K6_LOAD_TEST_ID must be > 0 when loadtest is enabled")
	}

	return loadtest.NewService(serviceCfg, client, guard), nil
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
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
