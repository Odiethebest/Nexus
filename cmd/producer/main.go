package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"nexus/internal/envconf"
	"nexus/internal/envutil"
	"nexus/internal/grpcserver"
	"nexus/internal/httpapi"
	"nexus/internal/hub"
	"nexus/internal/kbroker"
	"nexus/internal/loadtest"
	"nexus/internal/notifcache"
	"nexus/internal/replay"
	"nexus/internal/store"
	"nexus/internal/wsfeed"
)

const (
	defaultLoadtestRequestTimeout = 20 * time.Second
	minLoadtestRequestTimeout     = 5 * time.Second
	maxLoadtestRequestTimeout     = 30 * time.Second
	defaultLoadtestStatusTimeout  = 4 * time.Second
	defaultLoadtestQueryTimeout   = 3 * time.Second
)

func main() {
	if path, err := envutil.LoadDotEnvIfPresent(); err != nil {
		slog.Warn("failed to auto-load .env", "err", err)
	} else if path != "" {
		slog.Info("loaded environment file", "path", path)
	}

	pgDSN := envconf.String("POSTGRES_DSN", "postgres://nexus:nexus@localhost:5432/nexus?sslmode=disable")
	redisURL := envconf.String("REDIS_URL", "redis://localhost:6379")
	listenAddr := envconf.String("LISTEN_ADDR", ":"+envconf.String("PORT", "8080"))
	grpcAddr := envconf.String("GRPC_ADDR", ":50051")

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

	allowedOrigins := httpapi.LoadAllowedOrigins()
	if httpapi.AllowsAll(allowedOrigins) {
		slog.Warn("CORS: every origin is trusted — set CORS_ALLOWED_ORIGINS to restrict")
	} else {
		slog.Info("CORS: origin allow-list active", "count", len(allowedOrigins))
	}
	// Browsers do not apply CORS to WebSocket handshakes, so the upgrader
	// gets the same allow-list explicitly — otherwise /ws would stay
	// readable from any page even with the REST API locked down.
	wsHub := hub.New(httpapi.OriginChecker(allowedOrigins))

	// Root context so background samplers (lag reader, ws bridge) stop when
	// SIGTERM fires.
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	// Delivery events originate in the worker process, which has no HTTP
	// server. This bridge subscribes to the Redis channel the worker
	// publishes on and fans each envelope out to this replica's /ws clients.
	go func() {
		if err := wsfeed.NewBridge(rdb, wsHub, slog.Default()).Run(rootCtx); err != nil &&
			!errors.Is(err, context.Canceled) {
			slog.Error("ws feed bridge stopped", "err", err)
		}
	}()

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
	var pub httpapi.EventPublisher = kafkaPub
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
	loadtestEnabled := envconf.Bool("LOADTEST_ENABLED", false)
	demoRunDuration := time.Duration(envconf.Int("LOADTEST_DEMO_RUN_SECONDS", 55)) * time.Second
	demoLoadtestSvc := loadtest.NewDemoService(loadtest.DemoServiceConfig{
		PollInterval: time.Duration(envconf.Int("LOADTEST_POLL_INTERVAL_SECONDS", 2)) * time.Second,
		RunDuration:  demoRunDuration,
	})
	slog.Info("loadtest endpoints configured", "enabled", loadtestEnabled)
	var latestLoadtestRun atomic.Int64

	// ── HTTP server ───────────────────────────────────────────────────────
	srv := &http.Server{
		Addr: listenAddr,
		Handler: httpapi.NewRouter(httpapi.Deps{
			Publisher:      pub,
			Cache:          notifCache,
			Store:          st,
			Replayer:       replayer,
			Hub:            wsHub,
			Loadtest:       loadtestSvc,
			DemoLoadtest:   demoLoadtestSvc,
			LatestRun:      &latestLoadtestRun,
			AllowedOrigins: allowedOrigins,
		}),
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

func initLoadtestService() (*loadtest.Service, error) {
	enabled := envconf.Bool("LOADTEST_ENABLED", false)
	pollInterval := time.Duration(envconf.Int("LOADTEST_POLL_INTERVAL_SECONDS", 2)) * time.Second
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	statusTimeout := time.Duration(envconf.Int("LOADTEST_STATUS_TIMEOUT_SECONDS", int(defaultLoadtestStatusTimeout.Seconds()))) * time.Second
	queryTimeout := time.Duration(envconf.Int("LOADTEST_QUERY_TIMEOUT_SECONDS", int(defaultLoadtestQueryTimeout.Seconds()))) * time.Second

	serviceCfg := loadtest.ServiceConfig{
		Enabled:              enabled,
		LoadTestID:           envconf.Int64("K6_LOAD_TEST_ID", 0),
		PollInterval:         pollInterval,
		DailyVUHCap:          envconf.Float64("LOADTEST_BUDGET_VUH_PER_DAY", 0),
		MaxRunDuration:       time.Duration(envconf.Int("LOADTEST_MAX_RUN_SECONDS", 55)) * time.Second,
		StatusRequestTimeout: statusTimeout,
		MetricQueryTimeout:   queryTimeout,
	}

	guardCfg := loadtest.GuardConfig{
		AdminKey:         os.Getenv("LOADTEST_ADMIN_KEY"),
		MaxParallel:      envconf.Int("LOADTEST_MAX_PARALLEL", 1),
		Cooldown:         time.Duration(envconf.Int("LOADTEST_COOLDOWN_SECONDS", 300)) * time.Second,
		MinStartInterval: time.Duration(envconf.Int("LOADTEST_MIN_START_INTERVAL_SECONDS", 0)) * time.Second,
	}

	guard := loadtest.NewGuard(guardCfg)
	if !enabled {
		return loadtest.NewService(serviceCfg, nil, guard), nil
	}
	if strings.TrimSpace(guardCfg.AdminKey) == "" {
		return nil, fmt.Errorf("LOADTEST_ADMIN_KEY must be set when loadtest is enabled")
	}
	// strconv.ParseFloat accepts "NaN" and "Inf", and a non-finite cap makes
	// the budget check in loadtest.Service inert: every comparison against NaN
	// is false, so neither the "unconfigured" branch (DailyVUHCap <= 0) nor the
	// exceeded branch (used >= cap) ever fires, and the budget stops applying
	// without saying so. Negative values are deliberately not rejected — they
	// land in the <= 0 branch, which is the documented "no budget" setting.
	//
	// This runs after the !enabled early return on purpose: a deployment that
	// has loadtest switched off must not fail to boot over a variable it never
	// reads.
	if vuhCap := serviceCfg.DailyVUHCap; math.IsNaN(vuhCap) || math.IsInf(vuhCap, 0) {
		return nil, fmt.Errorf("LOADTEST_BUDGET_VUH_PER_DAY must be a finite number, got %v", vuhCap)
	}

	timeoutRaw := time.Duration(envconf.Int("LOADTEST_REQUEST_TIMEOUT_SECONDS", int(defaultLoadtestRequestTimeout.Seconds()))) * time.Second
	timeout := sanitizeLoadtestRequestTimeout(timeoutRaw)
	if timeout != timeoutRaw {
		slog.Warn("adjusted loadtest request timeout", "requested", timeoutRaw.String(), "effective", timeout.String())
	}

	client, err := loadtest.NewClient(loadtest.ClientConfig{
		BaseURL:  envconf.String("K6_API_BASE", "https://api.k6.io"),
		APIToken: os.Getenv("K6_API_TOKEN"),
		StackID:  os.Getenv("K6_STACK_ID"),
		HTTPClient: &http.Client{
			Timeout: timeout,
		},
		RetryMaxAttempts:               envconf.Int("LOADTEST_UPSTREAM_RETRY_MAX", 2),
		RetryBaseDelay:                 time.Duration(envconf.Int("LOADTEST_UPSTREAM_RETRY_BASE_MS", 250)) * time.Millisecond,
		RetryMaxDelay:                  time.Duration(envconf.Int("LOADTEST_UPSTREAM_RETRY_MAX_MS", 2000)) * time.Millisecond,
		CircuitBreakerFailureThreshold: envconf.Int("LOADTEST_CIRCUIT_BREAKER_THRESHOLD", 5),
		CircuitBreakerOpenDuration:     time.Duration(envconf.Int("LOADTEST_CIRCUIT_BREAKER_OPEN_SECONDS", 30)) * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("loadtest client: %w", err)
	}

	if serviceCfg.LoadTestID <= 0 {
		return nil, fmt.Errorf("K6_LOAD_TEST_ID must be > 0 when loadtest is enabled")
	}

	return loadtest.NewService(serviceCfg, client, guard), nil
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
