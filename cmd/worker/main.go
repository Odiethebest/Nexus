package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"nexus/internal/envutil"
	"nexus/internal/idempotency"
	"nexus/internal/kbroker"
	"nexus/internal/kworker"
	"nexus/internal/mailer"
	_ "nexus/internal/metrics" // register Prometheus collectors
	"nexus/internal/store"
	"nexus/internal/wsfeed"
)

func main() {
	if path, err := envutil.LoadDotEnvIfPresent(); err != nil {
		slog.Warn("failed to auto-load .env", "err", err)
	} else if path != "" {
		slog.Info("loaded environment file", "path", path)
	}

	redisURL := getenv("REDIS_URL", "redis://localhost:6379")
	pgDSN := getenv("POSTGRES_DSN", "postgres://nexus:nexus@localhost:5432/nexus?sslmode=disable")

	emailPool := parseInt(getenv("EMAIL_WORKER_POOL", "10"))
	inappPool := parseInt(getenv("INAPP_WORKER_POOL", "5"))
	webhookPool := parseInt(getenv("WEBHOOK_WORKER_POOL", "8"))

	kcfg, err := kbroker.LoadConfig()
	if err != nil {
		slog.Error("kafka config", "err", err)
		os.Exit(1)
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		slog.Error("invalid redis URL", "err", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()

	idem := idempotency.New(rdb)

	st, err := store.New(pgDSN)
	if err != nil {
		slog.Error("failed to connect to store", "err", err)
		os.Exit(1)
	}
	if err := st.Migrate(context.Background()); err != nil {
		slog.Error("migration failed", "err", err)
		os.Exit(1)
	}

	var m *mailer.Mailer
	if smtpHost := getenv("SMTP_HOST", ""); smtpHost != "" {
		m = mailer.New(mailer.Config{
			Host:     smtpHost,
			Port:     getenv("SMTP_PORT", "587"),
			Username: getenv("SMTP_USER", ""),
			Password: getenv("SMTP_PASS", ""),
			From:     getenv("EMAIL_FROM", "Nexus <no-reply@example.com>"),
		})
		slog.Info("mailer: SMTP configured", "host", smtpHost)
	} else {
		slog.Warn("mailer: SMTP_HOST not set, email sending disabled")
	}

	// Live dashboard feed. The worker has no HTTP server, so it cannot push
	// to WebSocket clients itself — it publishes envelopes onto Redis and
	// every producer replica fans them out to its own /ws clients.
	feed := wsfeed.NewPublisher(rdb, slog.Default())
	defer feed.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	run := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("worker starting", "name", name)
			if err := fn(ctx); err != nil && err != context.Canceled {
				slog.Error("worker exited with error", "name", name, "err", err)
			}
		}()
	}

	// Expose Prometheus metrics on a dedicated port so Prometheus can scrape
	// the worker independently from the producer.
	metricsAddr := getenv("METRICS_ADDR", ":9091")
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		slog.Info("worker metrics listening", "addr", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			slog.Error("metrics server error", "err", err)
		}
	}()

	republisher, err := kworker.NewKafkaRepublisher(kcfg)
	if err != nil {
		slog.Error("kafka republisher", "err", err)
		os.Exit(1)
	}

	// One runner per (channel, priority) lane. Independent consumer groups
	// per lane so a stuck lane cannot block committed offsets on another —
	// this is what preserves the AMQP-era guarantee that low priority
	// backlog can't slow the high priority path.
	procs := map[kbroker.Channel]kworker.Processor{
		kbroker.ChannelEmail:   &kworker.EmailProcessor{Mailer: m, Log: slog.Default()},
		kbroker.ChannelInApp:   &kworker.InAppProcessor{Log: slog.Default()},
		kbroker.ChannelWebhook: kworker.NewWebhookProcessor(slog.Default()),
	}
	pools := map[kbroker.Channel]map[kbroker.Priority]int{
		kbroker.ChannelEmail:   kworker.PoolSizesFor(emailPool),
		kbroker.ChannelInApp:   kworker.PoolSizesFor(inappPool),
		kbroker.ChannelWebhook: kworker.PoolSizesFor(webhookPool),
	}
	for _, ch := range kbroker.Channels {
		for _, p := range kbroker.Priorities {
			runner, err := kworker.NewRunner(kcfg, kworker.RunnerOptions{
				Channel:     ch,
				Priority:    p,
				PoolSize:    pools[ch][p],
				Processor:   procs[ch],
				Idempotency: idem,
				Store:       st,
				Republisher: republisher,
				Feed:        feed,
				Log:         slog.Default(),
			})
			if err != nil {
				slog.Error("build lane runner", "channel", ch, "priority", p, "err", err)
				os.Exit(1)
			}
			name := string(ch) + "." + string(p)
			run(name, runner.Run)
		}
	}
	slog.Info("worker backend", "impl", "kafka",
		"brokers", kcfg.Brokers,
		"lanes", len(kbroker.Channels)*len(kbroker.Priorities))

	<-ctx.Done()
	slog.Info("shutting down workers...")
	wg.Wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := republisher.Close(shutdownCtx); err != nil {
		slog.Warn("kafka republisher close", "err", err)
	}
	slog.Info("all workers stopped")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseInt(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
