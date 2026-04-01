package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"nexus/internal/broker"
	"nexus/internal/hub"
	"nexus/internal/idempotency"
	"nexus/internal/mailer"
	_ "nexus/internal/metrics" // register Prometheus collectors
	"nexus/internal/store"
	"nexus/internal/worker"
)

func main() {
	amqpURL := getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
	redisURL := getenv("REDIS_URL", "redis://localhost:6379")
	pgDSN := getenv("POSTGRES_DSN", "postgres://nexus:nexus@localhost:5432/nexus?sslmode=disable")

	emailPool := parseInt(getenv("EMAIL_WORKER_POOL", "10"))
	inappPool := parseInt(getenv("INAPP_WORKER_POOL", "5"))
	webhookPool := parseInt(getenv("WEBHOOK_WORKER_POOL", "8"))

	conn, err := broker.New(amqpURL)
	if err != nil {
		slog.Error("failed to connect to broker", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

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

	wsHub := hub.New()
	ch := conn.Channel()

	emailW, err := worker.NewEmailWorker(ch, m, idem, st, emailPool)
	if err != nil {
		slog.Error("failed to create email worker", "err", err)
		os.Exit(1)
	}

	inappW, err := worker.NewInAppWorker(ch, wsHub, idem, st, inappPool)
	if err != nil {
		slog.Error("failed to create inapp worker", "err", err)
		os.Exit(1)
	}

	webhookW, err := worker.NewWebhookWorker(ch, idem, st, webhookPool)
	if err != nil {
		slog.Error("failed to create webhook worker", "err", err)
		os.Exit(1)
	}

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

	run("email", emailW.Run)
	run("inapp", inappW.Run)
	run("webhook", webhookW.Run)

	<-ctx.Done()
	slog.Info("shutting down workers...")
	wg.Wait()
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
