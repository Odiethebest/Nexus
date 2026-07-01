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
	"nexus/internal/broker"
	"nexus/internal/envutil"
	"nexus/internal/hub"
	"nexus/internal/idempotency"
	"nexus/internal/kbroker"
	"nexus/internal/kworker"
	"nexus/internal/mailer"
	_ "nexus/internal/metrics" // register Prometheus collectors
	"nexus/internal/store"
	"nexus/internal/worker"
)

func main() {
	if path, err := envutil.LoadDotEnvIfPresent(); err != nil {
		slog.Warn("failed to auto-load .env", "err", err)
	} else if path != "" {
		slog.Info("loaded environment file", "path", path)
	}

	amqpURL := getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
	redisURL := getenv("REDIS_URL", "redis://localhost:6379")
	pgDSN := getenv("POSTGRES_DSN", "postgres://nexus:nexus@localhost:5432/nexus?sslmode=disable")
	useKafka := getenvBool("USE_KAFKA", false)

	emailPool := parseInt(getenv("EMAIL_WORKER_POOL", "10"))
	inappPool := parseInt(getenv("INAPP_WORKER_POOL", "5"))
	webhookPool := parseInt(getenv("WEBHOOK_WORKER_POOL", "8"))

	var conn *broker.Connection
	if !useKafka {
		c, err := broker.New(amqpURL)
		if err != nil {
			slog.Error("failed to connect to broker", "err", err)
			os.Exit(1)
		}
		conn = c
		defer conn.Close()
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

	wsHub := hub.New()

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

	var kafkaRepub *kworker.KafkaRepublisher
	if useKafka {
		kcfg, err := kbroker.LoadConfig()
		if err != nil {
			slog.Error("kafka config", "err", err)
			os.Exit(1)
		}
		kafkaRepub, err = kworker.NewKafkaRepublisher(kcfg)
		if err != nil {
			slog.Error("kafka republisher", "err", err)
			os.Exit(1)
		}
		// One runner per (channel, priority) lane. Independent consumer
		// groups per lane so a stuck lane cannot block committed offsets
		// on another.
		procs := map[kbroker.Channel]kworker.Processor{
			kbroker.ChannelEmail:   &kworker.EmailProcessor{Mailer: m, Log: slog.Default()},
			kbroker.ChannelInApp:   &kworker.InAppProcessor{Hub: wsHub, Log: slog.Default()},
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
					Republisher: kafkaRepub,
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
		slog.Info("worker backend", "impl", "kafka", "lanes", len(kbroker.Channels)*len(kbroker.Priorities))
	} else {
		emailW, err := worker.NewEmailWorker(conn, m, idem, st, emailPool)
		if err != nil {
			slog.Error("failed to create email worker", "err", err)
			os.Exit(1)
		}
		inappW, err := worker.NewInAppWorker(conn, wsHub, idem, st, inappPool)
		if err != nil {
			slog.Error("failed to create inapp worker", "err", err)
			os.Exit(1)
		}
		webhookW, err := worker.NewWebhookWorker(conn, idem, st, webhookPool)
		if err != nil {
			slog.Error("failed to create webhook worker", "err", err)
			os.Exit(1)
		}
		run("email", emailW.Run)
		run("inapp", inappW.Run)
		run("webhook", webhookW.Run)
		slog.Info("worker backend", "impl", "amqp")
	}

	<-ctx.Done()
	slog.Info("shutting down workers...")
	wg.Wait()
	// After all runners' Run returns, flush and close the shared republisher.
	if kafkaRepub != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := kafkaRepub.Close(shutdownCtx); err != nil {
			slog.Warn("kafka republisher close", "err", err)
		}
	}
	slog.Info("all workers stopped")
}

func getenvBool(key string, fallback bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return v
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
