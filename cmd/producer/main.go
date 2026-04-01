package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"nexus/internal/broker"
	"nexus/internal/hub"
	"nexus/internal/store"
)

func main() {
	amqpURL := getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
	pgDSN := getenv("POSTGRES_DSN", "postgres://nexus:nexus@localhost:5432/nexus?sslmode=disable")
	listenAddr := getenv("LISTEN_ADDR", ":8080")

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
	pub := broker.NewPublisher(conn)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", handlePublish(pub))
	mux.HandleFunc("GET /notifications", handleListNotifications(st))
	mux.HandleFunc("GET /ws", wsHub.ServeWS)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		slog.Info("producer listening", "addr", listenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	slog.Info("producer shut down")
}

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

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
