package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"nexus/internal/hub"
	"nexus/internal/metrics"
	"nexus/internal/notifcache"
	"nexus/internal/replay"
	"nexus/internal/store"
)

// EventPublisher is the local narrowing of what handlers need from a
// publisher.
type EventPublisher interface {
	Publish(ctx context.Context, eventType, priority string, payload map[string]any) (string, error)
}

type publishRequest struct {
	Type     string         `json:"type"`
	Priority string         `json:"priority"`
	Payload  map[string]any `json:"payload"`
}

func handleMetricsSummary(h *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snapshot := metrics.ComputeSummary(h.Count())
		writeJSON(w, http.StatusOK, snapshot)
	}
}

func handlePublish(pub EventPublisher) http.HandlerFunc {
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
