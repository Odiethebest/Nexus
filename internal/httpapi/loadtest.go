package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"nexus/internal/loadtest"
	"nexus/internal/metrics"
)

type loadtestStartResponse struct {
	Mode             string             `json:"mode,omitempty"`
	RunID            int64              `json:"run_id"`
	TestID           int64              `json:"test_id"`
	Status           loadtest.RunStatus `json:"status"`
	StartedAt        time.Time          `json:"started_at"`
	PollAfterSeconds int                `json:"poll_after_seconds"`
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
