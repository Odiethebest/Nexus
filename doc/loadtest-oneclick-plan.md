# One-Click Load Test + Insight Visualization Plan

## 1. Objective

Build a production-safe one-click load testing flow for Nexus that:

- Starts a Grafana Cloud k6 load test from the Nexus UI.
- Streams run status and key performance metrics back to the UI in near real time.
- Renders high-impact visual feedback so users can immediately understand system behavior and bottlenecks.
- Enforces strict guardrails to avoid abuse, accidental spend, and target misuse.

This document is implementation-focused and can be used directly as an engineering task spec.

---

## 2. Current Baseline In Repo

- Producer HTTP router already exists and is the right place to add load test APIs: [cmd/producer/main.go](/Users/odieyang/Documents/Projects/Nexus/cmd/producer/main.go)
- Prometheus app metrics already exist for publish/process paths: [internal/metrics/metrics.go](/Users/odieyang/Documents/Projects/Nexus/internal/metrics/metrics.go)
- Frontend panel architecture already exists and can host a new "Stress Lab" panel: [web/src/App.jsx](/Users/odieyang/Documents/Projects/Nexus/web/src/App.jsx)
- Producer container currently does not install k6 CLI binary, so local in-container `k6 run` is not plug-and-play: [deploy/Dockerfile.producer](/Users/odieyang/Documents/Projects/Nexus/deploy/Dockerfile.producer)
- README performance section now explains how to capture deployment-specific metrics via Stress Lab runs: [README.md](/Users/odieyang/Documents/Projects/Nexus/README.md)

---

## 3. Recommended Strategy

Use Grafana Cloud k6 REST APIs from backend only.

Reason:

- No k6 binary dependency in Railway runtime.
- Token and stack credentials stay server-side.
- Easier to control cost, permissions, and rate limits.
- Easier to normalize data and expose a single stable UI contract.

Core external APIs:

- Start run: `POST /cloud/v6/load_tests/{id}/start`
- Poll run status: `GET /cloud/v6/test_runs/{id}`
- Query metrics: `GET /cloud/v5/test_runs/{id}/query_range_k6(...)`, `GET /cloud/v5/test_runs/{id}/query_aggregate_k6(...)`

Important version note:

- Resource lifecycle endpoints are v6.
- Metrics query endpoints are v5.

---

## 4. High-Level Architecture

```
Web UI ("Stress Lab")
   -> POST /ops/loadtest/start
Producer
   -> Grafana k6 v6 start run
   <- test_run_id
Web UI polls /ops/loadtest/{run_id}
Producer
   -> Grafana k6 v6 test run status
   -> Grafana k6 v5 metrics queries
Producer computes "insight cards"
   <- normalized JSON
Web UI animates throughput, p95, error rate, saturation, and outcome
```

---

## 5. Backend Implementation Plan

### 5.1 New Environment Variables

Add to `.env.example` and deployment variables:

- `LOADTEST_ENABLED=true`
- `LOADTEST_ADMIN_KEY=replace-me`
- `LOADTEST_COOLDOWN_SECONDS=300`
- `LOADTEST_MAX_PARALLEL=1`
- `LOADTEST_POLL_INTERVAL_SECONDS=3`
- `LOADTEST_REQUEST_TIMEOUT_SECONDS=20`
- `K6_API_TOKEN=...`
- `K6_STACK_ID=12345`
- `K6_LOAD_TEST_ID=1234`
- `K6_API_BASE=https://api.k6.io`

Optional:

- `LOADTEST_ALLOWED_ORIGINS=https://your-frontend-domain`
- `LOADTEST_BUDGET_VUH_PER_DAY=200`

### 5.2 New Internal Package

Create `internal/loadtest/` with:

- `client.go`: HTTP client for k6 API calls.
- `types.go`: run status, metric points, normalized DTOs.
- `service.go`: orchestration, cooldown, budget checks, insight calculation.
- `guard.go`: auth key validation, concurrency lock, throttling logic.
- `service_test.go`: unit tests using `httptest.Server`.

### 5.3 Producer API Endpoints

Add to producer router in [cmd/producer/main.go](/Users/odieyang/Documents/Projects/Nexus/cmd/producer/main.go):

- `POST /ops/loadtest/start`
- `GET /ops/loadtest/{run_id}`
- `GET /ops/loadtest/latest` (optional convenience)

Authentication:

- Require header `X-Admin-Key: <LOADTEST_ADMIN_KEY>` for `start`.
- Optional same requirement for `status` if environment demands.

### 5.4 API Contract Draft

`POST /ops/loadtest/start`

Request:

```json
{
  "scenario": "default",
  "note": "railway smoke run",
  "preset": "quick"
}
```

Response `202`:

```json
{
  "run_id": 987654,
  "test_id": 1234,
  "status": "created",
  "started_at": "2026-04-01T23:40:00Z",
  "poll_after_seconds": 3
}
```

Error examples:

- `403` invalid admin key
- `409` already running
- `429` cooldown active
- `502` upstream k6 API failure

`GET /ops/loadtest/{run_id}`

Response `200`:

```json
{
  "run": {
    "id": 987654,
    "status": "running",
    "result": null,
    "created": "2026-04-01T23:40:00Z",
    "ended": null
  },
  "series": {
    "rps": [[1712014800, 520.1], [1712014805, 545.3]],
    "p95_ms": [[1712014800, 38.2], [1712014805, 44.9]],
    "error_rate_pct": [[1712014800, 0.12], [1712014805, 0.21]],
    "vus": [[1712014800, 80], [1712014805, 100]]
  },
  "snapshot": {
    "rps": 545.3,
    "p95_ms": 44.9,
    "error_rate_pct": 0.21,
    "vus": 100,
    "insight": "Stable under target SLO"
  },
  "health_score": 87,
  "signals": [
    "No sustained error spike detected",
    "p95 remains below 60ms threshold"
  ]
}
```

### 5.5 Insight Computation

Normalize raw k6 metrics into user-facing indicators:

- Throughput: `http_reqs` rate.
- Latency: p95 from `http_req_duration`.
- Error rate: `http_req_failed / http_reqs`.
- Saturation signal: VU trend + response time acceleration.
- Stability: moving window variance on p95 and error rate.

Health score formula (v1 suggestion):

- Base 100.
- Subtract latency penalty.
- Subtract error penalty.
- Subtract volatility penalty.
- Clamp to `[0,100]`.

### 5.6 Guardrails

Must-have controls:

- Global mutex to prevent parallel runs.
- Cooldown timer after each run.
- Daily budget cap in VUH.
- Request timeout and retry with jitter for upstream API.
- Circuit-breaker style open state after repeated upstream failures.

Nice-to-have:

- Per-user/IP rate limiting.
- Audit log of who started tests and why.

### 5.7 Backend Metrics

Expose additional producer metrics:

- `nexus_loadtest_start_total{status="ok|deny|error"}`
- `nexus_loadtest_upstream_latency_seconds{endpoint="start|run|query"}`
- `nexus_loadtest_active_runs`
- `nexus_loadtest_health_score`

---

## 6. Frontend Implementation Plan

### 6.1 New Panel

Add a new panel in [web/src/App.jsx](/Users/odieyang/Documents/Projects/Nexus/web/src/App.jsx):

- Title: `Stress Lab`
- Primary CTA: `Start Load Test`
- Run status line: `created -> queued -> initializing -> running -> processing_metrics -> completed`
- Live score badge: `0-100`
- Key stat cards: `RPS`, `p95(ms)`, `Error%`, `VUs`

### 6.2 "Flashy but truthful" Visual Design

Use effects only when backed by real values:

- Neon waveform for RPS line.
- Pulse glow on p95 card when crossing threshold.
- Red warning sweep when error rate spikes.
- Fan-out channel beam animation tied to throughput changes.
- End-of-run reveal animation with final score and 3 textual insights.

Do not fake success:

- If no metrics yet, show explicit `Warming up`.
- If upstream failed, show clear retry guidance.

### 6.3 Frontend State Machine

Use explicit state enum:

- `idle`
- `starting`
- `running`
- `analyzing`
- `completed`
- `failed`

Polling behavior:

- Poll every 3 seconds while running.
- Stop polling at terminal states.
- Backoff to 5 to 8 seconds if temporary errors occur.

### 6.4 UX Copy (v1)

Keep copy concrete:

- Button default: `Start Load Test`
- During run: `Load Test Running`
- Terminal success: `Run Completed`
- Terminal fail: `Run Failed`
- Cooldown: `Try again in 02:15`

---

## 7. Security and Cost Controls

Mandatory:

- Start endpoint restricted by admin key.
- CORS restricted to trusted frontend origin.
- Never expose k6 API token to browser.
- Enforce max one active run globally.
- Add cooldown and budget limits.

Deployment practice:

- Store secrets in Railway service variables.
- Rotate `K6_API_TOKEN` periodically.
- Log only masked token fragments if needed for debugging.

---

## 8. Railway Deployment Notes

Producer service:

- No Dockerfile change required for cloud-API approach.
- Add new env vars to producer service only.

Worker service:

- No change required for this feature.

Networking:

- Ensure producer can reach `https://api.k6.io`.
- Keep outbound timeout strict.

---

## 9. Testing Plan

### 9.1 Backend Tests

- Unit tests for k6 client request/response mapping.
- Unit tests for guardrails: cooldown, concurrency, budget deny.
- Unit tests for insight score calculation.
- Handler tests for HTTP status behavior.

### 9.2 Frontend Tests

- Component tests for state machine transitions.
- Contract tests for API JSON parsing.
- Visual regression snapshots for critical states.

### 9.3 Manual E2E Checklist

- Start run with valid key returns `202`.
- Second start while active returns `409`.
- Completed run displays score and insight cards.
- Upstream error displays actionable failure state.
- Cooldown is enforced and countdown visible.

---

## 10. Delivery Phases

Phase 1 (MVP):

- Backend start/status APIs.
- Frontend Stress Lab panel with polling and basic charts.
- Guardrails: key + single active run + cooldown.

Phase 2:

- Insight scoring and explanation strings.
- Rich animations bound to live metrics.
- Backend metrics for loadtest subsystem.

Phase 3:

- Budget cap + audit trail.
- Run history table and compare mode.
- Auto-sync final results into README performance section.

---

## 11. Acceptance Criteria

- User can trigger a test from UI with one click.
- UI shows real run lifecycle and live metrics in under 5 seconds from start.
- No direct `K6_API_TOKEN` exposure in frontend code or network traces.
- Dashboard admin key is not persisted in browser storage.
- Concurrency and cooldown limits are enforced by backend.
- Feature works on Railway deployment with env-only configuration.
- Failures are visible with clear reason and recovery action.

---

## 12. Suggested Task Breakdown

1. `internal/loadtest` package skeleton and k6 client.
2. Producer endpoints and guard middleware.
3. Insight computation and normalized response schema.
4. Stress Lab panel UI and polling loop.
5. Animated rendering for score and signal cards.
6. Tests and docs updates.
7. Railway variable template and rollout checklist.

---

## 13. API and Documentation References

- Grafana Cloud k6 REST API overview: https://grafana.com/docs/grafana-cloud/testing/k6/reference/cloud-rest-api/
- Load tests API (`/cloud/v6/load_tests/...`): https://grafana.com/docs/grafana-cloud/testing/k6/reference/cloud-rest-api/load-tests/
- Test runs API (`/cloud/v6/test_runs/...`): https://grafana.com/docs/grafana-cloud/testing/k6/reference/cloud-rest-api/test-runs/
- Metrics API (`/cloud/v5/test_runs/...`): https://grafana.com/docs/grafana-cloud/testing/k6/reference/cloud-rest-api/metrics/

---

## 14. Implementation Decision

Decision:

- Adopt backend-orchestrated Grafana Cloud k6 integration.
- Do not run local k6 CLI in Railway runtime for v1.

Rationale:

- Lower ops complexity.
- Better security posture.
- Faster delivery.
- Better fit for current Nexus deployment model.
