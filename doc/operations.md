# Operations Guide

## Docker Compose Stack

Full stack (all seven services):

```bash
docker compose -f deploy/docker-compose.yml up --build
```

Infrastructure only (for local development against native binaries):

```bash
docker compose -f deploy/docker-compose.yml up -d rabbitmq redis postgres
```

### Services

| Service | Image | Ports | Notes |
|---------|-------|-------|-------|
| `rabbitmq` | rabbitmq:3.13-management-alpine | 5672, 15672 | Management UI at `/` |
| `redis` | redis:7-alpine | 6379 | |
| `postgres` | postgres:16-alpine | 5432 | Volume: `postgres_data` |
| `producer` | Dockerfile.producer | 8080 | HTTP (gRPC listens on 50051 inside container) |
| `worker` | Dockerfile.worker | 9091 | Metrics only |
| `prometheus` | prom/prometheus:v2.53.0 | 9090 | 7-day retention |
| `grafana` | grafana/grafana:11.1.0 | 3000 | admin/admin |

All application services wait for their infrastructure dependencies to pass health checks before starting.

---

## Observability

### Prometheus

Scrapes two targets every 15 seconds:

| Job | Target | Endpoint |
|-----|--------|---------|
| `nexus-producer` | `producer:8080` | `/metrics` |
| `nexus-worker` | `worker:9091` | `/metrics` |

Web UI: `http://localhost:9090`

### Metrics Reference

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `nexus_messages_published_total` | Counter | `type`, `priority` | Events accepted by the exchange |
| `nexus_messages_processed_total` | Counter | `channel`, `status` | Worker outcomes per channel |
| `nexus_publish_duration_seconds` | Histogram | — | End-to-end publish latency incl. broker ack |
| `nexus_worker_process_duration_seconds` | Histogram | `channel` | Per-message processing time |
| `nexus_loadtest_start_total` | Counter | `status` | Loadtest start attempts by outcome |
| `nexus_loadtest_upstream_latency_seconds` | Histogram | `endpoint` | Upstream k6 API latency by endpoint class |
| `nexus_loadtest_active_runs` | Gauge | — | Active loadtest runs tracked by guard |
| `nexus_loadtest_health_score` | Gauge | — | Latest computed loadtest health score |

`status` label values: `delivered`, `failed`, `duplicate`, `dlq`.
`nexus_loadtest_start_total{status}` values: `ok`, `deny`, `error`.
`nexus_loadtest_upstream_latency_seconds{endpoint}` values: `start`, `run`, `query`, `other`.

### Grafana

Pre-provisioned dashboard at `http://localhost:3000` (admin / admin).

The dashboard is defined in `deploy/grafana/dashboard.json` and auto-loaded via volume mount. To update it: export from Grafana UI, save over the file, and restart the Grafana container.

---

## Dead-Letter Queue (DLQ)

Messages land in a DLQ when:
- **Email / In-app**: malformed message payload is nacked without requeue.
- **Webhook**: HTTP delivery fails after 3 retries (backoff: 2 s / 4 s / 8 s). Retry count is tracked via the `x-death` AMQP header.

### DLQ naming

```
nexus.{channel}.{priority}.dlq

e.g.  nexus.email.high.dlq
      nexus.webhook.normal.dlq
```

### Replaying a DLQ

```bash
curl -X POST http://localhost:8080/dlq/replay \
  -H 'Content-Type: application/json' \
  -d '{"queue":"nexus.email.high.dlq","max":100}'
# → {"replayed":N}
```

The replayer reads up to `max` messages with `basic.get` (non-destructive peek + ack), recovers the original routing key from the `x-death` header, and republishes to `nexus.events`. If no `x-death` routing key is found, it falls back to `event.unknown.normal`.

`max` is clamped to 1000. Omitting it or setting it ≤ 0 defaults to 100.

---

## Deployment (Railway)

Configured in:
- `railway.toml` (producer service)
- `deploy/railway.worker.toml` (worker service)

| Service | Dockerfile | Start command | Health check |
|---------|-----------|---------------|-------------|
| producer | `deploy/Dockerfile.producer` | `/app/producer` | `GET /health` (300 s timeout) |
| worker | `deploy/Dockerfile.worker` | `/app/worker` | — |

Set the same environment variables as listed in the [development guide](development.md#environment-variables) as Railway service variables.

For one-click loadtest, set these on the **producer service only**:

| Variable | Required | Notes |
|----------|----------|-------|
| `LOADTEST_ENABLED` | yes | Set `true` to enable the feature |
| `LOADTEST_ADMIN_KEY` | yes | Server-side key; dashboard sends it only on start requests and does not persist it in browser storage |
| `LOADTEST_COOLDOWN_SECONDS` | yes | Suggested `300` |
| `LOADTEST_MIN_START_INTERVAL_SECONDS` | yes | Suggested `10` per actor throttle |
| `LOADTEST_MAX_PARALLEL` | yes | Suggested `1` |
| `LOADTEST_POLL_INTERVAL_SECONDS` | yes | Suggested `2` |
| `LOADTEST_MAX_RUN_SECONDS` | yes | Suggested `55`; server forces abort when exceeded to keep demo flow responsive |
| `LOADTEST_STATUS_TIMEOUT_SECONDS` | yes | Suggested `4`; timeout for upstream run status calls |
| `LOADTEST_QUERY_TIMEOUT_SECONDS` | yes | Suggested `3`; timeout for each upstream metrics query |
| `LOADTEST_REQUEST_TIMEOUT_SECONDS` | yes | Suggested `20`; runtime clamps to `5-30` seconds |
| `LOADTEST_UPSTREAM_RETRY_MAX` | yes | Suggested `2` |
| `LOADTEST_UPSTREAM_RETRY_BASE_MS` | yes | Suggested `250` |
| `LOADTEST_UPSTREAM_RETRY_MAX_MS` | yes | Suggested `2000` |
| `LOADTEST_CIRCUIT_BREAKER_THRESHOLD` | yes | Suggested `5` |
| `LOADTEST_CIRCUIT_BREAKER_OPEN_SECONDS` | yes | Suggested `30` |
| `K6_API_BASE` | yes | `https://api.k6.io` |
| `K6_API_TOKEN` | yes | Grafana Cloud k6 API token |
| `K6_STACK_ID` | yes | Grafana stack ID for `X-Stack-Id` header |
| `K6_LOAD_TEST_ID` | yes | Existing cloud load test ID |
| `LOADTEST_ALLOWED_ORIGINS` | no | Comma-separated trusted origins for cross-origin HTTP + `/ws` (leave empty for same-origin only) |
| `LOADTEST_BUDGET_VUH_PER_DAY` | no | Optional daily VUH cap |

Do not set `K6_*` and `LOADTEST_*` variables on the worker service.

### Railway Networking Check

From the Railway producer shell, verify outbound connectivity to k6 API:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  --connect-timeout 5 \
  --max-time 12 \
  "${K6_API_BASE:-https://api.k6.io}"
```

Any HTTP status means egress/TLS is reachable. `000` means the request did not connect.

---

## Broker Reconnect Behaviour

The `broker.Connection` watches `amqp.Connection.NotifyClose()` in a background goroutine. On disconnect it retries with exponential backoff:

| Attempt | Wait |
|---------|------|
| 1 | 1 s |
| 2 | 2 s |
| 3 | 4 s |
| … | doubles each time, capped at 30 s |

While reconnecting, publish calls will block waiting for the mutex. Worker `Run` loops exit cleanly when `ctx` is cancelled.

---

## Database Maintenance

### Manual migration

Both binaries call `store.Migrate` on startup — the `CREATE TABLE IF NOT EXISTS` is idempotent. No separate migration tool is needed.

### Pruning old records

Notifications are never deleted automatically. To prune records older than 30 days:

```sql
DELETE FROM notifications WHERE created_at < now() - interval '30 days';
```

---

## Useful Commands

```bash
# Tail producer logs
docker compose -f deploy/docker-compose.yml logs -f producer

# Check RabbitMQ queue depths
curl -u guest:guest http://localhost:15672/api/queues | jq '.[].name, .[].messages'

# Inspect a DLQ in the management UI
open http://localhost:15672/#/queues/%2F/nexus.email.high.dlq

# Connect to PostgreSQL
psql postgres://nexus:nexus@localhost:5432/nexus

# Count deliveries per channel
psql postgres://nexus:nexus@localhost:5432/nexus \
  -c "SELECT channel, status, count(*) FROM notifications GROUP BY 1,2 ORDER BY 1,2;"
```
