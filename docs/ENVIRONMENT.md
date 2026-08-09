# Environment Variable Reference

Source of truth for backend defaults is `.env.example`.
Never commit real secrets (API tokens, SMTP credentials, private keys).

Both binaries auto-load `.env` from the repo root (or one/two directories up)
via `internal/envutil`. Values already present in the process environment are
**not** overridden.

## 1. Message Bus (producer and worker)

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BROKERS` | *(none — required)* | Comma-separated seed brokers. No default on purpose: booting against the wrong cluster silently is worse than not booting. |
| `KAFKA_CLIENT_ID` | `nexus` | |
| `KAFKA_TOPIC_PARTITIONS` | `12` | Must match across services. Derivation in the README. |
| `KAFKA_REPLICATION_FACTOR` | `1` | `3` on Redpanda Cloud dev clusters. |
| `KAFKA_TLS` | `false` | `true` for managed brokers. |
| `KAFKA_SASL_MECHANISM` | *(empty)* | `SCRAM-SHA-256` or `SCRAM-SHA-512`. Defaults to `SCRAM-SHA-256` when `KAFKA_SASL_USER` is set. |
| `KAFKA_SASL_USER` / `KAFKA_SASL_PASS` | *(empty)* | |

## 2. Shared Infrastructure Connectivity

- `REDIS_URL` — default `redis://localhost:6379`. Used for idempotency claims
  (worker) and the cache-aside read path (producer).
- `POSTGRES_DSN` — default
  `postgres://nexus:nexus@localhost:5432/nexus?sslmode=disable`.

## 3. Producer Runtime

- `LISTEN_ADDR` — default `:8080`.
- `PORT` — fallback used to build the default `LISTEN_ADDR`.
- `GRPC_ADDR` — default `:50051`.
- `METRICS_INTERNAL_URL` — worker metrics scrape target for
  `/api/metrics/summary`; defaults to `http://localhost:9091/metrics`.
- `CORS_ALLOWED_ORIGINS` — comma-separated browser origin allow-list. Gates
  both the REST API and the `/ws` WebSocket upgrade. Empty means trust every
  origin (zero-config demo default); set it in production. Same-origin
  requests, and requests with no `Origin` header (curl, loadgen, Prometheus),
  are always allowed. `*` explicitly means allow-all.

## 4. Load-Test Control Plane

- `LOADTEST_ENABLED` — default `false`. Demo mode works regardless; this gates
  the real k6 Cloud path.
- `LOADTEST_ADMIN_KEY` — required when `LOADTEST_ENABLED=true`; the producer
  refuses to start without it.
- `LOADTEST_ALLOWED_ORIGINS` — **deprecated**, superseded by
  `CORS_ALLOWED_ORIGINS`. Read only as a fallback when the latter is unset,
  and logs a warning. It now governs every route, not just `/ops/loadtest`.
- `LOADTEST_MAX_PARALLEL` (`1`), `LOADTEST_COOLDOWN_SECONDS` (`300`),
  `LOADTEST_MIN_START_INTERVAL_SECONDS` (`0`)
- `LOADTEST_POLL_INTERVAL_SECONDS` (`2`), `LOADTEST_MAX_RUN_SECONDS` (`55`),
  `LOADTEST_DEMO_RUN_SECONDS` (`55`)
- `LOADTEST_STATUS_TIMEOUT_SECONDS` (`4`),
  `LOADTEST_QUERY_TIMEOUT_SECONDS` (`3`),
  `LOADTEST_REQUEST_TIMEOUT_SECONDS` (`20`, clamped to 5–30)
- `LOADTEST_UPSTREAM_RETRY_MAX` (`2`), `LOADTEST_UPSTREAM_RETRY_BASE_MS`
  (`250`), `LOADTEST_UPSTREAM_RETRY_MAX_MS` (`2000`)
- `LOADTEST_CIRCUIT_BREAKER_THRESHOLD` (`5`),
  `LOADTEST_CIRCUIT_BREAKER_OPEN_SECONDS` (`30`)
- `LOADTEST_BUDGET_VUH_PER_DAY` (`0` = uncapped)
- `K6_API_BASE` (`https://api.k6.io`), `K6_API_TOKEN`, `K6_STACK_ID`,
  `K6_LOAD_TEST_ID` — the last must be `> 0` when loadtest is enabled

## 5. Worker Runtime and Concurrency

- `EMAIL_WORKER_POOL` (`10`), `INAPP_WORKER_POOL` (`5`),
  `WEBHOOK_WORKER_POOL` (`8`) — the high-priority lane size. Normal and low
  lanes get `pool/2` and `pool/4` (minimum 1).
- `METRICS_ADDR` — worker metrics listen address, default `:9091`.

## 6. SMTP (Email Channel)

- `SMTP_HOST` — leave empty to disable sending. The email processor then
  reports `delivered` without contacting an SMTP server, which is the intended
  local-dev behaviour.
- `SMTP_PORT` (`587` STARTTLS, `465` implicit TLS), `SMTP_USER`, `SMTP_PASS`
- `EMAIL_FROM` — default `Nexus <no-reply@example.com>`

## 7. Frontend (`web/.env.local`)

- `NEXT_PUBLIC_API_URL` — producer base URL. **Build-time**, baked into the
  bundle.
- `NEXT_PUBLIC_WS_URL` — WebSocket URL. Also build-time.
- `METRICS_INTERNAL_URL` — server-side only, for future API-route proxying.

## 8. Maintenance Rules

- Add new keys to both `.env.example` and this file.
- Remove deprecated keys from both locations in the same change.
- Prefer explicit defaults in code for non-secret operational knobs.
