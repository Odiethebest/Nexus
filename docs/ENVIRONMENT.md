# Environment Variable Reference

Source of truth for backend defaults is `.env.example`.  
Never commit real secrets (API tokens, SMTP credentials, private keys).

## 1. Shared Infrastructure Connectivity

Used by producer and worker:

- `AMQP_URL`
- `REDIS_URL`
- `POSTGRES_DSN`

## 2. Producer Runtime

- `LISTEN_ADDR`
- `PORT` (optional fallback used to construct default HTTP listen address)
- `GRPC_ADDR`
- `METRICS_INTERNAL_URL` (worker metrics scrape target; defaults to `http://localhost:9091/metrics`)

## 3. Load-Test Control Plane

- `LOADTEST_ENABLED`
- `LOADTEST_ADMIN_KEY`
- `LOADTEST_ALLOWED_ORIGINS`
- `LOADTEST_MAX_PARALLEL`
- `LOADTEST_COOLDOWN_SECONDS`
- `LOADTEST_MIN_START_INTERVAL_SECONDS`
- `LOADTEST_POLL_INTERVAL_SECONDS`
- `LOADTEST_MAX_RUN_SECONDS`
- `LOADTEST_DEMO_RUN_SECONDS`
- `LOADTEST_STATUS_TIMEOUT_SECONDS`
- `LOADTEST_QUERY_TIMEOUT_SECONDS`
- `LOADTEST_REQUEST_TIMEOUT_SECONDS`
- `LOADTEST_UPSTREAM_RETRY_MAX`
- `LOADTEST_UPSTREAM_RETRY_BASE_MS`
- `LOADTEST_UPSTREAM_RETRY_MAX_MS`
- `LOADTEST_CIRCUIT_BREAKER_THRESHOLD`
- `LOADTEST_CIRCUIT_BREAKER_OPEN_SECONDS`
- `LOADTEST_BUDGET_VUH_PER_DAY`
- `K6_API_BASE`
- `K6_API_TOKEN`
- `K6_STACK_ID`
- `K6_LOAD_TEST_ID`

## 4. Worker Runtime and Concurrency

- `EMAIL_WORKER_POOL`
- `INAPP_WORKER_POOL`
- `WEBHOOK_WORKER_POOL`
- `METRICS_ADDR` (worker metrics listen address, default `:9091`)

## 5. SMTP (Email Channel)

- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASS`
- `EMAIL_FROM`

## 6. Frontend (`web/.env.local`)

- `NEXT_PUBLIC_API_URL`
- `NEXT_PUBLIC_WS_URL`
- `METRICS_INTERNAL_URL` (for server-side metric fetch/proxy scenarios)

## 7. Maintenance Rules

- Add new keys to both `.env.example` and this file.
- Remove deprecated keys from both locations in the same change.
- Prefer explicit defaults in code for non-secret operational knobs.
