# Deployment Guide

## 1. Deployment Modes

- Local containerized stack via Docker Compose
- Cloud deployment on Railway with split services, backed by a managed
  Redpanda Cloud cluster

## 2. Local Containerized Deployment

`deploy/docker-compose.yml` brings up a full local environment:

- Infrastructure: Redpanda, Redis, PostgreSQL
- Application: producer and worker
- Observability: Prometheus and Grafana

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

The producer and worker services read `../.env` via `env_file`, so
`cp .env.example .env` before the first run.

Primary local ports:

- Producer: `8080`
- Worker metrics: `9091`
- Redpanda Kafka (external): `19092`, Admin API: `9644`
- Prometheus: `9090`
- Grafana: `3000` (admin / admin) — clashes with `npm run dev`; see
  `RUN_LOCAL.md`

The compose file does **not** include the web service; `Dockerfile.web` exists
for Railway. Prometheus scrapes three jobs: the producer, the worker, and
Redpanda's own `/public_metrics`.

### Grafana dashboards

One board, `deploy/grafana/dashboards/nexus-kafka.json` — **"Nexus — Kafka
pipeline"** — provisioned into the `Nexus` folder and set as the default home
dashboard: consumer lag by lane, e2e lag p99 with a 1.5s threshold, DLQ
backlog by lane, `by_id` cache hit rate with a 0.95 threshold, three-stage
p99, publish/processed rate, delivery success rate, and processed rate split
by channel and status.

The pre-Kafka board (`deploy/grafana/dashboard.json`) was removed: it was
still wired as the default home page, and its latency panels queried
`nexus_publish_duration_seconds`, a metric nothing has observed since the
Kafka cutover, so they rendered permanently empty. Its two unique views —
delivery rate by channel and duplicate-skip rate — are folded into the
"Processed rate by channel and status" panel. The metric itself has been
dropped from `internal/metrics`.

## 3. Railway Service Profiles

Three independently deployable services:

- Producer: `deploy/railway.toml` -> `deploy/Dockerfile.producer`
- Worker: `deploy/railway.worker.toml` -> `deploy/Dockerfile.worker`
- Web: `deploy/railway.web.toml` -> `deploy/Dockerfile.web`

Railway has no built-in Kafka, so point both Go services at a **Redpanda
Cloud** dev cluster:

| Variable | Value |
|---|---|
| `KAFKA_BROKERS` | `<seed>.redpanda.cloud:9092` |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-256` |
| `KAFKA_SASL_USER` / `KAFKA_SASL_PASS` | cluster credentials |
| `KAFKA_TLS` | `true` |
| `KAFKA_TOPIC_PARTITIONS` | `12` (must match across services) |
| `KAFKA_REPLICATION_FACTOR` | `3` (dev clusters run 3 nodes) |

Notes on the other config files:

- A root `railway.toml` also exists and duplicates the producer profile; the
  `deploy/` profiles are the source of truth.
- Root `nixpacks.toml` builds the producer binary only and does **not**
  participate in Railway deploys (Railway uses the Dockerfiles).

## 4. Build Characteristics

### Producer and Worker

- Go multi-stage builds on `golang:1.25-alpine`
- Final runtime images carry only the binary and CA certificates

### Web

- Next.js standalone output (`output: "standalone"`)
- `NEXT_PUBLIC_API_URL` and `NEXT_PUBLIC_WS_URL` are **build-time** args, not
  runtime env — set them before building or the bundle points at the wrong
  host. `deploy/railway.web.toml` currently hardcodes
  `https://nexus.odieyang.com`.
- Runtime entrypoint: `node server.js`

## 5. Zero-Downtime Rolling Updates

Railway's default strategy is rolling: the new instance must pass its health
check before the old one is sent `SIGTERM`.

On the Kafka side the consumer group coordinator moves partitions from the old
worker to the new one. The runner uses `BlockRebalanceOnPoll`, so revocation
only happens between poll cycles, and on `SIGTERM` it drains in-flight records
and commits before exiting.

`RUNBOOK.md` documents the local proxy for this
(`docker compose restart worker` under load) and the measured result.

## 6. Data and Migration Behavior

- Both producer and worker run `store.Migrate` at startup (`CREATE TABLE IF
  NOT EXISTS`), so either can be deployed first.
- The producer also runs `kbroker.EnsureTopics` at boot. It is best-effort:
  failures are logged and startup continues, so a managed cluster without
  topic-create rights still works if topics are pre-created.
- Ensure `POSTGRES_DSN` is valid and reachable before the first deploy.

## 7. Pre-Deployment Checklist

- Required environment variables configured (see `ENVIRONMENT.md`).
- `CORS_ALLOWED_ORIGINS` set to the web origin. Leaving it empty trusts every
  origin, which is the demo default and not what you want in production. It
  gates both the REST API and the `/ws` upgrade — if it does not include the
  web origin, the frontend gets `403`.
- Redpanda, Redis, and PostgreSQL connectivity verified.
- Producer health check (`/health`) returns HTTP `200`.
- Web build-time public URLs target the correct producer domain and WebSocket
  endpoint.
