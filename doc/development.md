# Development Guide

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Go | ≥ 1.22 | Backend binaries |
| Node.js | ≥ 18 | Frontend (Vite + React) |
| Docker + Compose | any recent | Infrastructure services |

---

## Quick Start

```bash
# 1. Clone and enter the repo
git clone <repo-url>
cd nexus

# 2. Copy env file
cp .env.example .env
# Edit .env if needed (defaults work with docker-compose)

# 3. Bring up infrastructure
docker compose -f deploy/docker-compose.yml up -d rabbitmq redis postgres

# 4. Run producer (in one terminal)
go run ./cmd/producer

# 5. Run worker (in another terminal)
go run ./cmd/worker

# 6. Run frontend dev server (optional)
cd web && npm install && npm run dev
```

The frontend dev server proxies `/events`, `/notifications`, `/ws`, and `/dlq/replay` to `localhost:8080` via Vite's proxy config.

---

## Environment Variables

Both binaries read configuration from environment variables. Copy `.env.example` to `.env` and source it, or set variables directly.

### Producer (`cmd/producer`)

| Variable | Default | Description |
|----------|---------|-------------|
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection URL |
| `POSTGRES_DSN` | `postgres://nexus:nexus@localhost:5432/nexus?sslmode=disable` | PostgreSQL DSN |
| `LISTEN_ADDR` | `:8080` | HTTP server listen address |
| `GRPC_ADDR` | `:50051` | gRPC server listen address |

### Worker (`cmd/worker`)

| Variable | Default | Description |
|----------|---------|-------------|
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection URL |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `POSTGRES_DSN` | `postgres://nexus:nexus@localhost:5432/nexus?sslmode=disable` | PostgreSQL DSN |
| `EMAIL_WORKER_POOL` | `10` | Email goroutine pool size |
| `INAPP_WORKER_POOL` | `5` | In-app goroutine pool size |
| `WEBHOOK_WORKER_POOL` | `8` | Webhook goroutine pool size |
| `SMTP_HOST` | *(empty)* | SMTP hostname — email disabled if unset |
| `SMTP_PORT` | `587` | SMTP port (587 = STARTTLS, 465 = implicit TLS) |
| `SMTP_USER` | *(empty)* | SMTP username |
| `SMTP_PASS` | *(empty)* | SMTP password / API key |
| `EMAIL_FROM` | `Nexus <no-reply@example.com>` | Sender address |
| `METRICS_ADDR` | `:9091` | Worker Prometheus metrics listen address |

---

## Building Binaries

```bash
# Producer
go build -o producer ./cmd/producer

# Worker
go build -o worker ./cmd/worker
```

Docker multi-stage builds are in `deploy/Dockerfile.producer` and `deploy/Dockerfile.worker`.

---

## Project Structure

```
nexus/
├── cmd/
│   ├── producer/main.go   HTTP + gRPC server, event publisher
│   └── worker/main.go     Email / in-app / webhook consumer
├── internal/
│   ├── broker/            AMQP connection, publisher, priority lanes
│   ├── grpcserver/        gRPC service (JSON codec, no protoc)
│   ├── hub/               WebSocket fan-out hub
│   ├── idempotency/       Redis SET NX deduplication
│   ├── mailer/            SMTP email sender
│   ├── metrics/           Prometheus counters and histograms
│   ├── replay/            DLQ message replay
│   ├── store/             PostgreSQL notification persistence
│   └── worker/            email.go, inapp.go, webhook.go consumers
├── proto/
│   └── event.proto        gRPC service definition (documentation only)
├── web/
│   ├── index.html
│   └── src/               React + Vite frontend
├── deploy/
│   ├── docker-compose.yml
│   ├── Dockerfile.producer
│   ├── Dockerfile.worker
│   ├── prometheus.yml
│   └── grafana/
└── doc/                   Engineering documentation
```

---

## HTTP API Reference

### `POST /events`

Publishes an event. Returns the assigned message ID.

```bash
curl -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"type":"order","priority":"high","payload":{"user_id":"u1"}}'
# → {"message_id":"019..."}
```

**Request body:**

| Field | Type | Required | Values |
|-------|------|----------|--------|
| `type` | string | yes | any (e.g. `order`, `payment`) |
| `priority` | string | yes | `high`, `normal`, `low` |
| `payload` | object | no | arbitrary JSON |

### `GET /notifications`

Returns the 50 most recent delivery records.

```bash
curl http://localhost:8080/notifications
```

### `POST /dlq/replay`

Republishes up to `max` messages from a dead-letter queue back to the main exchange.

```bash
curl -X POST http://localhost:8080/dlq/replay \
  -H 'Content-Type: application/json' \
  -d '{"queue":"nexus.email.high.dlq","max":50}'
# → {"replayed":3}
```

### `GET /health`

Returns `200 OK`. Used as a container liveness probe.

---

## gRPC API Reference

Service: `event.v1.EventService` on port `:50051`.

The server uses a JSON codec registered under the `"proto"` name — no generated protobuf stubs required. Any gRPC client that sends `Content-Type: application/grpc+json` can call it directly.

Schema is documented in `proto/event.proto`.

```bash
# Example with grpcurl
grpcurl -plaintext -d '{"type":"order","priority":"high","payload":{"user_id":"u1"}}' \
  localhost:50051 event.v1.EventService/Publish
```
