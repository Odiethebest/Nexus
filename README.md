# Nexus

A production-grade, message-driven notification system built in Go. Nexus routes application events through a RabbitMQ topic exchange to concurrent goroutine-pool workers, delivering notifications across multiple channels with idempotent consumers, dead letter queue handling, and persisted delivery history.

Inspired by patterns from a distributed security auditing platform handling 12,000+ events/sec.

**Live demo:** [nexus.odieyang.com](https://nexus.odieyang.com)

## Start Here

New to Nexus or evaluating the dashboard for the first time?

- English beginner guide: [doc/intro-en.md](doc/intro-en.md)
- 中文入门说明: [doc/intro-zh.md](doc/intro-zh.md)

---

## Architecture

```
[Event Producers] ──► RabbitMQ Topic Exchange ──► [Queues + DLQ]
                       routing key:                     │
                       event.{type}.{priority}          ▼
                                              [Go Worker Pools]
                                               ├── Email worker
                                               ├── In-app worker
                                               └── Webhook worker
                                                        │
                                          ┌─────────────┼─────────────┐
                                          ▼             ▼             ▼
                                        Redis       PostgreSQL   React Dashboard
                                     (idempotency   (notification  (embedded UI
                                        cache)        history)       + /ws client)
```

### Why this design

**Topic exchange over direct/fanout** — routing key `event.{type}.{priority}` lets consumers subscribe selectively (e.g. `event.order.*` for all order events). Adding a new channel requires binding a new queue — no producer changes needed.

**Per-channel queues with DLQ** — each delivery channel (email, in-app, webhook) has an independent queue and dead letter queue. A failure in the webhook worker never blocks email delivery. Failed messages are routed to the DLQ with original headers preserved for inspection and replay.

**Goroutine pool over unbounded `go func()`** — worker concurrency is capped via a semaphore channel. This prevents downstream services (SMTP, third-party webhooks) from being overwhelmed during traffic bursts, mirroring the backpressure strategy used in production audit pipelines.

**Redis idempotency** — each message carries a `message_id`. Before processing, workers check `SET msg:{id} 1 NX EX 86400`. If the key exists, the message is acknowledged and skipped. This turns at-least-once delivery into effectively exactly-once without distributed transactions.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Go 1.22 |
| Message broker | RabbitMQ 3.13 |
| Idempotency cache | Redis 7 |
| Persistence | PostgreSQL 16 |
| Frontend | React + Vite |
| Real-time | WebSocket (gorilla/websocket) |
| Containerization | Docker + Docker Compose |
| Deployment | Railway |

---

## Features

- **Topic-based routing** — flexible event routing via RabbitMQ exchange bindings
- **Multi-channel delivery** — email, in-app notifications, and outbound webhooks
- **Idempotent consumers** — Redis-backed deduplication prevents duplicate delivery on worker restart
- **Dead letter queue handling** — per-channel priority queues are configured with dedicated DLQs
- **Webhook retries with exponential backoff** — 2s/4s/8s retry schedule before DLQ
- **Goroutine pool concurrency control** — configurable per-channel worker pool size
- **Backpressure handling** — publisher confirms + per-lane QoS prefetch limits
- **Notification history** — full delivery audit trail persisted to PostgreSQL
- **Embedded dashboard frontend** — React bundle served by the producer via `go:embed`
- **One-click Stress Lab** — start/poll cloud load tests from dashboard with backend guardrails

---

## Project Structure

```
nexus/
├── cmd/
│   ├── producer/          # Event producer service (HTTP API)
│   └── worker/            # Worker service entrypoint
├── internal/
│   ├── broker/            # RabbitMQ connection, channel management, publisher
│   ├── loadtest/          # k6 client, guardrails, insight scoring
│   ├── worker/
│   │   ├── email.go       # Email worker + goroutine pool
│   │   ├── inapp.go       # In-app worker + WebSocket hub
│   │   └── webhook.go     # Webhook worker + retry logic
│   ├── idempotency/       # Redis deduplication
│   ├── store/             # PostgreSQL notification history
│   └── hub/               # WebSocket connection manager
├── scripts/               # Operational scripts (manual loadtest E2E checks)
├── web/                   # React + Vite frontend
├── deploy/
│   ├── docker-compose.yml
│   ├── railway.toml
│   └── railway.worker.toml
├── railway.toml            # Root Railway config (producer)
└── README.md
```

---

## Getting Started

### Prerequisites

- Go 1.22+
- Docker + Docker Compose

### Run locally

```bash
git clone https://github.com/Odiethebest/nexus
cd nexus

# Start RabbitMQ, Redis, PostgreSQL
docker compose -f deploy/docker-compose.yml up -d rabbitmq redis postgres

# Start worker
go run ./cmd/worker

# Start producer API
go run ./cmd/producer

# Start frontend
cd web && npm install && npm run dev
```

The producer API is available at `http://localhost:8080`.
RabbitMQ management UI at `http://localhost:15672` (guest/guest).
React dashboard at `http://localhost:5173`.

### Publish a test event

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "type": "order",
    "priority": "high",
    "payload": { "user_id": "u123", "order_id": "o456" }
  }'
```

---

## Key Design Decisions

### Retry and DLQ strategy

Each worker lane queue is configured with `x-dead-letter-routing-key` to a matching `{queue}.dlq`.

- **Webhook worker** retries on delivery failures with exponential backoff (2s → 4s → 8s) based on `x-death` count, then routes to DLQ after max retries.
- **Email / In-app workers** acknowledge duplicates, requeue transient failures, and dead-letter malformed payloads.

The producer exposes `POST /dlq/replay` for manual replay. There is currently no dedicated background DLQ consumer.

### WebSocket hub scope

The producer and worker are separate binaries and each creates its own in-memory `hub.Hub`. Without an external bridge (for example Redis pub/sub), in-app broadcasts emitted by the worker are process-local and are not automatically visible to producer-hosted `/ws` clients.

### Goroutine pool implementation

Each worker channel runs a fixed-size pool controlled by a buffered semaphore:

```go
sem := make(chan struct{}, poolSize)

for msg := range msgs {
    sem <- struct{}{}
    go func(d amqp.Delivery) {
        defer func() { <-sem }()
        process(d)
    }(msg)
}
```

Pool size is configurable per channel via environment variables (`EMAIL_WORKER_POOL`, `INAPP_WORKER_POOL`, `WEBHOOK_WORKER_POOL`).

### Idempotency key design

```
key:   msg:{message_id}
value: 1
TTL:   24h (covers any realistic redelivery window)
```

`message_id` is generated by the producer as a UUIDv7 (time-ordered), enabling both deduplication and chronological sorting of notification history without a separate timestamp index.

---

## Performance

Stress Lab uses Grafana Cloud k6. Measured production-like numbers depend on your k6 scenario, target environment, and Railway resource tier.

To run and capture reproducible metrics for your deployment:

1. Configure `LOADTEST_*` + `K6_*` producer variables.
2. Use dashboard **Start Load Test** or run the manual checklist script in [doc/testing.md](doc/testing.md).
3. Record the final score, `RPS`, `P95`, and `Error %` shown in the completed run summary.

---

## License

MIT
