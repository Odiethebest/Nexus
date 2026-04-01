# Nexus

A production-grade, message-driven notification system built in Go. Nexus routes application events through a RabbitMQ topic exchange to concurrent goroutine-pool workers, delivering notifications across multiple channels with idempotent consumers, dead letter queue handling, and real-time status streaming.

Inspired by patterns from a distributed security auditing platform handling 12,000+ events/sec.

**Live demo:** [nexus.odieyang.com](https://nexus.odieyang.com)

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
                                     (idempotency   (notification  (WebSocket
                                      + rate limit)   history)      live view)
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
| Idempotency / rate limiting | Redis 7 |
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
- **Dead letter queue + exponential backoff** — failed messages retry 3× with 2s/4s/8s delays before landing in DLQ
- **Goroutine pool concurrency control** — configurable per-channel worker pool size
- **Backpressure handling** — publisher confirms + channel flow control under burst traffic
- **Notification history** — full delivery audit trail persisted to PostgreSQL
- **Real-time dashboard** — React frontend streams live delivery status via WebSocket

---

## Project Structure

```
nexus/
├── cmd/
│   ├── producer/          # Event producer service (HTTP API)
│   └── worker/            # Worker service entrypoint
├── internal/
│   ├── broker/            # RabbitMQ connection, channel management, publisher
│   ├── worker/
│   │   ├── email.go       # Email worker + goroutine pool
│   │   ├── inapp.go       # In-app worker + WebSocket hub
│   │   └── webhook.go     # Webhook worker + retry logic
│   ├── idempotency/       # Redis deduplication
│   ├── store/             # PostgreSQL notification history
│   └── hub/               # WebSocket connection manager
├── web/                   # React + Vite frontend
├── deploy/
│   ├── docker-compose.yml
│   └── railway.toml
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
docker compose up -d

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

Messages that fail processing are re-queued with a `x-death` header count. After 3 attempts (exponential backoff: 2s → 4s → 8s), the message is forwarded to `{queue}.dlq` with all original headers intact. A separate DLQ consumer logs and persists failed messages for manual inspection or replay.

This mirrors the retry and DLQ handling built for a high-throughput audit ingestion pipeline at QAX Technology Group.

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

Load tested with [k6](https://k6.io):

| Metric | Result |
|---|---|
| Throughput | X,XXX events/sec |
| p95 processing latency | XXms |
| Duplicate delivery rate | <0.X% |
| DLQ rate under normal load | <0.X% |

*(Numbers updated after load testing)*

---

## Roadmap

- [ ] Priority queue routing (high/normal/low lanes)
- [ ] Prometheus metrics + Grafana dashboard
- [ ] gRPC producer API alongside HTTP
- [ ] Replay endpoint for DLQ messages

---

## License

MIT