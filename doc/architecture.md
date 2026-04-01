# Architecture

## Overview

Nexus is a notification fan-out system. A single published event is routed to three independent worker channels — email, in-app broadcast, and webhook — each backed by its own queue lanes and goroutine pools.

```
Client
  │
  ├── POST /events  (HTTP)
  └── EventService.Publish  (gRPC)
          │
          ▼
     Producer binary
          │
          ▼
   RabbitMQ topic exchange  (nexus.events)
   routing key: event.{type}.{priority}
          │
    ┌─────┼──────┐
    ▼     ▼      ▼
  email  inapp  webhook   ← three queues per priority lane (high/normal/low)
    │     │      │
    ▼     ▼      ▼
  Worker binary (goroutine pools)
    │     │      │
    │   WebSocket Hub     ← process-local broadcast hub in worker process
    │     │      │
    └─────┴──────┘
          │
          ▼
      PostgreSQL          ← notification history, upsert by (message_id, channel)
      Redis               ← idempotency keys, SET NX EX 86400
```

Important runtime note: `cmd/producer` and `cmd/worker` each instantiate their own in-memory `hub.Hub`. Without an external bridge (for example Redis pub/sub), worker in-app broadcasts are not automatically visible on producer-hosted `/ws` connections.

---

## Binaries

### `cmd/producer`

Accepts inbound events and owns the HTTP + gRPC surface:

| Route | Method | Purpose |
|-------|--------|---------|
| `/events` | POST | Publish event to exchange |
| `/notifications` | GET | Last 50 delivery records |
| `/dlq/replay` | POST | Re-queue messages from a DLQ |
| `/ws` | GET | WebSocket upgrade endpoint served by producer hub |
| `/metrics` | GET | Prometheus scrape endpoint |
| `/health` | GET | Liveness probe |

gRPC on `:50051` — `event.v1.EventService/Publish` mirrors the HTTP endpoint.

### `cmd/worker`

Three concurrent worker groups share one process. Each group opens three AMQP channels (one per priority lane) and runs a goroutine pool per lane.

| Worker | Queue prefix | Pool env var | Default pool |
|--------|-------------|--------------|--------------|
| email | `nexus.email` | `EMAIL_WORKER_POOL` | 10 |
| inapp | `nexus.inapp` | `INAPP_WORKER_POOL` | 5 |
| webhook | `nexus.webhook` | `WEBHOOK_WORKER_POOL` | 8 |

---

## Internal Packages

| Package | Responsibility |
|---------|---------------|
| `broker/connection` | AMQP connection with exponential-backoff reconnect (1 s → 30 s). `RWMutex`-protected shared channel for consumers; dedicated confirm-mode channel for publishers. |
| `broker/publisher` | Serialises `Event` to JSON, publishes with routing key `event.{type}.{priority}`, waits for broker ack. Retries once on channel failure. |
| `broker/priority` | Declares the three `Lane` descriptors and their binding patterns. `OpenChannel()` opens an independent AMQP channel from the live connection. |
| `store` | PostgreSQL wrapper. Auto-migrates `notifications` table on startup. `SaveNotification` is an upsert — concurrent workers for the same message update the same row. |
| `idempotency` | `SET NX EX 86400` on key `msg:{messageID}`. Returns `true` (first-seen) or `false` (duplicate). Empty IDs always pass. |
| `hub` | Process-local WebSocket fan-out hub. `Broadcast` is non-blocking — slow clients are dropped to protect throughput. |
| `mailer` | SMTP with STARTTLS (port 587) or implicit TLS (port 465). No-op when `SMTP_HOST` is unset. |
| `metrics` | Prometheus counters/histograms registered in `init()`. Imported for side effects in both binaries. |
| `replay` | Drains a DLQ via `basic.get`, recovers the original routing key from the `x-death` header, republishes to `nexus.events`. |
| `grpcserver` | JSON codec registered under the `"proto"` name avoids a protoc dependency while keeping standard gRPC wire framing. Manual `grpc.ServiceDesc` mirrors what `protoc-gen-go-grpc` would generate. |

---

## Message Lifecycle

```
1. Publisher assigns UUID v7 message ID, timestamps the event.
2. Event is serialised to JSON and published to nexus.events exchange.
3. Exchange routes to three queues based on {priority} in routing key.
4. Each worker lane pulls from its queue (proportional QoS prefetch).
5. Worker checks idempotency — duplicate → ack + skip.
6. Worker delivers (SMTP / in-process hub broadcast / HTTP POST).
7. Worker upserts delivery record to PostgreSQL.
8. Failure handling is channel-specific: webhook retries with 2s/4s/8s backoff then DLQs; email/inapp ack duplicates, requeue transient failures, and dead-letter malformed payloads.
```

---

## Priority Lanes

Each channel (email, inapp, webhook) has three queues:

| Lane | Queue name (email example) | Prefetch |
|------|---------------------------|---------|
| high | `nexus.email.high` | `poolSize` |
| normal | `nexus.email.normal` | `max(poolSize/2, 1)` |
| low | `nexus.email.low` | `max(poolSize/4, 1)` |

Dead-letter queues use the same names with `.dlq` suffix (`nexus.email.high.dlq`, etc.).

---

## Webhook Retry Policy

| Attempt | Backoff before retry |
|---------|---------------------|
| 1st retry | 2 s |
| 2nd retry | 4 s |
| 3rd retry | 8 s |
| 4th failure | Dead-lettered to DLQ |

Retry count is read from the `x-death` AMQP header so the counter survives worker restarts.

---

## Data Store

### PostgreSQL — `notifications` table

```sql
CREATE TABLE notifications (
    message_id  TEXT        NOT NULL,
    channel     TEXT        NOT NULL,
    event_type  TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    payload     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, channel)
);
CREATE INDEX ON notifications (created_at DESC);
```

A single publish produces up to **three rows** — one per channel — all sharing the same `message_id`.

### Redis — idempotency keys

```
Key:    msg:{uuid}
Value:  1
TTL:    24 hours
```
