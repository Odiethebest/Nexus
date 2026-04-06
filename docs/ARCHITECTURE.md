# Nexus Architecture

## 1. Purpose

Nexus is an event-driven notification system used to demonstrate production-oriented backend engineering patterns:

- asynchronous event ingestion and fan-out delivery
- multi-channel processing (`email`, `inapp`, `webhook`)
- priority-aware queue topology and dead-letter replay
- Redis-based idempotency
- PostgreSQL-backed durable history
- operational visibility through metrics and dashboarding

## 2. Runtime Components

### Producer (`cmd/producer`)

- Serves HTTP and gRPC ingestion endpoints
- Exposes WebSocket endpoint for live event subscribers
- Hosts load-test control endpoints (`/ops/loadtest/*`)
- Exposes operational endpoints (`/api/metrics/summary`, `/metrics`, `/health`)

### Worker (`cmd/worker`)

- Consumes RabbitMQ queues for all channels and priority lanes
- Performs idempotency checks using Redis (`message_id` based)
- Executes channel-specific delivery logic
- Persists delivery records to PostgreSQL
- Exposes Prometheus metrics (default `:9091/metrics`)

### Infrastructure Dependencies

- RabbitMQ: topic exchange and queue routing
- Redis: deduplication state with TTL
- PostgreSQL: notification persistence

## 3. End-to-End Message Flow

1. A client publishes an event through `POST /events` or gRPC `Publish`.
2. Producer publishes to the `nexus.events` topic exchange.
3. Routing key pattern `event.{type}.{priority}` fans out into 3 channels x 3 priority lanes.
4. Worker processing pipeline:
   - idempotency check in Redis
   - channel delivery (SMTP / WebSocket broadcast / outbound HTTP webhook)
   - upsert delivery record in PostgreSQL
   - `ACK` on success, `NACK` on failure (dead-letter on configured paths)
5. Frontend dashboards consume REST and WebSocket data for monitoring and operations.

## 4. Queue Topology

Each delivery channel is split into `high`, `normal`, and `low` priority queues and corresponding DLQs.

- Email: `nexus.email.{priority}` + `nexus.email.dlq.{priority}`
- In-app: `nexus.inapp.{priority}` + `nexus.inapp.dlq.{priority}`
- Webhook: `nexus.webhook.{priority}` + `nexus.webhook.dlq.{priority}`

## 5. Core Data Model

The `notifications` table stores one delivery record per `(message_id, channel)`:

- `message_id`
- `channel`
- `event_type`
- `status`
- `payload` (`JSONB`)
- `created_at`

Primary key: `(message_id, channel)`.

This schema enables one-to-many channel tracking for a single logical event while preserving idempotent updates.
