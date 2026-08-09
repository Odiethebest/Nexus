# Nexus Architecture

> Message bus is **Redpanda** (Kafka protocol, KRaft, no ZooKeeper), accessed
> through the `franz-go` client on both the produce and consume sides. The
> RabbitMQ implementation this project started from was removed in full; see
> `MIGRATION.md` for the transition record.

## 1. Purpose

Nexus is an event-driven notification system used to demonstrate
production-oriented backend engineering patterns:

- asynchronous event ingestion and fan-out delivery
- multi-channel processing (`email`, `inapp`, `webhook`)
- priority-aware lane topology and dead-letter replay
- Redis-based idempotency and a cache-aside read path
- PostgreSQL-backed durable history
- operational visibility through three-stage latency tracing and dashboards

## 2. Runtime Components

### Producer (`cmd/producer`)

- HTTP API on `:8080`, gRPC `Publish` on `:50051`
- WebSocket endpoint (`GET /ws`) for live event subscribers
- Cache-aside read path in front of the notifications store
- DLQ replay endpoint (`POST /dlq/replay`)
- Consumer-lag sampler (`internal/kbroker.LagReader`, 3s cadence via `kadm`)
- Load-test control endpoints (`/ops/loadtest/*`)
- Operational endpoints: `/api/metrics/summary`, `/metrics`, `/health`
- Creates all topics at boot (`kbroker.EnsureTopics`); failure is logged, not
  fatal, so it can run against a managed cluster where it lacks admin rights
- Consumes nothing

### Worker (`cmd/worker`)

- Nine lane runners: 3 channels x 3 priorities, each its own consumer group
- Redis idempotency claim per `(channel, message_id)`
- Channel-specific delivery (SMTP / WebSocket broadcast / outbound HTTP POST)
- Persists delivery records to PostgreSQL
- Exposes Prometheus metrics on `:9091` (`METRICS_ADDR`)
- Makes no outbound HTTP other than webhook delivery

### Infrastructure Dependencies

- Redpanda: Kafka-protocol broker, single binary in KRaft mode
- Redis: idempotency claims *and* the cache-aside read cache
- PostgreSQL: notification history

## 3. Topology

Every `(channel, priority)` pair is a separate topic **and** a separate
consumer group, so a stuck lane never gates committed offsets on another one.

| Kind | Name | Count |
|---|---|---|
| Primary lane topic | `nexus.<channel>.<priority>` | 9 |
| Dead-letter topic | `nexus.dlq.<channel>.<priority>` | 9 |
| Lane consumer group | `nexus.<channel>.<priority>` | 9 |
| Replay consumer group | `nexus.replay` | 1 |

Partition count comes from `KAFKA_TOPIC_PARTITIONS` (default 12); the
derivation is in the README "Partition sizing" section. Worker pool sizes per
lane are `[pool, pool/2, pool/4]` for high/normal/low.

`kbroker.NormalizeDLQTopic` accepts the legacy AMQP-style name
`nexus.<channel>.dlq.<priority>` and maps it onto the Kafka-native form, so
older clients calling `POST /dlq/replay` keep working.

## 4. End-to-End Message Flow

1. A client publishes through `POST /events` or gRPC `Publish`.
2. `kbroker.Publisher` generates a UUIDv7 and **fans the event out to all
   three channel lane topics** — one record each, keyed by `message_id`, with
   headers `x-msg-id`, `x-event-type`, `x-priority`, `x-produced-at`
   (unix nanoseconds), `x-retry-count`. Produce is async with `acks=all` and
   the idempotent producer; `Publish` returns once every fan-out target acks.
3. The lane runner (`kworker.Runner.handle`) for each record:
   - observes `now - x-produced-at` into the e2e-lag histogram
   - takes a Redis `SETNX` claim on `msg:<channel>:<id>` (TTL 24h)
   - dispatches via the channel `Processor`, which returns an `Outcome`
   - persists to PostgreSQL
   - commits the offset — **only after persistence**, which is what makes the
     pipeline at-least-once
4. The in-app processor broadcasts the raw event JSON to the WebSocket hub;
   the frontend reads it directly.

### Failure handling

| Outcome | Behaviour |
|---|---|
| `Delivered` | persist `delivered`, commit |
| `Skipped` | persist `skipped`, commit (e.g. no `webhook_url` in the payload) |
| `TransientError`, budget remaining | persist `failed`, **release the idempotency claim**, back off 2s/4s/8s, re-produce to the same topic with `x-retry-count++`, commit |
| `TransientError`, budget exhausted | produce to the DLQ topic, persist `dlq`, commit |
| `PermanentError` | straight to the DLQ topic, persist `dlq`, commit |
| Malformed JSON body | straight to the DLQ topic, commit (no row — the event never parsed) |
| Duplicate (claim held **and** row present in PostgreSQL) | commit, no new row |
| Claim held but **no** row in PostgreSQL | reprocess — the previous holder died mid-flight |

`MaxRetries` is 3, so a record sees at most four delivery attempts.

The claim release on the retry path is load-bearing: the re-produced record
carries the same `message_id` into the same lane, so leaving the claim in
place would make every retry fail the dedup check and be dropped as a
duplicate.

The claim is also not trusted on its own. It is taken *before* the work, so
its presence proves only that some worker started. A worker killed between
the `SETNX` and the row write (SIGKILL, OOM, eviction) leaves a claim with
no row, and skipping on that basis would drop the message for the full 24h
TTL. The duplicate path therefore confirms against PostgreSQL — a
primary-key lookup, and only on that path. Two workers racing the same
record can both conclude "not done" and both deliver; that is the correct
trade for an at-least-once pipeline, and the `(message_id, channel)` upsert
keeps history single-rowed regardless.

### Rebalance and shutdown

`BlockRebalanceOnPoll` defers revocation to the next `AllowRebalance`, which
the runner calls as soon as a batch is dispatched — while the pool is still
working. An `OnPartitionsRevoked` hook therefore blocks the rebalance until
in-flight records finish, so a partition never moves out from under a
goroutine that is still processing it.

On shutdown the runner drains in-flight and closes, but deliberately does
not bulk-commit. franz-go refreshes its "uncommitted" set on every
`PollFetches`, so committing it would also commit records the handler left
uncommitted on purpose (a Redis error, a failed retry re-produce) and
silently drop them. Every record that completed already committed its own
offset.

Webhook verdicts: 5xx and 429 are transient; other 4xx are permanent (the
upstream is not going to start accepting it); a malformed URL is permanent.

## 5. Core Data Model

`notifications` stores one delivery record per `(message_id, channel)`:

| Column | Notes |
|---|---|
| `message_id` | UUIDv7 from the publisher |
| `channel` | `email` \| `inapp` \| `webhook` |
| `event_type` | caller-supplied |
| `status` | `delivered` \| `skipped` \| `failed` \| `dlq` |
| `priority` | `high` \| `normal` \| `low`, `NOT NULL DEFAULT 'normal'` |
| `payload` | `JSONB`, the full event envelope |
| `created_at` | `TIMESTAMPTZ`, indexed `DESC` |

Primary key `(message_id, channel)`; `SaveNotification` upserts on it, so a
retry that eventually succeeds overwrites its own earlier `failed` row.

`Migrate` runs on boot in both services and is guarded by a
transaction-scoped advisory lock — `CREATE TABLE IF NOT EXISTS` races between
two sessions and one dies on `pg_type_typname_nsp_index`. When `priority` was
added, existing rows were backfilled from `payload->>'priority'` rather than
defaulted, because the stored envelope already carried the real value.

There is no `duplicate` status: a duplicate is committed without persisting,
so it appears only as the
`nexus_messages_processed_total{status="duplicate"}` counter label.

## 6. Caching

| Path | Key | TTL | Scope label |
|---|---|---|---|
| `GET /notifications/{message_id}` | `cache:notif:v2:<id>` | 60s | `by_id` |
| `GET /notifications` | `cache:notif:list:v2:<limit>` | 2s | `list` |

`by_id` is the hot path and the one the RUNBOOK's hit-rate figure measures.
A Redis error on read propagates; a failed refill is swallowed (the next read
simply misses again).

## 7. Observability

Three-stage latency tracing splits where time goes:

| Metric | Stage |
|---|---|
| `nexus_stage_ingest_duration_seconds{channel,priority}` | producer submit to broker ack |
| `nexus_stage_processing_duration_seconds{channel}` | worker fetch to just before commit |
| `nexus_stage_delivery_duration_seconds{channel}` | the dispatch call alone |
| `nexus_event_e2e_lag_seconds{channel}` | `now - x-produced-at` at pick-up |

`x-produced-at` is preserved across retries and DLQ round-trips, so the e2e
histogram measures true event age rather than time since the last attempt.

Backlog is reported two ways: `nexus_consumer_lag_records{channel,priority}`
(offset gap, sampled by the producer) and the e2e-lag histogram (event age).

See `RUNBOOK.md` for the metric-to-claim mapping and reproduction commands.
