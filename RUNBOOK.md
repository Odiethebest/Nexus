# RUNBOOK — how to reproduce every résumé bullet

Every bullet below is backed by **live code + a Prometheus metric or a shell command**. Numbers in the "measured" columns come from a real run on the local docker compose stack (`docker compose -f deploy/docker-compose.yml up -d`), M-series laptop, 2026-07-01.

---

## Prerequisites

```bash
# 1. Start the stack (Redpanda + Redis + PostgreSQL + producer + worker + Prometheus + Grafana)
docker compose -f deploy/docker-compose.yml up -d --build

# 2. Wait for producer to become healthy
curl -sf http://localhost:8080/health

# 3. Grafana: http://localhost:3000  (admin / admin)
# 4. Prometheus: http://localhost:9090
# 5. Redpanda console (optional):  docker exec deploy-redpanda-1 rpk topic list
```

To generate load with the in-repo Go loadgen:

```bash
go build -o bin/loadgen ./cmd/loadgen
./bin/loadgen -base http://localhost:8080 -rate 150 -dur 45s -read-ratio 20.0
```

---

## Bullet 1 — Go + Kafka pipeline; partition tuning + consumer-group scaling; p99 < 50ms; consumer lag < 1.5s

| Claim | Code location | How to verify | Measured on local |
|---|---|---|---|
| Kafka pipeline (via Redpanda) | `internal/kbroker/publisher.go`, `internal/kworker/runner.go` | Publish an event: `curl -X POST http://localhost:8080/events -H 'content-type: application/json' -d '{"type":"payment.completed","priority":"high","payload":{}}'`. Then `docker exec deploy-redpanda-1 rpk topic consume nexus.email.high -n 1 --offset -1` — the record + headers (x-msg-id, x-produced-at, x-retry-count) show up. | ✓ |
| Partition tuning | `internal/kbroker/config.go` (`KAFKA_TOPIC_PARTITIONS`), `deploy/docker-compose.yml` (env=12). Derivation lives in the README section "Partition sizing" — target throughput ÷ per-partition throughput (5–10K msg/s) + ~30% headroom → 12 partitions covers a 50K/s ceiling. | `docker exec deploy-redpanda-1 rpk topic describe nexus.email.high` shows 12 partitions. Halve `KAFKA_TOPIC_PARTITIONS` in compose and re-observe throughput drop. | 12 partitions |
| Consumer-group scaling | Nine independent groups, one per (channel, priority) — `internal/kbroker/topics.go:ConsumerGroup`. | `docker exec deploy-redpanda-1 rpk group list` prints all nine `Stable` groups. Scale via `docker compose up -d --scale worker=2`, then `rpk group describe nexus.email.high` — partitions are split across the two workers. | 9 groups, 12 partitions each |
| **Publish p99 < 50ms** | Histogram `nexus_stage_ingest_duration_seconds` emitted by `internal/kbroker/publisher.go`. Panel: **Grafana → Nexus — Kafka pipeline → Three-stage latency p99 (ms)** ingest series. | `histogram_quantile(0.99, sum by (le)(rate(nexus_stage_ingest_duration_seconds_bucket[1m])))` in Prometheus. Loadgen prints the same as `publish_p99_ms`. | **13.3 ms** ✓ |
| **Consumer lag < 1.5s** (event-age p99) | Histogram `nexus_event_e2e_lag_seconds` — the worker observes `now − x-produced-at` in `internal/kworker/runner.go:handle`. Panel: **Grafana → Nexus — Kafka pipeline → End-to-end lag p99 (seconds)** with a 1.5s threshold line. Also exposed in `/api/metrics/summary` as `e2e_lag_p99_seconds`. | `histogram_quantile(0.99, sum by (le, channel)(rate(nexus_event_e2e_lag_seconds_bucket[1m])))`. | **24.7 ms (0.025s)** ✓ |
| Consumer offset lag (records) | Gauge `nexus_consumer_lag_records{channel,priority}` from `internal/kbroker/lag.go` (3s cadence via kadm). Panel: **Grafana → Consumer lag (records) by lane**. Summary endpoint puts the same into `queue_depth`. | `curl -s http://localhost:8080/api/metrics/summary | jq '.queue_depth'`. | 0 at steady state; briefly 6 after burst |

### Which e2e-lag p99 to read (measurement honesty)

`/api/metrics/summary` computes `e2e_lag_p99_seconds` from **cumulative**
histogram buckets, so it is a lifetime p99 for the process: a single restart
permanently raises it and it never decays. Measured right after a mid-load
worker restart it reads ~**9.9 s**, which is the rejoin window showing up in
the lifetime figure — not the steady-state lag.

For "what is the lag right now", use the windowed Prometheus query instead,
which is what the Grafana panel and the `< 1.5s` claim refer to:

```promql
histogram_quantile(0.99, sum by (le, channel)(rate(nexus_event_e2e_lag_seconds_bucket[1m])))
```

### Peak-throughput note (bullet honesty)

- **Local sustained:** 150/s stably meets every SLO above.
- **Local peak burst:** ~940/s. Beyond that, publish p99 climbs to 158ms and e2e lag p99 to ~29s — recorded honestly, **not** claimed as "达标".
- **50K/s** is a **k6 Cloud target**, not a local measurement. Not tested on this laptop.

---

## Bullet 2 — Redis cache-aside + PostgreSQL persistence; 95% by_id hit rate under load

| Claim | Code location | How to verify | Measured on local |
|---|---|---|---|
| Cache-aside read path | `internal/notifcache/cache.go:GetByMessageID`. Handler: `cmd/producer/main.go:handleGetNotification`. Route: `GET /notifications/{message_id}`. | 1) Publish an event, capture `message_id`. 2) `curl -s http://localhost:8080/notifications/<id>` — first call is a miss, subsequent calls within 60s are hits (visible in Redis: `docker exec deploy-redis-1 redis-cli GET "cache:notif:v2:<id>"`). | ✓ |
| Hit/miss counters split by scope | Counters `nexus_cache_hits_total{scope}` / `nexus_cache_misses_total{scope}` in `internal/metrics/metrics.go`, updated in `internal/notifcache/cache.go`. scope ∈ {`by_id`, `list`}. Panel: **Grafana → Cache hit rate — scope=by_id**, 0.95 threshold line. | Prometheus query: `sum(rate(nexus_cache_hits_total{scope="by_id"}[1m])) / (sum(rate(nexus_cache_hits_total{scope="by_id"}[1m])) + sum(rate(nexus_cache_misses_total{scope="by_id"}[1m])))`. | See below |
| **95% by_id hit rate under load** | Loadgen reads with `-read-ratio 20`(mimicking "user opens a notification, refreshes ~20× within 60s window"). | `./bin/loadgen -base http://localhost:8080 -rate 150 -dur 45s -read-ratio 20.0` — prints `cache_by_id_hit_rate`. | **95.13%** ✓ |
| Postgres persistence | `internal/store/store.go`. Table `notifications (message_id, channel, event_type, status, payload, created_at)` with PK `(message_id, channel)`. `SaveNotification` upserts. `GetByMessageID` returns all channel rows for one msg_id (small — at most 3). | `docker exec deploy-postgres-1 psql -U nexus -d nexus -c "SELECT channel, count(*) FROM notifications GROUP BY channel;"` after a loadgen run — three rows per publish. | 3 rows / publish |

---

## Bullet 3 — Idempotent consumers + retry-with-backoff + DLQ + graceful shutdown → at-least-once

| Claim | Code location | How to verify | Measured on local |
|---|---|---|---|
| Idempotent consumers | `internal/idempotency/idempotency.go:CheckScoped` (Redis SETNX, key `msg:<channel>:<id>`, 24h TTL). Called from `internal/kworker/runner.go:handle` before dispatch. | Publish the same event twice with the same `message_id`(via gRPC or by manual `rpk topic produce nexus.email.high --key <msg_id>` echoing the same record). PG still has one row per channel. Also `nexus_messages_processed_total{status="duplicate"}` bumps. | ✓ |
| Retry-with-backoff | `MaxRetries=3` in `internal/kworker/runner.go`; exponential 2s / 4s / 8s, so a record sees up to 4 delivery attempts. Transient failures re-produce back to same topic with `x-retry-count++` via `internal/kworker/republisher.go:Retry`. Original `x-produced-at` is preserved (see `cloneWithRetry`) so e2e lag reflects true event age even across retries. The per-attempt idempotency claim is released before the re-produce (`runner.go:releaseClaim`) — otherwise the retry carries the same `message_id` into the same lane and is skipped as a duplicate. | Deterministic: `go test ./internal/kworker/ -run TestHandle` (fakes, sub-second) and `go test -tags=integration ./internal/integration/ -run TransientFailure` (real Redpanda + PostgreSQL). Manual: point a webhook event at a URL that returns 500 (e.g. `http://httpbin.org/status/500`) and watch `nexus_messages_processed_total{status="failed"}` climb before the record lands in DLQ. | 4 attempts then DLQ; retry delivered end-to-end in **8.5s** incl. container start ✓ |
| Dead-letter topics | `nexus.dlq.<channel>.<priority>` (see `internal/kbroker/topics.go:DLQTopic`). Records permanently failing (bad JSON, 4xx non-429) or exceeding retry budget go straight to DLQ. Gauge `nexus_dlq_messages_total{channel,priority}` samples end offsets every 3s (`internal/kbroker/lag.go`). Panel: **Grafana → DLQ backlog by lane**. | `docker exec deploy-redpanda-1 rpk topic consume nexus.dlq.webhook.normal -n 1 --offset -1` after generating a failure. | 0 at steady state |
| DLQ replay | `internal/replay/kafka.go`; endpoint `POST /dlq/replay`. Uses dedicated consumer group `nexus.replay`, resets `x-retry-count=0`, keeps `x-produced-at`. Accepts legacy AMQP-style names for backward compatibility. | `curl -X POST http://localhost:8080/dlq/replay -H 'content-type: application/json' -d '{"queue":"nexus.dlq.webhook.normal","max":100}'` → response `{"replayed": N}`. The primary topic sees the records again and the worker re-processes them. Note the DLQ gauge does **not** fall: `lag.go:sampleDLQ` samples DLQ topic end offsets, which is a cumulative count of everything ever dead-lettered, and replay only commits offsets. Confirm the replay via the primary lane and the resulting PostgreSQL rows instead. | Verified via manual replay |
| Recovery from a claim with no row | `internal/kworker/runner.go:handle` — when the Redis claim is held the runner confirms against PostgreSQL (`store.HasNotification`) before skipping. The claim is taken *before* the work, so on its own it only proves a worker started. | Deterministic: stop the worker, `POST /events`, then `redis-cli SET msg:<channel>:<msg_id> 1 EX 86400` for all three channels to mimic a worker killed between the SETNX and the row write, then start the worker. | All 3 rows written (`email/delivered`, `inapp/delivered`, `webhook/skipped`) and one `claim held but nothing persisted; reprocessing` warning per channel. The previous behaviour wrote **0 rows** and lost the message for the full 24h TTL. ✓ |
| Graceful shutdown | `cmd/worker/main.go` — SIGTERM triggers `signal.NotifyContext` cancel → each lane runner exits its poll loop → waits for in-flight goroutines → closes kgo client → shared republisher `Flush + Close`. No bulk commit on the way out: franz-go refreshes its uncommitted set on every poll, so committing it would also commit records the handler left uncommitted on purpose. | `docker compose restart worker` mid-load: `docker logs deploy-worker-1` shows the "worker starting" logs after restart, no orphaned messages, PG row counts across channels stay equal. | ✓ |
| **At-least-once under rolling restart** | Combination of manual offset commit + producer `acks=all` + idempotent producer + graceful shutdown + consumer-group rebalance (`BlockRebalanceOnPoll` + `OnPartitionsRevoked` in `runner.go` — a partition only leaves after in-flight has finished; see `awaitInflightBeforeRevoke`). Duplicate suppression is confirmed against PostgreSQL, so a worker killed between the Redis claim and the row write does not silently drop the message. | Run loadgen at 100/s for 30s, `docker compose restart worker` at t=10s. When loadgen finishes, `docker exec deploy-postgres-1 psql -U nexus -d nexus -c "SELECT channel, count(*) FROM notifications GROUP BY channel;"` — all three channels report the same non-zero count and equal loadgen's `publish_ok`. | **2999 / 2999 / 2999** with 2999 publishes, under both `docker compose restart worker` and a hard `docker kill -s KILL` ✓ |

---

## Bullet 4 — End-to-end three-stage latency tracing + Railway zero-downtime rolling updates

| Claim | Code location | How to verify | Measured on local |
|---|---|---|---|
| Stage 1 — ingest (produce path) | Histogram `nexus_stage_ingest_duration_seconds{channel,priority}` — observed inside the publisher callback in `internal/kbroker/publisher.go`. | `histogram_quantile(0.99, sum by (le)(rate(nexus_stage_ingest_duration_seconds_bucket[1m])))` or Grafana panel **Three-stage latency p99**. | 13.3 ms |
| Stage 2 — processing (consume → commit) | Histogram `nexus_stage_processing_duration_seconds{channel}` — measured in `runner.handle` from fetch to just before commit. | Same Grafana panel, `processing p99` series. | 1–20 ms |
| Stage 3 — delivery (dispatch call) | Histogram `nexus_stage_delivery_duration_seconds{channel}` — measured around `processor.Deliver(...)` only, isolating downstream latency from processing overhead. | Same Grafana panel, `delivery p99` series. Compare `email` vs `webhook` to tell "worker is slow" from "downstream is slow". | 0–1 ms (no SMTP/HTTP configured locally) |
| E2E age (all stages combined) | Histogram `nexus_event_e2e_lag_seconds{channel}` — `now − x-produced-at` at pick-up time. See Bullet 1. | See Bullet 1. | 24.7 ms |
| Railway zero-downtime rolling update | `deploy/railway.toml` / `railway.worker.toml` — restart policy + comment block explaining the flow. Kafka side: consumer-group rebalance handles partition handoff; `BlockRebalanceOnPoll` defers revocation to the next poll boundary and `OnPartitionsRevoked` then blocks it until in-flight work has finished. | **Local proxy**: `docker compose restart worker` under load, as in Bullet 3 — the same guarantee applies. On Railway: watch `nexus_event_e2e_lag_seconds` p99 stay under 1.5s across a redeploy; `rpk group describe nexus.email.high` shows partitions move between the old and new member without gaps. | Rolling-restart e2e lag p99 = **395 ms** (< 1.5s) ✓ |

---

## Quick failure drills

| Scenario | Command | Expected |
|---|---|---|
| Publish a bad JSON payload | `curl -X POST http://localhost:8080/events -H 'content-type: application/json' -d 'not-json'` | 400. `nexus_events_published_total` does not bump. |
| Simulate transient webhook failure | Send an event with `payload.webhook_url` pointing at `http://httpbin.org/status/503`. | Runner retries 3× (2s/4s/8s), then routes to DLQ. `nexus_messages_processed_total{status="dlq"}` bumps; DLQ gauge climbs. |
| Simulate permanent webhook failure | Same as above, but point at `http://httpbin.org/status/400`. | Straight to DLQ, no retries. |
| Empty DLQ replay | `curl -X POST http://localhost:8080/dlq/replay -H 'content-type: application/json' -d '{"queue":"nexus.dlq.email.high","max":100}'` after nothing has failed | `{"replayed": 0}`, no side-effects. |

---

## Grafana panels ↔ résumé bullets

Dashboard: **Nexus — Kafka pipeline** (`deploy/grafana/dashboards/nexus-kafka.json`, auto-provisioned via `dashboards.yml`).

| Panel | Backs |
|---|---|
| Consumer lag (records) by lane | Bullet 1 (partition tuning, consumer scaling) |
| End-to-end lag p99 (seconds) | Bullet 1 ("lag < 1.5s") |
| DLQ backlog by lane | Bullet 3 (DLQ) |
| Cache hit rate — scope=by_id | Bullet 2 (95%) |
| Three-stage latency p99 (ms) | Bullet 4 (three-stage tracing) |
| Publish rate / Processed rate / Delivery success rate | Bullets 1, 3 (throughput + at-least-once) |
| Total DLQ records | Bullet 3 |
