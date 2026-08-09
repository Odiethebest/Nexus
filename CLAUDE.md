# CLAUDE.md — Nexus Project

> This is the global specification document for the Nexus project.
> All AI-assisted development and human collaboration should reference this file.
> Update this document before changing any subsystem design.

---

## 1. Project Overview

**Nexus** is a production-grade message-driven notification system demonstrating high-concurrency backend architecture, multi-channel dispatch, real-time monitoring, and modern frontend engineering.

**Project context**: Nexus is a personal Go-based rebuild that grew out of themes I worked on during my internship at QAX Technology Group (奇安信科技集团); it is used both for continued learning and as a portfolio piece. No proprietary code, data, or business logic from the internship is reproduced here — the implementation and design trade-offs are entirely my own. Key capabilities exercised:
- Message queue + priority queue design
- Redis idempotency deduplication
- Dead-letter queue + manual replay
- WebSocket real-time broadcast
- Prometheus full-pipeline instrumentation
- Full-stack deployment (Railway)

**Tech stack**

| Layer | Technology |
|---|---|
| Backend language | Go 1.25 |
| Message bus | Redpanda 24.2 (Kafka protocol; franz-go client, KRaft, no ZooKeeper) |
| Database | PostgreSQL 16 |
| Cache + idempotency | Redis 7 (SETNX scoped by channel, cache-aside for reads) |
| Real-time communication | WebSocket (Gorilla) |
| API | HTTP REST + gRPC |
| Monitoring | Prometheus (three-stage histograms + consumer lag) + provisioned Grafana dashboard |
| Frontend | Next.js 15 (App Router) + TypeScript |
| UI library | shadcn/ui + Tailwind CSS |
| Deployment | Railway (producer + worker + web) + Redpanda Cloud dev cluster |

---

## 2. Backend Architecture

### 2.1 Service Split

**Producer** (`cmd/producer`, port `8080` / gRPC `50051`)
- Responsibilities: event ingestion, HTTP/gRPC API, WebSocket Hub management, load test orchestration
- Does NOT consume any messages

**Worker** (`cmd/worker`, metrics port `9091`)
- Responsibilities: consume from 9 Kafka lane topics (3 channels × 3 priorities), dispatch, write to PostgreSQL
- One `kworker.Runner` per (channel, priority) — each with its own consumer group `nexus.<channel>.<priority>` and its own pool size `[pool, pool/2, pool/4]` for high/normal/low
- Pool defaults: email=10, inapp=5, webhook=8 (configurable via `*_WORKER_POOL` envs)
- No outbound HTTP (Prometheus scrapes `:9091/metrics` only)

### 2.2 Message Flow

```
POST /events
    │
    ▼
kbroker.Publisher (franz-go, acks=all, idempotent, sticky-key partitioner)
    │ Fan-out one record per channel with headers:
    │   x-msg-id, x-event-type, x-priority, x-produced-at (ns), x-retry-count
    ▼
Redpanda topics:
    nexus.email.<high|normal|low>
    nexus.inapp.<high|normal|low>
    nexus.webhook.<high|normal|low>
    │
    ▼
kworker.Runner (one per lane, independent consumer group):
    1. Redis idempotency SETNX (key = msg:<channel>:<msg_id>, TTL=24h)
    2. Processor.Deliver (SMTP / WS broadcast / HTTP POST) → Outcome
    3. Persist to PostgreSQL (message_id, channel) upsert
    4. CommitRecords (only on success → at-least-once)
    │
    ▼ (on failure)
Transient  → re-produce back to same topic w/ x-retry-count++, MaxRetries=3
Permanent  → produce to nexus.dlq.<channel>.<priority>
```

### 2.3 Key Design Decisions (interview-ready)

| Design | Rationale |
|---|---|
| Per-lane topics + per-lane consumer groups | High-priority lane's committed offset never depends on a slow low-priority lane |
| franz-go async Produce + acks=all + idempotent producer | Supports 50K/s ceiling without the single-inflight-confirm bottleneck of the AMQP publisher; broker-side dedup on retries |
| `x-retry-count` in record header (not `x-death`) | Kafka has no built-in redelivery count; header is portable and survives DLQ round-trips |
| `x-produced-at` preserved across retries + DLQ | e2e lag histogram measures true event age, not "time since last retry" |
| Scoped Redis idempotency `msg:<channel>:<id>` | Fan-out event persists correctly in all three channels (bug fixed in Step 10 of MIGRATION.md) |
| DLQ = separate topics + dedicated `nexus.replay` consumer group | Replay is a normal Kafka consume operation; no admin ops needed |
| Manual `CommitRecords` after PG upsert + `BlockRebalanceOnPoll` | Rolling deploys / SIGTERM never lose an in-flight record (at-least-once) |
| Cache-aside `GET /notifications/{id}` (TTL 60s, scope=`by_id`) | Repeat lookups of the same recently-fanned-out msg are the hot pattern → 95% hit rate under load |
| Three-stage tracing: `nexus_stage_{ingest,processing,delivery}_duration_seconds` + `nexus_event_e2e_lag_seconds` | Distinguishes producer-side / worker-side / downstream-side slowness in one dashboard |
| Load test demo mode | Deterministic UI demo without k6 Cloud quota |

### 2.4 REST API Reference

| Method | Path | Description |
|---|---|---|
| POST | `/events` | Publish a new event (fan-out to all 3 channel topics) |
| GET | `/notifications` | List notifications (limit=50, short-TTL cache scope=`list`) |
| GET | `/notifications/{message_id}` | Cache-aside single-msg lookup (Redis, TTL 60s, scope=`by_id`) |
| POST | `/notifications/clear` | Clear notifications older than cutoff |
| GET | `/ws` | WebSocket upgrade endpoint |
| GET | `/api/metrics/summary` | Aggregated metrics summary (see §2.5) |
| POST | `/ops/loadtest/start` | Start load test (real or demo mode) |
| GET | `/ops/loadtest/{run_id}` | Query load test result by run_id |
| GET | `/ops/loadtest/latest` | Query the most recent load test result |
| POST | `/dlq/replay` | Manually replay DLQ messages (accepts legacy AMQP-style name) |

### 2.5 `/api/metrics/summary` Response Schema

Handler: `internal/metrics/summary.go:ComputeSummary`. Sourced from the local Prometheus registry (producer-owned counters) merged with a scrape of the worker (`METRICS_INTERNAL_URL`, default `http://localhost:9091/metrics`).

```json
{
  "publish_rate_per_sec": 250.0,        // EVENTS/s — see units note below
  "processed_rate_per_sec": 750.0,      // RECORDS/s — rate(nexus_messages_processed_total), all channels
  "processed_rate_per_sec_by_channel": {  // RECORDS/s per channel; drives the dashboard chart
    "email": 250.0, "inapp": 250.0, "webhook": 250.0
  },
  "processing_latency_p99_ms": 38.1,    // p99 of nexus_stage_processing_duration_seconds
  "e2e_lag_p99_seconds": 0.025,         // p99 of nexus_event_e2e_lag_seconds (résumé: "lag < 1.5s")
  "queue_depth": {                      // from nexus_consumer_lag_records gauge
    "email_high": 0, "email_normal": 12, "email_low": 45,
    "inapp_high": 0, "inapp_normal": 8,  "inapp_low": 20,
    "webhook_high": 2, "webhook_normal": 5, "webhook_low": 11
  },
  "delivery_success_rate": 0.986,
  "dlq_count": 3,                       // sum of nexus_dlq_messages_total gauge (cumulative; replay does not lower it)
  "active_ws_connections": 7,
  "uptime_seconds": 84732
}
```

**Units — events vs records.** One `POST /events` is one *event*, which the publisher fans out into one *record per channel*. `publish_rate_per_sec` counts events; every `processed_*` figure counts records. So at steady state each per-channel processed rate tracks `publish_rate_per_sec` 1:1, and `processed_rate_per_sec` sits at ~3×. Comparing `publish_rate_per_sec` against `processed_rate_per_sec` directly is a unit error — to detect backpressure, compare the publish rate against the *slowest* entry in `processed_rate_per_sec_by_channel`.

`publish_rate_per_sec` is derived as the **max** over `nexus_events_published_total{channel}`, not `sum ÷ 3`: dividing would bake the fan-out width into the metrics layer, and the three counters are bumped from independent async ack callbacks so they can differ by the number of in-flight publishes (`metrics.eventsFromPerChannel`).

Per-channel throughput uses *processed*, not *published*, because fan-out makes the three published counters identical by construction — a per-channel publish chart would be three overlapping lines.

> The frontend `useMetrics` hook polls this endpoint every 5 seconds (not via WebSocket) and keeps a 15-minute rolling history, each sample stamped with its client-side arrival time.

### 2.6 WebSocket Message Format

Delivery events originate in the worker, which has no HTTP server, so they travel to the producer over Redis pub/sub (`internal/wsfeed`, channel `nexus:ws:events`) and every producer replica fans them out to its own `/ws` clients. The producer forwards the payload verbatim.

The worker emits **one envelope per (message_id, channel) verdict**, so a single published event produces three — mirroring the notifications table:

```json
{
  "message_id": "uuid",
  "type": "payment.completed",
  "priority": "high",
  "channel": "webhook",
  "status": "dlq",
  "payload": { ... },
  "timestamp": "2026-04-05T10:00:00Z"
}
```

> **Note**: Field names are `type` (not `event_type`) and `timestamp` (not `created_at`). `channel` and `status` are always present and real — the client used to hardcode `channel: "inapp"`, which made the `/live` channel filter inert. The frontend `WsEvent` type in `types/index.ts` must match this exactly.

The feed is best-effort by design: `wsfeed.Publisher` buffers and drops frames rather than blocking delivery, and bounds its shutdown drain so a slow Redis cannot hold up worker exit.

---

## 3. Database

### 3.1 Schema

```sql
CREATE TABLE notifications (
  message_id  TEXT        NOT NULL,
  channel     TEXT        NOT NULL,  -- 'email' | 'inapp' | 'webhook'
  event_type  TEXT        NOT NULL,
  status      TEXT        NOT NULL,  -- 'delivered' | 'skipped' | 'failed' | 'dlq'
  payload     JSONB,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (message_id, channel)
);

-- Only one index exists currently
CREATE INDEX notifications_created_at_idx ON notifications(created_at DESC);
```

> **TODO**: `priority` column not yet in schema. Required before frontend can filter by priority — needs a migration. `status` and `channel` indexes also pending.

### 3.2 Query Rules

- List queries are hardcoded at `limit=50` with no filter parameters (frontend consumes full result)
- Time-range queries use the `created_at DESC` index
- No soft deletes — historical data is retained for portfolio demo purposes

---

## 4. Frontend Architecture

### 4.1 Tech Choices

| Item | Choice | Reason |
|---|---|---|
| Framework | Next.js 15 (App Router) | SSR capability + modern React patterns |
| Language | TypeScript (strict mode) | Type safety, demonstrates engineering standards |
| Styling | Tailwind CSS | Native shadcn/ui integration |
| Component library | shadcn/ui | High quality, customizable, not a black box |
| State management | React hooks (no Redux) | Project scale does not warrant a global store |
| Data fetching | Native fetch + SWR | Lightweight, well-suited for polling |
| Charts | Recharts | Consistent with React ecosystem |

### 4.2 Page Specs

#### `/` — Dashboard
- Displays four metric cards (publish rate in events/s with a per-lane backpressure badge; E2E lag p99 with the 1.5s SLO badge; cumulative dead-lettered count; active WebSocket connections) above a stacked "Delivery Throughput by Channel" area chart fed by `processed_rate_per_sec_by_channel`
- Data source: `useMetrics` hook (5s poll of `/api/metrics/summary`)
- Client-side rendering only (no SSR — data freshness is the priority)

#### `/notifications` — Notification List
- Filters: channel (email/inapp/webhook), status (delivered/skipped/failed/dlq)
- Pagination: 50 rows per page
- Data source: `useNotifications` hook → `GET /notifications`

#### `/live` — Live Event Feed
- WebSocket connection to `GET /ws`
- New messages enter from the top; retain latest 100
- Client-side filtering by channel / priority
- Data source: `useWebSocket` hook

#### `/loadtest` — Load Test Console
- Start / stop buttons (demo mode, ~55s)
- Real-time display: throughput line chart, success rate, queue backlog trend
- Disable start button while test is running
- Data source: `useLoadTest` hook (polls `GET /ops/loadtest/latest` or `GET /ops/loadtest/{run_id}`)

#### `/dlq` — Dead-Letter Queue
- Displays DLQ counts per queue (sourced from metrics summary)
- Replay button calls `POST /dlq/replay`
- Refreshes notifications list after replay
- Shows replay result via Toast

#### `/publish` — Manual Event Publisher (debug tool)
- Form: event_type (dropdown), priority (radio), payload (JSON editor)
- Submits to `POST /events`
- Returns `message_id`; user can navigate to `/live` to observe the event

### 4.3 Component Rules

- **shadcn/ui components**: install only via `npx shadcn add`. Never manually edit files under `components/ui/`
- **Business components**: live in `components/{page}/`, one responsibility per file
- **Props types**: all components must have an explicit props interface — no `any`
- **Naming**: components PascalCase, hooks `useXxx`, utilities camelCase

### 4.4 Data Fetching Rules

All backend requests go through `lib/api.ts`:

```typescript
const BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080'
```

Next.js API Routes are used only for:
1. Proxying Prometheus `:9091` metrics (avoids CORS + handles aggregation)
2. Future server-side auth scenarios

### 4.5 Type Definitions (`types/index.ts`)

```typescript
export type Channel  = 'email' | 'inapp' | 'webhook'
export type Priority = 'high' | 'normal' | 'low'
// Statuses the worker actually writes. There is no 'duplicate' row status —
// a duplicate is committed without persisting, so it exists only as the
// nexus_messages_processed_total{status="duplicate"} counter label.
export type Status   = 'delivered' | 'skipped' | 'failed' | 'dlq'

export interface Notification {
  message_id: string
  channel:    Channel
  event_type: string
  // priority field not yet in DB schema — re-enable after migration
  status:     Status
  payload:    Record<string, unknown>
  created_at: string
}

export interface WsEvent {
  message_id: string
  type:       string      // NOT event_type
  priority:   Priority
  payload:    Record<string, unknown>
  timestamp:  string      // NOT created_at
}

export interface MetricsSummary {
  publish_rate_per_sec:              number   // events/s
  processed_rate_per_sec:            number   // records/s, all channels
  processed_rate_per_sec_by_channel: Record<Channel, number>  // records/s per channel
  processing_latency_p99_ms:         number
  e2e_lag_p99_seconds:               number
  queue_depth:                       Record<string, number>
  delivery_success_rate:             number
  dlq_count:                         number
  active_ws_connections:             number
  uptime_seconds:                    number
}

// A summary reading plus the wall-clock time the browser received it.
// The chart selects its window by elapsed time, not by sample count — a
// throttled background tab polls irregularly.
export interface MetricsSample extends MetricsSummary {
  received_at: number
}
```

---

## 5. Environment Variables

### Go backend (producer / worker)

| Variable | Example | Notes |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:19092` / `<seed>.redpanda.cloud:9092` | Required. Comma-separated. |
| `KAFKA_CLIENT_ID` | `nexus` | Optional, default `nexus` |
| `KAFKA_TOPIC_PARTITIONS` | `12` | See README partition-sizing derivation |
| `KAFKA_REPLICATION_FACTOR` | `1` (local) / `3` (Redpanda Cloud) | |
| `KAFKA_TLS` | `true` | Enable TLS for managed brokers |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-256` | |
| `KAFKA_SASL_USER` / `KAFKA_SASL_PASS` | `...` | Managed broker credentials |
| `POSTGRES_DSN` | `postgres://...` | Railway PostgreSQL |
| `REDIS_URL` | `redis://...` | Railway Redis — used by both idempotency and cache-aside |
| `CORS_ALLOWED_ORIGINS` | `https://nexus.example.com,http://localhost:3000` | Browser origin allow-list. Gates HTTP routes **and** the `/ws` upgrade. Empty → trust all (demo default). Requests with no `Origin` (curl, loadgen, Prometheus) always pass. Legacy `LOADTEST_ALLOWED_ORIGINS` is a deprecated fallback. |
| `SMTP_HOST` | `smtp.gmail.com` | Optional; empty → email delivery is a no-op ("delivered" without send) |
| `SMTP_PORT` | `587` | |
| `SMTP_USER` | `...` | |
| `SMTP_PASS` | `...` | |
| `K6_API_TOKEN` | `...` | Real load test mode (optional — demo mode works without it) |

### Next.js frontend

| Variable | Example | Notes |
|---|---|---|
| `NEXT_PUBLIC_API_URL` | `https://producer.railway.app` | Go producer base URL |
| `NEXT_PUBLIC_WS_URL` | `wss://producer.railway.app/ws` | WebSocket URL |
| `METRICS_INTERNAL_URL` | `http://worker.internal:9091` | Server-side only (API Route proxy) |

---

## 6. Dev Commands

### Backend
```bash
go run ./cmd/producer
go run ./cmd/worker

go build -o bin/producer ./cmd/producer
go build -o bin/worker ./cmd/worker
```

### Frontend
```bash
cd web
npm install
npm run dev
npm run build

npx shadcn add button card table badge
```

### Local infrastructure
```bash
# docker-compose.yml lives in deploy/
docker compose -f deploy/docker-compose.yml up -d
```

---

## 7. Deployment (Railway)

| Service | Config file | Build method |
|---|---|---|
| `nexus-producer` | `deploy/railway.toml` | Dockerfile → `/app/producer` |
| `nexus-worker` | `deploy/railway.worker.toml` | Dockerfile → `/app/worker` |
| `nexus-web` | `deploy/railway.web.toml` | Dockerfile.web |
| Kafka | External **Redpanda Cloud** dev cluster | KAFKA_BROKERS / SASL_SSL |

**Important notes**:
- Railway has no built-in Kafka; the app is expected to point at Redpanda Cloud (dev cluster free tier) via `KAFKA_BROKERS` + SASL_SSL. See `README.md` deployment section.
- Rolling deploys are zero-downtime: Railway waits for `/health` on the new instance before killing the old. On the Kafka side, consumer-group rebalance moves partitions between the old and new worker. See `RUNBOOK.md` for the exact validation procedure.
- Root `nixpacks.toml` builds the producer binary only — it does **not** participate in Railway deployments (Railway uses Dockerfile)
- `embed.FS` SPA fallback is **not implemented** — producer does not serve static files
- `NEXT_PUBLIC_*` variables are injected at Railway **build time**, not runtime — set them before deploying

---

## 8. AI Development Guidelines

When using Claude Code or other AI tools:

- **Modifying backend API**: update §2.4 in this document
- **Adding a frontend page**: add a spec entry in §4.2
- **Adding an env variable**: update §5
- **Never edit** files under `components/ui/` (shadcn auto-generated)
- **Types first**: define new data structures in `types/index.ts` before writing implementation
- **No inline fetch**: all requests must go through `lib/api.ts` or a dedicated hook