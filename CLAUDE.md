# CLAUDE.md — Nexus Project

> This is the global specification document for the Nexus project.
> All AI-assisted development and human collaboration should reference this file.
> Update this document before changing any subsystem design.

---

## 1. Project Overview

**Nexus** is a production-grade message-driven notification system demonstrating high-concurrency backend architecture, multi-channel dispatch, real-time monitoring, and modern frontend engineering.

**Portfolio positioning**: Targeting Agent/backend engineering roles at Chinese tech companies (ByteDance, Alibaba, Baidu). Key capabilities showcased:
- Message queue + priority queue design
- Redis idempotency deduplication
- Dead-letter queue + manual replay
- WebSocket real-time broadcast
- Prometheus full-pipeline instrumentation
- Full-stack deployment (Railway)

**Tech stack**

| Layer | Technology |
|---|---|
| Backend language | Go 1.22 |
| Message queue | RabbitMQ (AMQP 0.9.1) |
| Database | PostgreSQL 16 |
| Cache / idempotency | Redis |
| Real-time communication | WebSocket (Gorilla) |
| API | HTTP REST + gRPC |
| Monitoring | Prometheus (`/api/metrics/summary` endpoint pending) |
| Frontend | Next.js 15 (App Router) + TypeScript |
| UI library | shadcn/ui + Tailwind CSS |
| Deployment | Railway (producer + worker deployed; web service pending) |

---

## 2. Backend Architecture

### 2.1 Service Split

**Producer** (`cmd/producer`, port `8080` / gRPC `50051`)
- Responsibilities: event ingestion, HTTP/gRPC API, WebSocket Hub management, load test orchestration
- Does NOT consume any messages

**Worker** (`cmd/worker`, metrics port `9091`)
- Responsibilities: consume from RabbitMQ, dispatch via three channels, write to PostgreSQL
- Three worker types: EmailWorker (pool=10), InAppWorker (pool=5), WebhookWorker (pool=8)
- No outbound HTTP (Prometheus scrapes `:9091/metrics` only)

### 2.2 Message Flow

```
POST /events
    │
    ▼
Publisher → Exchange "nexus.events" (topic)
            routing key: event.{type}.{priority}
            │
    ┌───────┼───────┐
    ▼       ▼       ▼
 Email   InApp  Webhook
 queues  queues  queues
(×3 priorities: high / normal / low)
    │
    ▼
Worker processing:
  1. Redis idempotency check (key=msg:{id}, TTL=24h)
  2. Actual dispatch (SMTP / WebSocket broadcast / HTTP POST)
  3. Upsert → PostgreSQL notifications table
  4. Ack (success) or Nack→DLQ (failure)
```

### 2.3 Key Design Decisions (interview-ready)

| Design | Rationale |
|---|---|
| Three-tier priority queues | High-priority messages (payments, alerts) are never blocked by low-priority tasks |
| Redis idempotency | Prevents duplicate notifications after worker restarts |
| DLQ + manual replay | Failed messages are never lost; ops can selectively retry after intervention |
| Non-blocking WebSocket broadcast | Slow clients are dropped without affecting other subscribers' latency |
| Prometheus full-pipeline instrumentation | Publish latency, processing time, and queue depth are all observable |
| Load test demo mode | Demonstrates real throughput curves without requiring k6 cloud quota |

### 2.4 REST API Reference

| Method | Path | Description |
|---|---|---|
| POST | `/events` | Publish a new event |
| GET | `/notifications` | List notifications (hardcoded limit=50, **no filtering/pagination**) |
| POST | `/notifications/clear` | Clear all notification records |
| GET | `/ws` | WebSocket upgrade endpoint |
| GET | `/api/metrics/summary` | Aggregated metrics summary (**pending implementation**, see §2.5) |
| POST | `/ops/loadtest/start` | Start load test (real or demo mode) |
| GET | `/ops/loadtest/{run_id}` | Query load test result by run_id |
| GET | `/ops/loadtest/latest` | Query the most recent load test result |
| POST | `/dlq/replay` | Manually replay DLQ messages |

### 2.5 `/api/metrics/summary` Response Schema

> **Status: pending implementation.** The handler does not yet exist in `internal/metrics/`. Schema defined here for frontend contract alignment.

```json
{
  "publish_rate_per_sec": 142.3,
  "processing_latency_p99_ms": 38.1,
  "queue_depth": {
    "email_high": 0,
    "email_normal": 12,
    "email_low": 45,
    "inapp_high": 0,
    "inapp_normal": 8,
    "inapp_low": 20,
    "webhook_high": 2,
    "webhook_normal": 5,
    "webhook_low": 11
  },
  "delivery_success_rate": 0.986,
  "dlq_count": 3,
  "active_ws_connections": 7,
  "uptime_seconds": 84732
}
```

> The frontend `useMetrics` hook polls this endpoint every 5 seconds (not via WebSocket).

### 2.6 WebSocket Message Format

The server broadcasts raw `broker.Event` JSON (InAppWorker forwards directly, no field remapping):

```json
{
  "message_id": "uuid",
  "type": "payment.completed",
  "priority": "high",
  "payload": { ... },
  "timestamp": "2026-04-05T10:00:00Z"
}
```

> **Note**: Field names are `type` (not `event_type`) and `timestamp` (not `created_at`). The `channel` and `status` fields are absent. The frontend `WsEvent` type in `types/index.ts` must match this exactly.

---

## 3. Database

### 3.1 Schema

```sql
CREATE TABLE notifications (
  message_id  TEXT        NOT NULL,
  channel     TEXT        NOT NULL,  -- 'email' | 'inapp' | 'webhook'
  event_type  TEXT        NOT NULL,
  status      TEXT        NOT NULL,  -- 'delivered' | 'failed' | 'duplicate' | 'dlq'
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
- Displays: real-time publish rate, P99 latency, DLQ count, active WebSocket connections, queue depth chart
- Data source: `useMetrics` hook (5s poll of `/api/metrics/summary`)
- Client-side rendering only (no SSR — data freshness is the priority)

#### `/notifications` — Notification List
- Filters: channel (email/inapp/webhook), status (delivered/failed/duplicate/dlq)
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
export type Status   = 'delivered' | 'failed' | 'duplicate' | 'dlq'

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
  publish_rate_per_sec:      number
  processing_latency_p99_ms: number
  queue_depth:               Record<string, number>
  delivery_success_rate:     number
  dlq_count:                 number
  active_ws_connections:     number
  uptime_seconds:            number
}
```

---

## 5. Environment Variables

### Go backend (producer / worker)

| Variable | Example | Notes |
|---|---|---|
| `AMQP_URL` | `amqp://user:pass@...` | Injected by Railway |
| `POSTGRES_DSN` | `postgres://...` | Railway PostgreSQL |
| `REDIS_URL` | `redis://...` | Railway Redis |
| `SMTP_HOST` | `smtp.gmail.com` | Email dispatch |
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
| `nexus-web` | pending | Dockerfile or nixpacks (Node), root=`web/` |

**Important notes**:
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