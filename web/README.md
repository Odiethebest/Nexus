# Nexus Frontend

A real-time monitoring and operations dashboard for the Nexus message-driven notification system. Built with Next.js 16 (App Router), shadcn/ui, and Recharts.

---

## Overview

The frontend provides full operational visibility into the Nexus backend pipeline — a Go-based system that routes events through Redpanda (Kafka protocol) across three worker channels (email, inapp, webhook) with three priority tiers (high, normal, low). The dashboard surfaces live metrics, event streams, load test results, and dead-letter queue management in a single interface.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Framework | Next.js 15 (App Router) |
| Language | TypeScript (strict mode) |
| UI Components | shadcn/ui (Radix + Nova preset) |
| Styling | Tailwind CSS |
| Charts | Recharts |
| Real-time | WebSocket (native browser API) |
| Relative time | date-fns |
| Notifications | Sonner |

---

## Architecture

### Directory Structure

```
web/
├── app/
│   ├── layout.tsx              # Root layout: SidebarProvider + TooltipProvider + Toaster
│   ├── page.tsx                # Redirects / → /dashboard
│   ├── dashboard/page.tsx      # System overview
│   ├── live/page.tsx           # Real-time WebSocket event feed
│   ├── notifications/page.tsx  # Full notification history with filters
│   ├── loadtest/page.tsx       # Load test console
│   ├── dlq/page.tsx            # Dead-letter queue management
│   └── publish/page.tsx        # Manual event publisher
├── components/
│   ├── ui/                     # shadcn/ui primitives (do not edit manually)
│   ├── app-sidebar.tsx         # Navigation sidebar
│   ├── site-header.tsx         # Page header with title
│   ├── section-cards.tsx       # Metric card grid (Publish Rate, P99, DLQ, WS)
│   ├── chart-area-interactive.tsx  # Throughput area chart (email/inapp/webhook)
│   ├── data-table.tsx          # Notification table with skeleton + empty states
│   └── live/EventCard.tsx      # Individual event card for the live feed
├── hooks/
│   ├── useMetrics.ts           # Polls /api/metrics/summary every 5s
│   ├── useNotifications.ts     # Polls GET /notifications every 10s
│   ├── useWebSocket.ts         # WebSocket connection + historical backfill
│   └── useLoadTest.ts          # Load test trigger + result polling
├── lib/
│   └── api.ts                  # Unified fetch wrapper (baseURL, error handling)
└── types/
    └── index.ts                # Shared TypeScript interfaces
```

### Data Flow

```
Backend (Go :8080)
    │
    ├── GET /api/metrics/summary ──► useMetrics (5s poll) ──► SectionCards + ThroughputChart
    ├── GET /notifications       ──► useNotifications (10s poll) ──► DataTable
    ├── GET /ws (WebSocket)      ──► useWebSocket ──► Live Feed EventCards
    └── GET /ops/loadtest/latest ──► useLoadTest (2s poll during run) ──► LoadTest chart
```

---

## Pages

### Dashboard (`/dashboard`)

The primary system overview. Displays four KPI cards updated every 5 seconds:

- **Publish Rate** — messages processed per second, derived from `nexus_messages_processed_total` counter delta across the worker Prometheus endpoint (`:9091`)
- **P99 Latency** — 99th percentile processing time in milliseconds, interpolated from `nexus_worker_process_duration_seconds` histogram buckets
- **DLQ Backlog** — total dead-letter queue depth; card border turns red when count > 0
- **WS Connections** — active WebSocket subscribers

Below the KPI row, an interactive area chart shows per-channel throughput (email, inapp, webhook) over a rolling time window (1 min / 5 min / 15 min). A notification table below the chart shows the 50 most recent events with channel and status badges.

### Live Feed (`/live`)

Real-time event stream over WebSocket. On mount, the hook pre-populates the feed with the 50 most recent notifications fetched from `GET /notifications` (mapped from the `Notification` type to `WsEvent` shape, with priority and payload unwrapped from the stored broker envelope). Subsequent events arrive via WebSocket and are prepended with a `fade-in` animation. The feed retains the latest 100 events.

Client-side filters allow narrowing by channel (email / inapp / webhook) and priority (high / normal / low). Since only the InApp worker broadcasts to WebSocket, live-arriving events are always `channel: inapp`; channel diversity in the backfilled history reflects the full pipeline.

### Notifications (`/notifications`)

Full notification history with three independent client-side filters: channel, status (delivered / skipped / failed / dlq), and a substring search on event type. A record count updates as filters change. A "Clear All" action (with `AlertDialog` confirmation) calls `POST /notifications/clear` and refreshes the list.

### Load Test (`/loadtest`)

Triggers the backend's demo load test mode via `POST /ops/loadtest/start`. The demo generates ~55 seconds of synthetic traffic with a realistic RPS ramp-up curve. While the test runs, the page polls `GET /ops/loadtest/latest` every 2 seconds and plots RPS and P95 latency as a dual-axis Recharts `LineChart`. An elapsed-time counter and progress bar indicate test state. Results are displayed as raw JSON after completion.

Note: demo mode generates synthetic metrics server-side and does not route real messages through the pipeline, so `publish_rate_per_sec` from the metrics summary endpoint stays at whatever the real system is doing (0 if idle) during a demo run. The load test chart reads directly from the backend's synthetic series data; the metric cards above it show the *real* pipeline, not the demo.

### Publish (`/publish`)

A debug tool for injecting test events into the pipeline. Exposes a form with:
- Event type selector (8 options: `payment.completed`, `payment.failed`, `order.shipped`, `order.cancelled`, `user.signup`, `user.deleted`, `alert.critical`, `alert.warning`)
- Priority toggle (high / normal / low)
- JSON payload editor pre-filled with a realistic template per event type, auto-updating on type change

On submit, the form validates JSON client-side, calls `POST /events` with `{ type, priority, payload }`, and displays a success toast with the returned `message_id` and a link to navigate to the Live Feed.

### DLQ (`/dlq`)

Dead-letter queue management. Displays the total DLQ count from the metrics summary and a per-queue depth breakdown table (9 queues: 3 channels × 3 priorities). A "Replay All" action (with `AlertDialog` confirmation) calls `POST /dlq/replay` and shows result via toast.

---

## Key Implementation Details

### WebSocket Connection Management

`useWebSocket` establishes a single WebSocket connection on mount and implements automatic reconnection with a 3-second backoff on close. It runs in parallel with an initial REST fetch to pre-populate the event list, so the Live Feed is never empty on arrival.

```typescript
// Backfill: REST notifications → WsEvent shape
priority: (n.payload as any)?.priority ?? 'normal'  // unwrap from broker envelope
channel:  n.channel                                  // use actual channel from DB
payload:  (n.payload as any)?.payload ?? n.payload   // unwrap inner payload
```

### Metrics Endpoint Architecture

`GET /api/metrics/summary` is served by the producer process (`:8080`) but the underlying data lives in the worker's Prometheus registry (`:9091`). The handler fetches worker metrics over HTTP, parses the Prometheus text format using `expfmt.TextParser`, and computes derived values:

- **publish_rate_per_sec**: **events**/s — counter delta of `nexus_events_published_total`, collapsed across the fan-out by taking the max over channels
- **processed_rate_per_sec** / **processed_rate_per_sec_by_channel**: **records**/s from `nexus_messages_processed_total`, in total and per channel
- **processing_latency_p99_ms**: histogram P99 interpolated from `nexus_stage_processing_duration_seconds` bucket boundaries
- **e2e_lag_p99_seconds**: histogram P99 of `nexus_event_e2e_lag_seconds` (`now − x-produced-at` at pick-up)
- **delivery_success_rate**: `delivered / all processed`

> **Units.** One `POST /events` is one event, fanned out to one record per channel. Publish counts events; processed counts records, so the aggregate sits at ~3× publish while each per-channel rate tracks publish 1:1. Compare publish against the *slowest channel* to spot backpressure — never against `processed_rate_per_sec`.

The fetcher has a 2-second timeout and returns all-zero values on worker unreachability.

### Chart Data

`ChartAreaInteractive` plots `processed_rate_per_sec_by_channel` — real per-channel counter deltas. It deliberately does not chart *published* rate per channel: fan-out makes those three series identical by construction, so they would render as three overlapping lines.

`useMetrics` stamps each sample with its client-side arrival time and keeps a 15-minute rolling window. The chart selects its range by elapsed time rather than sample count, because a backgrounded tab throttles `setInterval` and "the last 12 samples" is then not "the last minute".

### CORS

The producer wraps its entire mux in an origin allow-list (`CORS_ALLOWED_ORIGINS`, comma-separated). An empty value trusts every origin — the zero-config demo default — but it must be set in production. The same allow-list is handed to the WebSocket upgrader's `CheckOrigin`, because browsers do not apply CORS to WebSocket handshakes and `/ws` would otherwise stay open to any page. Requests with no `Origin` header, and same-origin requests, always pass.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `NEXT_PUBLIC_API_URL` | `http://localhost:8080` | Go producer base URL |
| `NEXT_PUBLIC_WS_URL` | `ws://localhost:8080/ws` | WebSocket endpoint |
| `METRICS_INTERNAL_URL` | `http://localhost:9091` | Worker Prometheus endpoint (server-side only) |

---

## Development

```bash
cd web

# Install dependencies
npm install

# Start dev server (requires Go backend on :8080)
npm run dev

# Type check + build
npm run build

# Add a shadcn component
npx shadcn add <component-name>
```

Local infrastructure (Redpanda, PostgreSQL, Redis):

```bash
docker compose -f deploy/docker-compose.yml up -d redpanda redis postgres
```

> The full compose stack also starts Grafana on `:3000`, which collides with
> `npm run dev`. Start only the infrastructure services, or remap one port.

---

## Known Limitations

- `publish_rate_per_sec` is 0 during load test demo mode (synthetic data does not route through the real pipeline)
- `priority` is not stored in the PostgreSQL `notifications` table — backfilled Live Feed events derive priority from the nested broker envelope in the `payload` JSONB column; a schema migration adding a `priority` column would allow direct querying and filtering
- Channel filters on the Live Feed show no results for email/webhook in real time, since only `InAppWorker` broadcasts to WebSocket — this is by design
- `processing_latency_p99_ms` reflects cumulative histogram data since worker startup, not a rolling window