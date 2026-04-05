# Nexus — Monorepo File Structure

```
nexus/
│
├── cmd/
│   ├── producer/               # HTTP + gRPC + WebSocket service entry (port 8080 / 50051)
│   │   └── main.go
│   └── worker/                 # RabbitMQ consumer entry (metrics port 9091)
│       └── main.go
│
├── internal/
│   ├── broker/                 # RabbitMQ connection, exchange/queue declarations, publisher
│   ├── store/                  # PostgreSQL CRUD (notifications table)
│   ├── hub/                    # WebSocket Hub, non-blocking broadcast
│   ├── idempotency/            # Redis idempotency dedup (24h TTL)
│   ├── grpcserver/             # gRPC server implementation
│   ├── replay/                 # DLQ replay logic
│   ├── loadtest/               # Dual-mode load test (k6 cloud / demo synthetic data)
│   ├── metrics/                # Prometheus metric registration (no HTTP handler; summary endpoint pending)
│   ├── mailer/                 # SMTP dispatch
│   ├── worker/
│   │   ├── email.go            # EmailWorker (pool=10)
│   │   ├── inapp.go            # InAppWorker (pool=5)
│   │   └── webhook.go          # WebhookWorker (pool=8)
│   └── envutil/                # Environment variable helpers
│
├── web/                        # ★ Next.js frontend (App Router) — pending creation
│   │
│   ├── app/
│   │   ├── layout.tsx          # Root layout: Sidebar + main content
│   │   ├── page.tsx            # / → Dashboard
│   │   ├── notifications/
│   │   │   └── page.tsx        # Notification list (filter / pagination)
│   │   ├── live/
│   │   │   └── page.tsx        # WebSocket live event feed
│   │   ├── loadtest/
│   │   │   └── page.tsx        # Load test console + real-time progress
│   │   ├── dlq/
│   │   │   └── page.tsx        # Dead-letter queue management + replay
│   │   ├── publish/
│   │   │   └── page.tsx        # Manual event publisher (debug tool)
│   │   └── api/
│   │       └── metrics/
│   │           └── route.ts    # Next.js API Route: proxies :9091 Prometheus endpoint
│   │
│   ├── components/
│   │   ├── ui/                 # shadcn/ui auto-generated components (do not edit manually)
│   │   ├── layout/
│   │   │   ├── Sidebar.tsx
│   │   │   └── TopBar.tsx
│   │   ├── dashboard/
│   │   │   ├── MetricCard.tsx       # Single metric card (rate, latency, DLQ, connections)
│   │   │   ├── ThroughputChart.tsx  # Area chart: email / inapp / webhook over time
│   │   │   └── QueueDepthChart.tsx  # Horizontal bar chart: 9 queues by priority
│   │   ├── notifications/
│   │   │   ├── NotificationTable.tsx
│   │   │   └── FilterBar.tsx
│   │   ├── live/
│   │   │   └── EventFeed.tsx        # Real-time WebSocket message stream
│   │   ├── loadtest/
│   │   │   ├── LoadTestControl.tsx
│   │   │   └── LoadTestProgress.tsx
│   │   └── dlq/
│   │       ├── DLQTable.tsx
│   │       └── ReplayButton.tsx
│   │
│   ├── hooks/
│   │   ├── useWebSocket.ts     # WebSocket connection manager with auto-reconnect
│   │   ├── useNotifications.ts # Notification list data + filter state
│   │   ├── useMetrics.ts       # Polls /api/metrics/summary every 5s
│   │   └── useLoadTest.ts      # Load test trigger + progress polling
│   │
│   ├── lib/
│   │   ├── api.ts              # Unified fetch wrapper (baseURL, error handling)
│   │   ├── websocket.ts        # WebSocket singleton + WsEvent type parsing
│   │   └── utils.ts            # cn() and other shared utilities
│   │
│   ├── types/
│   │   └── index.ts            # Notification, WsEvent, MetricsSummary type definitions
│   │
│   ├── public/
│   ├── components.json         # shadcn/ui config
│   ├── next.config.ts          # Rewrites: /api/backend/* → Go producer
│   ├── tailwind.config.ts
│   ├── tsconfig.json
│   └── package.json
│
├── docs/                       # ★ Pending creation
│   ├── ARCHITECTURE.md         # System architecture diagram + message flow
│   ├── API.md                  # Full REST / gRPC / WebSocket API reference
│   └── DEPLOYMENT.md           # Railway deployment steps + env variable checklist
│
├── go.mod
├── go.sum
├── nixpacks.toml               # Local build only (producer binary only; not used by Railway)
├── deploy/
│   ├── railway.toml            # Railway producer service (Dockerfile build)
│   ├── railway.worker.toml     # Railway worker service (Dockerfile build)
│   └── docker-compose.yml      # Local infrastructure (RabbitMQ + PostgreSQL + Redis)
└── CLAUDE.md                   # ★ Global project specification (for AI tools and collaborators)
```

## Railway Deployment

Two independent config files, both using Dockerfile builds:

| Service | Config file | Container start command |
|---|---|---|
| `nexus-producer` | `deploy/railway.toml` | `/app/producer` |
| `nexus-worker` | `deploy/railway.worker.toml` | `/app/worker` |
| `nexus-web` | pending | `next start` (root=`web/`) |

## next.config.ts Rewrite Rules (pending)

> Target configuration once `web/` exists. Not yet implemented.

```
/api/backend/**  →  http://producer.internal:8080/**
```

Prometheus metrics are proxied through `web/app/api/metrics/route.ts` to the internal `:9091` port — never exposed directly to the browser.