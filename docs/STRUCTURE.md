# Nexus Repository Structure

## Top-Level Layout

```text
nexus/
├── cmd/                  # executable entry points
├── deploy/               # Docker, Prometheus, Grafana, Railway configs
├── docs/                 # this directory
├── internal/             # backend application code
├── proto/                # gRPC protocol definition
├── scripts/              # operational / manual test scripts
├── web/                  # Next.js frontend
├── .env.example          # environment variable template
├── CLAUDE.md             # project specification / collaboration reference
├── MIGRATION.md          # RabbitMQ -> Redpanda transition record
├── README.md             # project overview
└── RUNBOOK.md            # claim -> code -> metric -> reproduction mapping
```

## `cmd/`

```text
cmd/
├── producer/             # HTTP + gRPC + WebSocket + loadtest API service
├── worker/               # nine lane consumers + metrics endpoint (:9091)
└── loadgen/              # in-repo Go load generator (write + read streams)
```

## `internal/`

```text
internal/
├── envutil/              # .env discovery and loading
├── grpcserver/           # gRPC EventService (hand-written JSON codec)
├── hub/                  # WebSocket hub + origin-checked upgrader
├── idempotency/          # Redis SETNX claims, scoped per channel
├── integration/          # end-to-end tests (build tag: integration)
├── kbroker/              # Redpanda/Kafka: config, topics, publisher,
│                         #   admin (EnsureTopics), consumer-lag sampler
├── kworker/              # lane runner, channel processors, republisher
├── loadtest/             # k6 Cloud orchestration (real) + synthetic (demo)
├── mailer/               # SMTP abstraction
├── metrics/              # Prometheus metric definitions + summary endpoint
├── notifcache/           # cache-aside read path (by_id + list scopes)
├── replay/               # DLQ -> primary topic replay
└── store/                # PostgreSQL data access layer
```

There is no `internal/broker` or `internal/worker`; those were the RabbitMQ
implementations and were removed. `kbroker` and `kworker` replace them.

## `web/`

```text
web/
├── app/                  # App Router pages: dashboard, live, notifications,
│                         #   loadtest, dlq, publish
├── components/
│   ├── ui/               # shadcn/ui primitives — never edit by hand
│   ├── live/             # EventCard
│   ├── notifications/    # FilterBar
│   └── *.tsx             # sidebar, header, metric cards, chart, data table
├── hooks/                # useMetrics, useNotifications, useWebSocket,
│                         #   useLoadTest, use-mobile
├── lib/                  # api.ts (all backend calls) + utils
├── public/               # static assets
├── types/                # shared TypeScript domain types
└── package.json
```

## `deploy/`

```text
deploy/
├── docker-compose.yml    # full local stack, including observability
├── Dockerfile.producer
├── Dockerfile.worker
├── Dockerfile.web
├── prometheus.yml        # scrapes producer, worker, and Redpanda
├── grafana/
│   ├── datasource.yml
│   ├── dashboards.yml    # provider config
│   └── dashboards/
│       └── nexus-kafka.json   # the dashboard, also the default home page
├── railway.toml          # producer service profile
├── railway.worker.toml   # worker service profile
└── railway.web.toml      # web service profile
```

## Notes

- `CLAUDE.md` is the project specification and collaboration reference; keep
  it in sync when changing an API, page, or environment variable.
- Both a root `railway.toml` and `deploy/railway.toml` exist; the `deploy/`
  profiles are the preferred source of truth.
- Root `nixpacks.toml` builds the producer only and is not used by Railway.
