# Nexus Repository Structure

> Last updated: 2026-04-06

## Top-Level Layout

```text
nexus/
├── cmd/                  # executable entry points (producer / worker)
├── deploy/               # Docker, Prometheus, Grafana, Railway configs
├── docs/                 # project documentation
├── internal/             # backend application code
├── proto/                # gRPC protocol definitions
├── scripts/              # operational / manual test scripts
├── web/                  # Next.js frontend
├── .env.example          # environment variable template
├── README.md             # project overview
└── structure.md          # this file
```

## `cmd/`

```text
cmd/
├── producer/
│   └── main.go           # HTTP + gRPC + WebSocket + load-test API service
└── worker/
    └── main.go           # queue consumers + metrics endpoint (:9091 by default)
```

## `internal/`

```text
internal/
├── broker/               # RabbitMQ connection, routing, publisher
├── envutil/              # .env discovery and loading utilities
├── grpcserver/           # gRPC server implementation
├── hub/                  # WebSocket hub
├── idempotency/          # Redis deduplication
├── integration/          # end-to-end integration tests (build tag: integration)
├── loadtest/             # load-test client and orchestration (real/demo modes)
├── mailer/               # SMTP abstraction
├── metrics/              # Prometheus metrics and summary computation
├── replay/               # dead-letter replay logic
├── store/                # PostgreSQL data access layer
└── worker/               # email / inapp / webhook workers
```

## `web/`

```text
web/
├── app/                  # App Router pages
├── components/           # feature components + shadcn UI primitives
├── hooks/                # client-side data and connection hooks
├── lib/                  # frontend API client and shared utilities
├── public/               # static assets
├── types/                # TypeScript domain types
├── package.json
└── README.md
```

## `deploy/`

```text
deploy/
├── docker-compose.yml    # full local stack, including observability
├── Dockerfile.producer
├── Dockerfile.worker
├── Dockerfile.web
├── prometheus.yml
├── grafana/
│   ├── dashboard.json
│   └── datasource.yml
├── railway.toml          # producer service profile
├── railway.worker.toml   # worker service profile
└── railway.web.toml      # web service profile
```

## Notes

- `CLAUDE.md` remains as the project specification and collaboration reference.
- Both root `railway.toml` and `deploy/railway.toml` exist; the `deploy/` profiles are the preferred source of truth.
