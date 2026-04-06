# Nexus: Event-Driven Notification Platform

> A production-oriented messaging system for multi-channel notifications, combining a Go backend with a real-time Next.js operations dashboard.

[![Go](https://img.shields.io/badge/Go-1.22-00ADD8?logo=go)](https://go.dev/)
[![Next.js](https://img.shields.io/badge/Next.js-16-000000?logo=next.js)](https://nextjs.org/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13-FF6600?logo=rabbitmq)](https://www.rabbitmq.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis)](https://redis.io/)
[![License](https://img.shields.io/badge/License-MIT-green)](./LICENSE)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Core Capabilities](#core-capabilities)
- [API Surface](#api-surface)
- [Getting Started](#getting-started)
- [Deployment](#deployment)
- [Observability](#observability)
- [Documentation](#documentation)
- [License](#license)

---

## Overview

Nexus routes events through RabbitMQ and processes them via dedicated workers for `email`, `inapp`, and `webhook` delivery channels. It enforces idempotency with Redis, persists delivery history in PostgreSQL, and exposes operational telemetry through Prometheus metrics and a Next.js dashboard.

The system is split into two backend services:

- **Producer** (`cmd/producer`): HTTP/gRPC ingestion, WebSocket endpoint, load-test control APIs
- **Worker** (`cmd/worker`): asynchronous consumers, channel delivery, persistence, metrics endpoint

### What It Handles

| Concern | Mechanism |
|---|---|
| Event ingestion | HTTP (`POST /events`) and gRPC (`Publish`) |
| Priority routing | RabbitMQ topic routing key `event.{type}.{priority}` |
| Idempotency | Redis `SETNX` with TTL |
| Delivery fan-out | Email, in-app broadcast, webhook workers |
| Failure isolation | Per-lane dead-letter queues + replay API |
| Delivery history | PostgreSQL `notifications` table |
| Runtime visibility | Prometheus + dashboard polling + WebSocket feed |

---

## Architecture

```text
┌──────────────┐            HTTP/gRPC             ┌───────────────────────────┐
│   Clients    │ ───────────────────────────────▶ │  Producer (cmd/producer)  │
└──────────────┘                                  │  - /events                │
                                                  │  - /ws                    │
                                                  │  - /ops/loadtest/*        │
                                                  └─────────────┬─────────────┘
                                                                │ publish
                                                                ▼
                                                  ┌───────────────────────────┐
                                                  │ RabbitMQ: nexus.events    │
                                                  │ topic exchange            │
                                                  └─────────────┬─────────────┘
                                                                │ routed by priority
                                                                ▼
                                                ┌───────────────────────────────┐
                                                │ Worker (cmd/worker)           │
                                                │ email / inapp / webhook       │
                                                │ + Redis idempotency checks    │
                                                └─────────────┬─────────────────┘
                                                              │
                                   ┌──────────────────────────┴──────────────────────────┐
                                   ▼                                                     ▼
                     PostgreSQL (notifications)                              Prometheus metrics
```

Infrastructure dependencies:

- RabbitMQ
- Redis
- PostgreSQL

---

## Core Capabilities

- **Multi-channel asynchronous delivery** with independent worker logic per channel.
- **Priority-aware queue lanes** (`high`, `normal`, `low`) across all channels.
- **Dead-letter replay operations** via `POST /dlq/replay`.
- **Load-test orchestration** with `real` (k6 upstream) and `demo` modes.
- **Real-time operations UI** for dashboard metrics, live event feed, notifications, and DLQ controls.

---

## API Surface

### Producer HTTP Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/events` | Publish a new event to RabbitMQ |
| `GET` | `/notifications` | List latest persisted notifications |
| `POST` | `/notifications/clear` | Clear notifications up to a cutoff timestamp |
| `POST` | `/dlq/replay` | Replay messages from a dead-letter queue |
| `POST` | `/ops/loadtest/start` | Start a load-test run (`demo` or `real`) |
| `GET` | `/ops/loadtest/{run_id}` | Fetch a specific load-test run |
| `GET` | `/ops/loadtest/latest` | Fetch the most recent load-test run |
| `GET` | `/ws` | WebSocket endpoint for live event stream |
| `GET` | `/api/metrics/summary` | Aggregated metrics snapshot |
| `GET` | `/metrics` | Producer Prometheus metrics |
| `GET` | `/health` | Health check |

### Producer gRPC Endpoint

- Service: `event.v1.EventService`
- Method: `Publish`

---

## Getting Started

### Prerequisites

- Go `1.22+`
- Node.js `20+`
- Docker and Docker Compose

### Local Setup

```bash
cp .env.example .env
```

```bash
cd deploy
docker compose up -d rabbitmq redis postgres
```

```bash
# terminal 1 (repo root)
go run ./cmd/producer

# terminal 2 (repo root)
go run ./cmd/worker

# terminal 3
cd web
npm install
npm run dev
```

Default local endpoints:

- Frontend: `http://localhost:3000`
- Producer API: `http://localhost:8080`
- Producer health: `http://localhost:8080/health`
- Worker metrics: `http://localhost:9091/metrics`

### Quick Smoke Test

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"type":"payment.completed","priority":"high","payload":{"email":"user@example.com"}}'
```

---

## Deployment

Deployment assets are under `deploy/`:

- `deploy/Dockerfile.producer`
- `deploy/Dockerfile.worker`
- `deploy/Dockerfile.web`
- `deploy/railway.toml`
- `deploy/railway.worker.toml`
- `deploy/railway.web.toml`

For full instructions, see [docs/DEPLOYMENT.md](./docs/DEPLOYMENT.md).

---

## Observability

- Producer metrics: `GET /metrics`
- Worker metrics (default): `http://localhost:9091/metrics`
- Summary API for dashboard polling: `GET /api/metrics/summary`
- Optional local monitoring stack: Prometheus + Grafana via `deploy/docker-compose.yml`

---

## Documentation

- [Documentation Index](./docs/README.md)
- [Architecture](./docs/ARCHITECTURE.md)
- [Local Development Runbook](./docs/RUN_LOCAL.md)
- [Deployment Guide](./docs/DEPLOYMENT.md)
- [Environment Variable Reference](./docs/ENVIRONMENT.md)
- [Repository Structure](docs/STRUCTURE.md)

---

## License

MIT
