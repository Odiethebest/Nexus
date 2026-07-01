# Nexus: Event-Driven Notification Platform

> A production-oriented messaging system for multi-channel notifications, combining a Go backend with a real-time Next.js operations dashboard.

[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go)](https://go.dev/)
[![Next.js](https://img.shields.io/badge/Next.js-16-000000?logo=next.js)](https://nextjs.org/)
[![Redpanda](https://img.shields.io/badge/Redpanda-24.2-blue)](https://redpanda.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis)](https://redis.io/)
[![License](https://img.shields.io/badge/License-MIT-green)](./LICENSE)

---

## Table of Contents

- [Overview](#overview)
- [Background](#background)
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

Nexus routes events through **Redpanda (Kafka-protocol)** and processes them via dedicated workers for `email`, `inapp`, and `webhook` delivery channels. It enforces idempotency with Redis, persists delivery history in PostgreSQL, adds a Redis cache-aside path in front of the notifications store, and exposes end-to-end three-stage latency tracing through Prometheus metrics and a Next.js dashboard.

The system is split into two backend services:

- **Producer** (`cmd/producer`): HTTP/gRPC ingestion, WebSocket endpoint, cache-aside read path, DLQ replay, consumer-lag sampler, load-test control APIs.
- **Worker** (`cmd/worker`): nine independent lane runners (3 channels × 3 priorities), each its own Kafka consumer group.

### What It Handles

| Concern | Mechanism |
|---|---|
| Event ingestion | HTTP (`POST /events`) and gRPC (`Publish`) |
| Priority routing | Per-lane topics `nexus.<channel>.<priority>` (9 primary + 9 DLQ) |
| Producer durability | franz-go idempotent producer, `acks=all`, async batched `Produce` |
| Consumer at-least-once | manual offset commit only after `SaveNotification`; `BlockRebalanceOnPoll` |
| Idempotency | Redis `SETNX`, key scoped by channel: `msg:<channel>:<id>` |
| Retry-with-backoff | `x-retry-count` header, exponential 2s/4s/8s, MaxRetries=3 → DLQ |
| DLQ | Dedicated topics `nexus.dlq.<channel>.<priority>`; `POST /dlq/replay` |
| Cache-aside | `GET /notifications/{message_id}` → Redis (`cache:notif:<id>`, TTL 60s) → PostgreSQL |
| Three-stage tracing | Histograms `nexus_stage_{ingest,processing,delivery}_duration_seconds` + `nexus_event_e2e_lag_seconds` |
| Delivery history | PostgreSQL `notifications` table, `PK (message_id, channel)` |
| Runtime visibility | Prometheus + provisioned Grafana dashboard (`Nexus — Kafka pipeline`) + WebSocket feed |

---

## Background

This repository is inspired by — and partially draws on themes from — a project I was involved with during my internship at **QAX Technology Group (奇安信科技集团)**. Nexus is a clean-room rebuild in Go, written outside of work for personal learning and to practice applying what I picked up there (priority-aware message routing, idempotent delivery, dead-letter handling, real-time fan-out, full-pipeline observability). No proprietary code, data, or business logic from the internship is reproduced here; the implementation, dependency choices, and design trade-offs are entirely my own.

---

## Architecture

```mermaid
flowchart LR
  C[Clients] -->|HTTP / gRPC| P(Producer<br/>cmd/producer)
  P -->|Publish fan-out<br/>acks=all + idempotent| K{{Redpanda<br/>Kafka protocol}}
  K --> EH[nexus.email.high/normal/low]
  K --> IH[nexus.inapp.high/normal/low]
  K --> WH[nexus.webhook.high/normal/low]
  EH --> W(Worker<br/>cmd/worker)
  IH --> W
  WH --> W
  W -->|SaveNotification| PG[(PostgreSQL)]
  W -->|SETNX msg:channel:id| R[(Redis)]
  W -.retry-w-backoff.-> K
  W -.permanent fail.-> DLQ[nexus.dlq.<ch>.<pri>]
  P -->|Cache-aside<br/>GET /notifications/{id}| R
  P -.->|LagReader kadm 3s| K
  P & W -->|/metrics| PROM[Prometheus]
  PROM --> GRAF[Grafana dashboard]
```

Each `(channel, priority)` pair is a separate Kafka topic + a dedicated consumer group. Independent groups mean a stuck low-priority lane never gates committed offsets on the high-priority lane — that's the AMQP-era "high never blocked by low" guarantee, preserved on Kafka.

Infrastructure dependencies:

- Redpanda (single-binary, KRaft mode; Kafka-protocol compatible — franz-go client used on both sides)
- Redis (idempotency + cache-aside)
- PostgreSQL (notifications history)

### Partition sizing

The partition count is deliberately derived, not guessed. From `KAFKA_TOPIC_PARTITIONS` (default 12) in `internal/kbroker/config.go`:

> **target throughput ÷ per-partition throughput (empirical: 5–10K msg/s) + ~30% headroom**
>
> Example: **50 000 msg/s ÷ 5 000 msg/s/partition ≈ 10 partitions, +30% → 12 partitions.**

Effective parallelism per consumer group = `min(partition count, consumer instance count × goroutines-per-instance)`. Scale horizontally by adding more worker replicas (`docker compose up -d --scale worker=3`) — the group coordinator rebalances partitions across them.

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
| `POST` | `/events` | Publish a new event (fan-outs to all 3 channel topics) |
| `GET` | `/notifications` | List latest persisted notifications (short-TTL cached, scope=`list`) |
| `GET` | `/notifications/{message_id}` | Cache-aside single-record lookup (scope=`by_id`, TTL 60s) |
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

- Go `1.25+`
- Node.js `20+`
- Docker and Docker Compose

### Local Setup

```bash
cp .env.example .env
```

Full stack (Redpanda + Redis + PostgreSQL + producer + worker + Prometheus + Grafana):

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

Or, infrastructure only + run Go services from source:

```bash
docker compose -f deploy/docker-compose.yml up -d redpanda redis postgres

# terminal 1 (repo root)
KAFKA_BROKERS=localhost:19092 go run ./cmd/producer

# terminal 2 (repo root)
KAFKA_BROKERS=localhost:19092 go run ./cmd/worker

# terminal 3
cd web && npm install && npm run dev
```

Inspect Kafka:

```bash
docker exec deploy-redpanda-1 rpk topic list
docker exec deploy-redpanda-1 rpk group describe nexus.email.high
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

On Railway, use a managed **Redpanda Cloud** dev cluster (Railway has no built-in Kafka). Create the cluster, then set these env vars on each Railway service:

| Variable | Value |
|---|---|
| `KAFKA_BROKERS` | `<seed>.redpanda.cloud:9092` |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-256` |
| `KAFKA_SASL_USER` / `KAFKA_SASL_PASS` | Redpanda Cloud user + password |
| `KAFKA_TLS` | `true` |
| `KAFKA_TOPIC_PARTITIONS` | `12` |
| `KAFKA_REPLICATION_FACTOR` | `3` (Redpanda Cloud dev clusters run 3 nodes) |

Rolling updates are zero-downtime: Railway waits for the new instance to pass `/health` before killing the old one; on the Kafka side, consumer-group rebalance moves partitions between the old and new worker (see `RUNBOOK.md` for the exact validation procedure).

For deeper deploy notes see [docs/DEPLOYMENT.md](./docs/DEPLOYMENT.md).

---

## Observability

- Producer metrics: `GET /metrics`
- Worker metrics (default): `http://localhost:9091/metrics`
- Summary API for dashboard polling: `GET /api/metrics/summary`
- Optional local monitoring stack: Prometheus + Grafana via `deploy/docker-compose.yml`

---

## Documentation

- **[MIGRATION.md](./MIGRATION.md)** — RabbitMQ → Redpanda refactor board (this repo's transition history + design deltas)
- **[RUNBOOK.md](./RUNBOOK.md)** — résumé-bullet ↔ code ↔ metric mapping with reproduction commands
- [Documentation Index](./docs/README.md)
- [Architecture](./docs/ARCHITECTURE.md)
- [Local Development Runbook](./docs/RUN_LOCAL.md)
- [Deployment Guide](./docs/DEPLOYMENT.md)
- [Environment Variable Reference](./docs/ENVIRONMENT.md)
- [Repository Structure](docs/STRUCTURE.md)

---

## License

MIT
