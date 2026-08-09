# Local Development Runbook

## 1. Prerequisites

- Go `1.25+` (`go.mod` requires 1.25.0 — franz-go's minimum)
- Node.js `20+`
- Docker and Docker Compose

```bash
cp .env.example .env
```

Both services read `.env` from the repo root automatically
(`internal/envutil`); existing process env vars win over file values.

## 2. Start Infrastructure

From the repository root:

```bash
docker compose -f deploy/docker-compose.yml up -d redpanda redis postgres
```

Default ports:

- Redpanda Kafka (external listener, for host tools): `19092`
- Redpanda Admin API / public metrics: `9644`
- Redis: `6379`
- PostgreSQL: `5432`

In-cluster services address the broker as `redpanda:9092`; from the host you
must use `localhost:19092`.

## 3. Start Backend Services

Two terminals from the repository root:

```bash
KAFKA_BROKERS=localhost:19092 go run ./cmd/producer
```

```bash
KAFKA_BROKERS=localhost:19092 go run ./cmd/worker
```

`KAFKA_BROKERS` is required — there is deliberately no default, because
silently pointing at the wrong cluster is worse than failing to boot.

Default service ports:

- Producer HTTP: `8080`
- Producer gRPC: `50051`
- Worker metrics: `9091`

The producer creates all 18 topics at boot. Both services run the database
migration on startup.

## 4. Start Frontend

```bash
cd web
npm install
npm run dev
```

Default frontend URL: `http://localhost:3000`

> **Port clash:** the Grafana service in `deploy/docker-compose.yml` also
> binds `3000`. Bring up the full compose stack *or* run `npm run dev`, not
> both — or remap one of them.

## 5. Smoke Test

```bash
# publish
curl -X POST http://localhost:8080/events \
  -H 'content-type: application/json' \
  -d '{"type":"payment.completed","priority":"high","payload":{"email":"user@example.com"}}'

# one row per channel should appear
curl -s http://localhost:8080/notifications/<message_id>

# aggregated metrics the dashboard polls
curl -s http://localhost:8080/api/metrics/summary
```

Pages: `/dashboard`, `/live`, `/notifications`, `/loadtest`, `/dlq`,
`/publish`. Producer health: `http://localhost:8080/health`. Raw metrics:
`http://localhost:8080/metrics` and `http://localhost:9091/metrics`.

## 6. Inspecting Kafka

```bash
docker exec deploy-redpanda-1 rpk topic list
docker exec deploy-redpanda-1 rpk topic describe nexus.email.high
docker exec deploy-redpanda-1 rpk group list          # nine Stable groups
docker exec deploy-redpanda-1 rpk group describe nexus.email.high
docker exec deploy-redpanda-1 rpk topic consume nexus.email.high -n 1 --offset -1
```

Container names follow the compose project, which defaults to the directory
name (`deploy-*`).

## 7. Generating Load

```bash
go build -o bin/loadgen ./cmd/loadgen
./bin/loadgen -base http://localhost:8080 -rate 150 -dur 45s -read-ratio 20.0
```

Prints a JSON summary: achieved rate, publish p50/p95/p99, GET p99, `by_id`
cache hit rate over the load window, e2e lag p99, DLQ count. The read stream
is biased toward recently published ids, which is what drives the cache hit
rate.

## 8. Test Commands

```bash
go test ./...                                     # unit, no Docker needed
go test -race ./...
go test -tags=integration ./internal/integration/...   # needs Docker
```

Integration tests spin up Redpanda and PostgreSQL via testcontainers and skip
themselves when no Docker socket is present. Redis is faked with miniredis, so
no container is needed for it.

Frontend:

```bash
cd web
npm run typecheck
npm run lint
npm run build
```
