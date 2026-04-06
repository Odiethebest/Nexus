# Local Development Runbook

## 1. Prerequisites

- Go `1.22+`
- Node.js `20+`
- Docker and Docker Compose

## 2. Start Infrastructure

From the repository root:

```bash
cd deploy
docker compose up -d rabbitmq redis postgres
```

Default ports:

- RabbitMQ AMQP: `5672` (management UI: `15672`)
- Redis: `6379`
- PostgreSQL: `5432`

## 3. Start Backend Services

Open two terminals from the repository root:

```bash
go run ./cmd/producer
```

```bash
go run ./cmd/worker
```

Default service ports:

- Producer HTTP: `8080`
- Producer gRPC: `50051`
- Worker metrics: `9091`

## 4. Start Frontend

```bash
cd web
npm install
npm run dev
```

Default frontend URL: `http://localhost:3000`

## 5. Smoke Test Endpoints

- Dashboard: `http://localhost:3000/dashboard`
- Publish test event: `http://localhost:3000/publish`
- Producer health: `http://localhost:8080/health`
- Producer metrics: `http://localhost:8080/metrics`
- Worker metrics: `http://localhost:9091/metrics`

## 6. Test Commands

Unit and package tests:

```bash
go test ./...
```

Integration tests (container-backed):

```bash
go test ./... -tags=integration
```
