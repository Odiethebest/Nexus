# Nexus Backend

Nexus is a Go backend for event ingestion, asynchronous notification delivery, and durable storage.

## Scope

- Producer API (`cmd/producer`)
- Worker service (`cmd/worker`)
- Message broker integration (RabbitMQ)
- Idempotency guard (Redis)
- Notification persistence (PostgreSQL)
- Load test orchestration APIs (`/ops/loadtest/*`)

This repository now keeps backend and database-related code only.

## Runtime dependencies

- PostgreSQL
- RabbitMQ
- Redis

## Local run

```bash
cd deploy
docker compose up -d rabbitmq redis postgres

# terminal 1
go run ./cmd/producer

# terminal 2
go run ./cmd/worker
```

Producer listens on `:8080` by default.

## Tests

```bash
go test ./...
```

## License

MIT
