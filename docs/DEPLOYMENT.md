# Deployment Guide

## 1. Deployment Modes

Nexus supports two primary deployment modes:

- Local containerized stack via Docker Compose
- Cloud deployment on Railway with split services

## 2. Local Containerized Deployment

`deploy/docker-compose.yml` brings up a full local environment:

- Infrastructure: RabbitMQ, Redis, PostgreSQL
- Application: producer and worker
- Observability: Prometheus and Grafana

Start command:

```bash
cd deploy
docker compose up -d
```

Primary local ports:

- Producer: `8080`
- Prometheus: `9090`
- Grafana: `3000`

## 3. Railway Service Profiles

Railway is configured as three independently deployable services:

- Producer: `deploy/railway.toml` -> `deploy/Dockerfile.producer`
- Worker: `deploy/railway.worker.toml` -> `deploy/Dockerfile.worker`
- Web: `deploy/railway.web.toml` -> `deploy/Dockerfile.web`

Each service has an isolated build context and start command.

## 4. Build Characteristics

### Producer and Worker

- Go multi-stage builds
- Final runtime images include only binaries and CA certificates

### Web

- Next.js standalone output (`output: "standalone"`)
- `NEXT_PUBLIC_API_URL` and `NEXT_PUBLIC_WS_URL` are injected at build time
- Runtime entrypoint: `node server.js`

## 5. Data and Migration Behavior

- Both producer and worker execute database migration on startup.
- Ensure `POSTGRES_DSN` is valid and reachable before first deployment.

## 6. Pre-Deployment Checklist

- Required environment variables are configured (see `ENVIRONMENT.md`).
- RabbitMQ, Redis, and PostgreSQL connectivity is verified.
- Producer health check (`/health`) returns HTTP `200`.
- Web build-time public URLs target the correct producer domain and WebSocket endpoint.
