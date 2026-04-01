# Nexus — TODO

Tasks are ordered bottom-up by dependency. Complete each block before
moving to the next.

---

## Block 1 — Local environment bootstrap ✅
> Nothing else can be tested until this is in place.

- [x] Add `.env.example` with all required env vars
      (`AMQP_URL`, `REDIS_URL`, `POSTGRES_DSN`, `LISTEN_ADDR`,
      `EMAIL_WORKER_POOL`, `INAPP_WORKER_POOL`, `WEBHOOK_WORKER_POOL`)
- [x] Run `npm install` inside `web/` and commit `package-lock.json`
- [x] Verify `docker compose -f deploy/docker-compose.yml up` brings up
      all five services cleanly end-to-end

---

## Block 2 — Broker reliability
> Workers and producer depend on a stable connection.

- [ ] Add publisher confirms to `internal/broker/publisher.go`
      (enable confirm mode on the channel, wait for ack before returning)
- [ ] Add reconnect logic to `internal/broker/connection.go`
      (watch the connection's NotifyClose channel, back off and re-dial)

---

## Block 3 — Email worker delivery
> Depends on Block 1 (env vars for SMTP credentials).

- [ ] Integrate a real email provider in `internal/worker/email.go`
      (SMTP via `net/smtp`, or a provider SDK such as Resend / SendGrid)
- [ ] Add `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`,
      `EMAIL_FROM` to `.env.example`

---

## Block 4 — Frontend polish
> Depends on Block 1 (npm install done).

- [ ] Add `web/src/index.css` with a CSS reset and base styles
- [ ] Import `index.css` in `web/src/main.jsx`
- [ ] Replace inline styles in `App.jsx` with CSS classes
- [ ] Add loading skeleton / empty-state illustration to `NotificationFeed`

---

## Block 5 — Testing
> Depends on Blocks 2–3 (stable broker + real workers).

- [ ] Unit test `internal/idempotency` (mock Redis with miniredis)
- [ ] Unit test `internal/store` (use pgx test containers or sqlmock)
- [ ] Integration test the full publish → worker → DB pipeline
      (spin up infra via `testcontainers-go`)

---

## Block 6 — Observability
> Depends on Block 5 (baseline correctness verified).

- [ ] Expose Prometheus metrics endpoint (`/metrics`) in the producer
- [ ] Add counters: messages published, messages processed per channel,
      DLQ drops, duplicate skips
- [ ] Add a Grafana dashboard definition to `deploy/grafana/`

---

## Block 7 — Roadmap features
> Depends on Blocks 1–5 being stable.

- [ ] Priority queue routing — bind separate queues for
      `event.*.high`, `event.*.normal`, `event.*.low` with different
      consumer prefetch counts
- [ ] DLQ replay endpoint — `POST /dlq/{queue}/replay` re-publishes
      messages from the DLQ back to the main exchange
- [ ] gRPC producer API — add `proto/event.proto` and a gRPC server
      alongside the existing HTTP server in `cmd/producer`
