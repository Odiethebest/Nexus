# Testing Guide

## Test Layers

| Layer | Package | Tag | Dependencies |
|-------|---------|-----|--------------|
| Unit | `internal/idempotency` | *(none)* | miniredis (in-process) |
| Integration | `internal/store` | `integration` | PostgreSQL via testcontainers |
| Integration | `internal/integration` | `integration` | RabbitMQ + PostgreSQL via testcontainers, miniredis |

---

## Running Tests

### Unit tests (no Docker needed)

```bash
go test ./internal/idempotency/... -v
```

All five cases run against an in-process [miniredis](https://github.com/alicebob/miniredis) instance — no external Redis required.

### Integration tests (requires Docker)

```bash
go test -tags=integration ./internal/store/... -v -timeout 120s
go test -tags=integration ./internal/integration/... -v -timeout 120s
```

testcontainers-go pulls `rabbitmq:3.13-alpine` and `postgres:16-alpine` images on first run and starts them as ephemeral containers for the duration of each test. Redis uses miniredis — no container needed.

### All tests

```bash
go test ./...                              # unit only
go test -tags=integration ./... -timeout 120s  # unit + integration
```

### Frontend tests (9.2)

```bash
npm --prefix web run test:run
```

This runs Vitest + Testing Library suites for the Stress Lab state machine, API contract parsing, and snapshots.

### Manual E2E checklist (9.3)

Run API-level checklist first:

```bash
chmod +x scripts/loadtest_manual_e2e.sh
ADMIN_KEY='<your-loadtest-admin-key>' \
BASE_URL='http://localhost:8080' \
./scripts/loadtest_manual_e2e.sh
```

What the script verifies:
- Start run with valid key returns `202`.
- Second start while active returns `409`.
- Run status endpoint is polled until terminal state (`completed` or `aborted`).
- Cooldown is enforced with `429` and a `retry in ...` hint (unless `EXPECT_COOLDOWN=0`).

Then verify UI behavior manually in the dashboard:
- Completed run displays `FINAL SCORE` and three insight bullets.
- Upstream failure displays actionable error hint:
  - easiest trigger is temporarily setting an invalid `K6_API_TOKEN`, redeploying producer, and clicking **Start Load Test**.
- Cooldown window shows visible countdown copy (`Try again in MM:SS`).

---

## Test Coverage by Package

| Package | Tests | What's covered |
|---------|-------|---------------|
| `internal/idempotency` | 5 unit tests | First-seen returns true; duplicate returns false; empty ID always passes; independent IDs each allowed once; TTL expiry re-allows delivery |
| `internal/store` | 4 integration tests | Save and list; upsert updates status on conflict; results ordered by `created_at DESC`; `limit` is respected |
| `internal/integration` | 3 integration tests | Full publish → worker → DB pipeline; idempotent delivery (no duplicate rows); fan-out across all three channels (email, inapp, webhook) |

Packages without dedicated tests include `broker`, `hub`, `mailer`, `metrics`, `replay`, and `grpcserver`. Pipeline integration tests exercise broker/worker/store paths, but do not directly cover replay or gRPC server handlers.

---

## Unit Tests — `internal/idempotency`

```
TestCheck_FirstTime_ReturnsTrue       — new message ID returns true (first-seen)
TestCheck_Duplicate_ReturnsFalse      — same ID on second call returns false
TestCheck_EmptyID_AlwaysAllows        — empty string always returns true (passthrough)
TestCheck_IndependentIDs_EachAllowedOnce — different IDs are tracked independently
TestCheck_TTLExpiry_AllowsRedelivery  — after 25h (FastForward), same ID is allowed again
```

Key detail: `mr.FastForward(25 * time.Hour)` advances miniredis's internal clock, expiring the key without real-time waiting.

---

## Integration Tests — `internal/store`

Each test spins up a fresh `postgres:16-alpine` container with database `nexus_test`. `store.Migrate` runs before each test, ensuring a clean schema.

```
TestStore_SaveAndList                         — round-trip: save one record, list it back
TestStore_Upsert_UpdatesStatus                — saving same (message_id, channel) updates status field
TestStore_ListNotifications_OrderedByCreatedAtDesc — newest record appears first
TestStore_ListNotifications_RespectsLimit     — list(3) returns exactly three rows
```

---

## Integration Tests — `internal/integration`

`setupPipeline` starts:
- RabbitMQ container (testcontainers)
- PostgreSQL container (testcontainers)
- miniredis instance

All three tests reuse `setupPipeline` via `t.Helper()` and register cleanup with `t.Cleanup`.

```
TestPipeline_PublishDeliveredToDB
  Publishes one event, starts email worker, polls DB until a row appears (10 s deadline).
  Asserts: message_id matches, status == "delivered".

TestPipeline_IdempotentDelivery_OneRowOnly
  Publishes one event, waits for delivery, then publishes another event to ensure worker liveness.
  Asserts: the original message_id appears exactly once in persisted rows.

TestPipeline_MultipleWorkers_AllChannelsDeliver
  Starts email + inapp + webhook workers, publishes one event.
  Polls DB until ≥ 3 rows (15 s deadline).
  Asserts: rows cover all three channels (email, inapp, webhook).
```

`waitForRows` polls `store.ListNotifications` every 100 ms until the expected count appears or the deadline expires.

---

## Adding New Tests

### For a new internal package

1. Create `internal/<pkg>/<pkg>_test.go`.
2. For pure logic (no I/O): use standard `testing` and miniredis/mocks as needed — no build tag.
3. For tests that require Docker: add `//go:build integration` at the top.

### For a new worker channel

Add a case to `TestPipeline_MultipleWorkers_AllChannelsDeliver` that:
1. Starts the new worker.
2. Extends the channel set assertion to include the new channel name.
