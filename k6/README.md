# k6 load test

`nexus-load.js` drives the same two streams as `cmd/loadgen` — `POST /events`
plus cache-aside reads of recently published ids — so local and cloud runs are
comparable.

The résumé claims are encoded as k6 thresholds, so a run that misses them exits
non-zero instead of quietly printing a number nobody checks:

| Threshold | Claim |
|---|---|
| `http_req_duration{op:publish} p(99)<50` | publish p99 under 50 ms |
| `http_req_failed{op:publish} rate<0.01` | ingestion does not shed load |
| `http_req_failed{op:read} rate<0.05` | read path stays healthy |

Reads declare `200, 404` as expected statuses. A 404 is a legitimate outcome —
the worker may not have persisted yet — and without that, `http_req_failed`
would sit above 50% on a perfectly healthy run.

---

## Running locally

No k6 install needed; use the official image against the compose network:

```bash
docker compose -f deploy/docker-compose.yml up -d redpanda redis postgres producer worker

docker run --rm -i --network deploy_default \
  -e BASE_URL=http://producer:8080 \
  -e TARGET_RPS=500 -e RAMP=8s -e DURATION=20s -e READ_RATIO=2 \
  grafana/k6:latest run - < k6/nexus-load.js
```

With k6 installed on the host, point it at the published port instead:

```bash
k6 run -e BASE_URL=http://localhost:8080 -e TARGET_RPS=500 k6/nexus-load.js
```

### Knobs

| Env | Default | Notes |
|---|---|---|
| `BASE_URL` | `http://localhost:8080` | producer base URL |
| `TARGET_RPS` | `200` | **events**/s, not HTTP req/s — each iteration is 1 publish + `READ_RATIO` reads |
| `DURATION` | `60s` | hold time at target, after the ramp |
| `RAMP` | `20s` | time spent ramping up to target |
| `READ_RATIO` | `2` | reads per publish; fractional values are handled probabilistically |
| `PRE_ALLOCATED_VUS` / `MAX_VUS` | derived from `TARGET_RPS` | raise if k6 warns it ran out of VUs |

---

## Running on Grafana Cloud k6

This is the part the repo cannot do for you: `K6_LOAD_TEST_ID` refers to a test
resource that lives in Grafana Cloud, not here. Create it once.

```bash
# 1. Authenticate (token from Grafana Cloud > k6 > Personal API token)
k6 cloud login --token <K6_API_TOKEN>

# 2. Upload and run. This creates the test resource on first run and prints
#    its URL; the numeric id in that URL is your K6_LOAD_TEST_ID.
k6 cloud run \
  -e BASE_URL=https://<your-producer-host> \
  -e TARGET_RPS=2000 \
  k6/nexus-load.js
```

Then set on the **producer** service so its `/ops/loadtest/*` endpoints can
drive that test:

| Variable | Where it comes from |
|---|---|
| `LOADTEST_ENABLED=true` | — |
| `LOADTEST_ADMIN_KEY` | any secret you choose; the UI asks for it |
| `K6_API_TOKEN` | Grafana Cloud personal API token |
| `K6_STACK_ID` | your Grafana Cloud stack id |
| `K6_LOAD_TEST_ID` | numeric id from step 2 |

All four are **hard preconditions**: with `LOADTEST_ENABLED=true` and any of
them missing the producer exits at boot rather than degrading.

The target must be reachable from the public internet — cloud load generators
cannot see `localhost`. Point `BASE_URL` at the deployed Railway producer.

> **Cost.** Cloud runs consume VUh against your plan. `LOADTEST_BUDGET_VUH_PER_DAY`
> caps what the orchestrator will spend per day; leave it at `0` only if you
> are watching the bill.

---

## Measured results

Local, this repo's compose stack **and** k6 all on one M-series laptop, so k6
competes with the system under test for CPU. Ramp 8s, hold 20s, `READ_RATIO=2`.

| Target | Achieved (events/s) | publish p99 | Threshold |
|---|---|---|---|
| 200/s | 174 | 12.4 ms | pass |
| 500/s | 435 | 13.6 ms | pass |
| 600/s | 523 | 44.1 ms | pass |
| 700/s | 609 | 50.5 ms | **fail** |
| 800/s | 695 | 84.8 ms | fail |
| 1000/s | 870 | 78.6 ms | fail |
| 2000/s | 1703 | 157.0 ms | fail |

**Publish p99 stays under 50 ms up to roughly 520 events/s** (~1,560 records/s
into Kafka, since every event fans out to three lanes). Past that it degrades
in *latency only* — publish error rate was 0.00% at every rate tested, up to
2000/s.

Consumer side, measured by draining a ~119k-record backlog with no new load:

```
  t+  6s  backlog= 118868 records  processed= 1786.9 rec/s
  t+ 18s  backlog=  90843 records  processed= 2605.6 rec/s
  t+ 30s  backlog=  54907 records  processed= 3355.2 rec/s
  t+ 48s  backlog=   9751 records  processed= 2867.0 rec/s
```

**Workers drain ~2,500–3,400 records/s**, i.e. ~830–1,100 events/s equivalent.
So on this hardware the binding constraint is the publish-latency SLO (~520
events/s), not consumer throughput — at or below that rate the pipeline keeps
up and the backlog stays flat.

### Not measured

A Grafana Cloud run against the deployed producer. That needs an account, a
token and a public target, none of which live in this repo. When you run it,
replace this section with the numbers rather than adding to them — and expect
the Railway hobby instance plus a Redpanda Cloud dev cluster to bind well
before these local figures, not after.
