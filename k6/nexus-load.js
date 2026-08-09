// Nexus pipeline load test.
//
// Drives the same two streams as cmd/loadgen so the numbers are comparable:
//   1. POST /events                       — the write path (fan-out to 3 lanes)
//   2. GET  /notifications/{message_id}   — the cache-aside read path
//
// Reads are biased toward recently published ids, which is what produces a
// meaningful by_id cache hit rate: a cold random id would miss every time.
//
// Run locally:
//   k6 run -e BASE_URL=http://localhost:8080 -e TARGET_RPS=200 k6/nexus-load.js
//
// Run on Grafana Cloud k6 (see k6/README.md for the one-time setup):
//   k6 cloud run -e BASE_URL=https://<producer-host> -e TARGET_RPS=2000 k6/nexus-load.js
//
// The thresholds below are the résumé claims written as assertions, so a run
// that does not meet them exits non-zero instead of quietly reporting a
// number nobody checks.

import http from 'k6/http'
import { check } from 'k6'

const BASE       = __ENV.BASE_URL   || 'http://localhost:8080'
const TARGET_RPS = Number(__ENV.TARGET_RPS || 200)
const DURATION   = __ENV.DURATION   || '60s'
const RAMP       = __ENV.RAMP       || '20s'
const READ_RATIO = Number(__ENV.READ_RATIO || 2)

// An arrival-rate executor needs enough VUs to sustain the rate even when
// responses slow down; k6 warns and throttles if it runs out. Sized off the
// target and overridable when a run proves the estimate wrong.
const PRE_VUS = Number(__ENV.PRE_ALLOCATED_VUS || Math.max(50, Math.ceil(TARGET_RPS / 5)))
const MAX_VUS = Number(__ENV.MAX_VUS || PRE_VUS * 4)

export const options = {
  scenarios: {
    pipeline: {
      executor: 'ramping-arrival-rate',
      startRate: Math.max(1, Math.round(TARGET_RPS / 10)),
      timeUnit: '1s',
      preAllocatedVUs: PRE_VUS,
      maxVUs: MAX_VUS,
      stages: [
        { target: TARGET_RPS, duration: RAMP },      // find the ceiling gradually
        { target: TARGET_RPS, duration: DURATION },  // hold: this is the measured window
      ],
    },
  },
  thresholds: {
    // "publish p99 < 50ms" — the claim, as an assertion.
    'http_req_duration{op:publish}': ['p(99)<50'],
    'http_req_failed{op:publish}': ['rate<0.01'],
    // 404 is a legitimate read outcome (the worker may not have persisted
    // yet), so reads declare it an expected status below and this threshold
    // then only catches transport errors and 5xx.
    'http_req_failed{op:read}': ['rate<0.05'],
  },
}

const EVENT_TYPES = ['payment.completed', 'order.shipped', 'user.signup', 'alert.warning']
const PRIORITIES = ['high', 'normal', 'low']

// VU-local ring of recently published ids. Each k6 VU has its own JS runtime,
// so this needs no synchronisation — and a per-VU working set is a fair model
// of "one user re-reading their own notifications".
const recent = []
const RECENT_MAX = 200

function pick(xs) {
  return xs[Math.floor(Math.random() * xs.length)]
}

export default function () {
  const body = JSON.stringify({
    type: pick(EVENT_TYPES),
    priority: pick(PRIORITIES),
    payload: { amount: 99.99, currency: 'USD', k6: true },
  })

  const res = http.post(`${BASE}/events`, body, {
    headers: { 'Content-Type': 'application/json' },
    tags: { op: 'publish' },
  })
  check(res, { 'publish accepted (202)': r => r.status === 202 }, { op: 'publish' })

  if (res.status === 202) {
    try {
      const id = res.json('message_id')
      if (id) {
        recent.push(id)
        if (recent.length > RECENT_MAX) recent.shift()
      }
    } catch (_) {
      // body was not the JSON we expect; the check above already recorded it
    }
  }

  // READ_RATIO reads per write, fractional part handled probabilistically so
  // the long-run average matches (2.5 => alternating 2 and 3).
  let reads = Math.floor(READ_RATIO)
  if (Math.random() < READ_RATIO - reads) reads += 1

  for (let i = 0; i < reads && recent.length > 0; i++) {
    const id = recent[Math.floor(Math.random() * recent.length)]
    const r = http.get(`${BASE}/notifications/${id}`, {
      tags: { op: 'read' },
      // Without this k6 scores every 404 as a failed request, which would
      // put http_req_failed{op:read} above 50% on a healthy run and make the
      // threshold meaningless.
      responseCallback: http.expectedStatuses(200, 404),
    })
    check(r, { 'read resolved (200/404)': x => x.status === 200 || x.status === 404 }, { op: 'read' })
  }
}
