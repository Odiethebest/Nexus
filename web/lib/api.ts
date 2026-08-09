const BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080'

export async function getNotifications() {
  const res = await fetch(`${BASE}/notifications`)
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

export async function getMetricsSummary() {
  const res = await fetch(`${BASE}/api/metrics/summary`)
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

export async function getLoadTestLatest() {
  const res = await fetch(`${BASE}/ops/loadtest/latest`)
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

export type LoadTestMode = 'demo' | 'real'

export async function postEvent(body: unknown) {
  const res = await fetch(`${BASE}/events`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

/**
 * Starts a load test. `real` drives Grafana Cloud k6 and is gated server-side
 * by Guard.Authorize, which constant-time-compares X-Admin-Key against
 * LOADTEST_ADMIN_KEY — so the key has to travel on the request. `demo`
 * ignores it entirely.
 */
export async function startLoadTest(mode: LoadTestMode = 'demo', adminKey?: string) {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  if (adminKey) headers['X-Admin-Key'] = adminKey

  const res = await fetch(`${BASE}/ops/loadtest/start`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ mode }),
  })
  if (!res.ok) {
    // The server distinguishes these, and they mean very different things to
    // whoever is standing at the console.
    const detail = await res.json().catch(() => null)
    throw new Error(detail?.error ? `${detail.error} (HTTP ${res.status})` : `HTTP ${res.status}`)
  }
  return res.json()
}

export async function clearNotifications() {
  const res = await fetch(`${BASE}/notifications/clear`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ before_unix_ms: Date.now() }),
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

export async function replayDLQ(queue: string, max = 100) {
  const res = await fetch(`${BASE}/dlq/replay`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ queue, max }),
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}
