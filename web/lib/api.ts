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

export async function postEvent(body: unknown) {
  const res = await fetch(`${BASE}/events`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

export async function startLoadTest(mode = 'demo') {
  const res = await fetch(`${BASE}/ops/loadtest/start`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mode }),
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
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
