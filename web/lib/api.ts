import type { Notification, MetricsSummary } from '@/types'

const BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080'

async function fetchJSON<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, init)
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${res.statusText}`)
  }
  return res.json() as Promise<T>
}

export async function getNotifications(): Promise<Notification[]> {
  return fetchJSON<Notification[]>('/notifications')
}

export async function getMetricsSummary(): Promise<MetricsSummary> {
  return fetchJSON<MetricsSummary>('/api/metrics/summary')
}

export async function postEvent(body: unknown): Promise<{ message_id: string }> {
  return fetchJSON<{ message_id: string }>('/events', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
}

export async function getLoadTestLatest(): Promise<unknown> {
  return fetchJSON<unknown>('/ops/loadtest/latest')
}

export async function replayDLQ(): Promise<void> {
  const res = await fetch(`${BASE}/dlq/replay`, { method: 'POST' })
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${res.statusText}`)
  }
}
