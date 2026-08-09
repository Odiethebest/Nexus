'use client'
import { useState, useEffect, useCallback } from 'react'
import { getMetricsSummary } from '@/lib/api'
import type { MetricsSample, MetricsSummary } from '@/types'

export const POLL_INTERVAL_MS = 5000

/**
 * How much history to keep. The dashboard chart offers a 15-minute range, so
 * retention has to cover it — the previous 30-sample cap silently capped the
 * chart at ~2.5 minutes and made the 5m/15m toggles inert.
 */
export const HISTORY_WINDOW_MS = 15 * 60 * 1000

/** Guard against unbounded growth if a poll ever runs faster than expected. */
const MAX_SAMPLES = Math.ceil(HISTORY_WINDOW_MS / POLL_INTERVAL_MS) + 10

/**
 * Drops samples older than the retention window. Trimming by timestamp
 * rather than by count keeps the window honest when polling stalls (a
 * backgrounded tab throttles setInterval, so N samples is not N×5s).
 */
function pruneHistory(samples: MetricsSample[], now: number): MetricsSample[] {
  const cutoff = now - HISTORY_WINDOW_MS
  const fresh = samples.filter(s => s.received_at >= cutoff)
  return fresh.length > MAX_SAMPLES ? fresh.slice(-MAX_SAMPLES) : fresh
}

export function useMetrics() {
  const [latest, setLatest]   = useState<MetricsSummary | null>(null)
  const [history, setHistory] = useState<MetricsSample[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState<string | null>(null)
  const [tick, setTick]       = useState(0)

  const refresh = useCallback(() => setTick(t => t + 1), [])

  useEffect(() => {
    let cancelled = false

    const fetchData = async () => {
      try {
        const data: MetricsSummary = await getMetricsSummary()
        if (cancelled) return

        // Stamp arrival time here rather than assuming a fixed cadence, so
        // the chart's time axis reflects when readings actually landed.
        const sample: MetricsSample = { ...data, received_at: Date.now() }
        setLatest(data)
        setHistory(h => pruneHistory([...h, sample], sample.received_at))
        setLoading(false)
        setError(null)
      } catch (e) {
        if (!cancelled) {
          setError(String(e))
          setLoading(false)
        }
      }
    }

    fetchData()
    const id = setInterval(fetchData, POLL_INTERVAL_MS)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [tick])

  return { latest, history, loading, error, refresh }
}
