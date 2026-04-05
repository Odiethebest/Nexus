'use client'

import { useState, useEffect, useCallback, useRef } from 'react'
import { getMetricsSummary } from '@/lib/api'
import type { MetricsSummary } from '@/types'

const MAX_HISTORY = 30
const POLL_INTERVAL = 5000

export function useMetrics() {
  const [latest, setLatest] = useState<MetricsSummary | null>(null)
  const [history, setHistory] = useState<MetricsSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const poll = useCallback(async () => {
    try {
      const data = await getMetricsSummary()
      setLatest(data)
      setHistory(prev => [...prev.slice(-(MAX_HISTORY - 1)), data])
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch metrics')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    poll()
    intervalRef.current = setInterval(poll, POLL_INTERVAL)
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
  }, [poll])

  return { latest, history, loading, error }
}
