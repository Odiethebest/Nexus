'use client'
import { useState, useEffect, useCallback } from 'react'
import { getMetricsSummary } from '@/lib/api'
import type { MetricsSummary } from '@/types'

export function useMetrics() {
  const [latest, setLatest]   = useState<MetricsSummary | null>(null)
  const [history, setHistory] = useState<MetricsSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState<string | null>(null)
  const [tick, setTick]       = useState(0)

  const refresh = useCallback(() => setTick(t => t + 1), [])

  useEffect(() => {
    let cancelled = false

    const fetchData = async () => {
      try {
        const data = await getMetricsSummary()
        if (!cancelled) {
          setLatest(data)
          setHistory(h => [...h.slice(-29), data])
          setLoading(false)
          setError(null)
        }
      } catch (e) {
        if (!cancelled) {
          setError(String(e))
          setLoading(false)
        }
      }
    }

    fetchData()
    const id = setInterval(fetchData, 5000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [tick])

  return { latest, history, loading, error, refresh }
}
