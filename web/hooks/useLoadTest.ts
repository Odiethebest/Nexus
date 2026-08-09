'use client'
import { useState, useEffect, useRef } from 'react'
import { startLoadTest, getLoadTestLatest, type LoadTestMode } from '@/lib/api'

export type LoadTestStatus = 'idle' | 'running' | 'completed' | 'error'

export interface LoadTestChartPoint {
  t:   number  // unix timestamp (seconds)
  rps: number
  p95: number
}

export interface LoadTestResult {
  run_id:    string
  status:    LoadTestStatus
  started_at: string
  completed_at?: string
  [key: string]: unknown
}

// Terminal statuses from the Go backend (loadtest/types.go)
const TERMINAL = new Set(['completed', 'aborted', 'error'])

export function useLoadTest() {
  const [status,    setStatus]    = useState<LoadTestStatus>('idle')
  const [result,    setResult]    = useState<LoadTestResult | null>(null)
  const [chartData, setChartData] = useState<LoadTestChartPoint[]>([])
  const [error,     setError]     = useState<string | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const start = async (mode: LoadTestMode = 'demo', adminKey?: string) => {
    try {
      setStatus('running')
      setError(null)
      setChartData([])
      // The run id comes back here, but polling uses /latest, so it is not
      // needed — we only await to surface a start failure.
      await startLoadTest(mode, adminKey)

      pollRef.current = setInterval(async () => {
        try {
          const d = await getLoadTestLatest()
          const runStatus = d?.run?.status ?? d?.status
          setResult(d)

          // Extract chart data from series tuples: [[ts, val], ...]
          const rpsPoints: [number, number][]  = d?.series?.rps     ?? []
          const p95Points: [number, number][]  = d?.series?.p95_ms  ?? []
          if (rpsPoints.length > 0) {
            const points: LoadTestChartPoint[] = rpsPoints.map(([t, rps], i) => ({
              t,
              rps: Math.round(rps * 10) / 10,
              p95: Math.round((p95Points[i]?.[1] ?? 0) * 10) / 10,
            }))
            setChartData(points)
          }

          if (TERMINAL.has(runStatus)) {
            setStatus(runStatus === 'aborted' ? 'error' : 'completed')
            if (runStatus === 'aborted') setError('Load test was aborted')
            clearInterval(pollRef.current!)
          }
        } catch (e) {
          console.warn('[loadtest] poll error:', e)
        }
      }, 2000)
    } catch (e) {
      console.error('[loadtest] start error:', e)
      setStatus('error')
      setError(String(e))
    }
  }

  const reset = () => {
    if (pollRef.current) clearInterval(pollRef.current)
    setStatus('idle')
    setResult(null)
    setChartData([])
    setError(null)
  }

  useEffect(() => () => { if (pollRef.current) clearInterval(pollRef.current) }, [])

  return { status, result, chartData, error, start, reset }
}
