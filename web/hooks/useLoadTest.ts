'use client'
import { useState, useEffect, useRef } from 'react'
import { startLoadTest, getLoadTestLatest } from '@/lib/api'

export type LoadTestStatus = 'idle' | 'running' | 'completed' | 'error'

export interface LoadTestResult {
  run_id: string
  status: LoadTestStatus
  started_at: string
  completed_at?: string
  [key: string]: unknown
}

// Terminal statuses from the Go backend (loadtest/types.go)
const TERMINAL = new Set(['completed', 'aborted', 'error'])

export function useLoadTest() {
  const [status, setStatus]   = useState<LoadTestStatus>('idle')
  const [result, setResult]   = useState<LoadTestResult | null>(null)
  const [error, setError]     = useState<string | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const start = async () => {
    try {
      setStatus('running')
      setError(null)
      console.log('[loadtest] POST /ops/loadtest/start { mode: "demo" }')
      const startResp = await startLoadTest('demo')
      console.log('[loadtest] start response:', startResp)

      pollRef.current = setInterval(async () => {
        try {
          const d = await getLoadTestLatest()
          const runStatus = d?.run?.status ?? d?.status
          console.log('[loadtest] poll run.status:', runStatus)
          setResult(d)
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
    setError(null)
  }

  useEffect(() => () => { if (pollRef.current) clearInterval(pollRef.current) }, [])

  return { status, result, error, start, reset }
}
