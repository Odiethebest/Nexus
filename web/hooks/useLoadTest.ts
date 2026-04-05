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

export function useLoadTest() {
  const [status, setStatus]   = useState<LoadTestStatus>('idle')
  const [result, setResult]   = useState<LoadTestResult | null>(null)
  const [error, setError]     = useState<string | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const start = async () => {
    try {
      setStatus('running')
      setError(null)
      await startLoadTest('demo')
      pollRef.current = setInterval(async () => {
        try {
          const d = await getLoadTestLatest()
          setResult(d)
          const s = d?.run?.status ?? d?.status
          if (s === 'finished' || s === 'completed' || s === 'error') {
            setStatus(s === 'error' ? 'error' : 'completed')
            clearInterval(pollRef.current!)
          }
        } catch {}
      }, 2000)
    } catch (e) {
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
