'use client'
import { useState, useEffect, useCallback } from 'react'
import { getNotifications } from '@/lib/api'
import type { Notification } from '@/types'

export function useNotifications() {
  const [notifications, setNotifications] = useState<Notification[]>([])
  const [loading, setLoading]             = useState(true)
  const [error, setError]                 = useState<string | null>(null)
  const [tick, setTick]                   = useState(0)

  const refresh = useCallback(() => setTick(t => t + 1), [])

  useEffect(() => {
    let cancelled = false

    const fetchData = async () => {
      try {
        const data = await getNotifications()
        if (!cancelled) {
          setNotifications(data)
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
    const id = setInterval(fetchData, 10000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [tick])

  return { notifications, loading, error, refresh }
}
