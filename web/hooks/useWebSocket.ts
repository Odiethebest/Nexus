'use client'
import { useState, useEffect, useRef } from 'react'
import type { WsEvent } from '@/types'

export function useWebSocket() {
  const [events, setEvents] = useState<WsEvent[]>([])
  const [connected, setConnected] = useState(false)
  const wsRef = useRef<WebSocket | null>(null)

  useEffect(() => {
    const WS_URL = process.env.NEXT_PUBLIC_WS_URL ?? 'ws://localhost:8080/ws'

    const connect = () => {
      const ws = new WebSocket(WS_URL)
      wsRef.current = ws

      ws.onopen = () => setConnected(true)
      ws.onclose = () => {
        setConnected(false)
        // auto-reconnect after 3s
        setTimeout(connect, 3000)
      }
      ws.onerror = () => ws.close()
      ws.onmessage = (e) => {
        try {
          const event: WsEvent = JSON.parse(e.data)
          setEvents(prev => [event, ...prev].slice(0, 100)) // keep latest 100
        } catch {}
      }
    }

    connect()
    return () => {
      wsRef.current?.close()
    }
  }, [])

  const clear = () => setEvents([])
  return { events, connected, clear }
}
