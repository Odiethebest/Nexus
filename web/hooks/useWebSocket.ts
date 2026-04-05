'use client'
import { useState, useEffect, useRef } from 'react'
import { getNotifications } from '@/lib/api'
import type { WsEvent } from '@/types'

export function useWebSocket() {
  const [events, setEvents] = useState<WsEvent[]>([])
  const [connected, setConnected] = useState(false)
  const wsRef = useRef<WebSocket | null>(null)

  useEffect(() => {
    const WS_URL = process.env.NEXT_PUBLIC_WS_URL ?? 'ws://localhost:8080/ws'

    // Pre-populate with recent notifications so the feed isn't empty on mount
    getNotifications()
      .then(notifications => {
        const initial: WsEvent[] = notifications
          .slice(0, 50)
          // n.payload is the full broker.Event JSON stored by the worker:
          //   { message_id, type, priority, payload: { ...inner... }, timestamp }
          // So we unwrap one level to get priority and the real inner payload.
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          .map((n: { message_id: string; event_type: string; channel: string; payload: any; created_at: string }) => ({
            message_id: n.message_id,
            type:       n.event_type,
            priority:   n.payload?.priority ?? 'normal',
            channel:    n.channel,
            payload:    n.payload?.payload  ?? n.payload ?? {},
            timestamp:  n.created_at,
          }))
        setEvents(initial)
      })
      .catch(() => {/* ignore — live events will still arrive via WS */})

    const connect = () => {
      const ws = new WebSocket(WS_URL)
      wsRef.current = ws

      ws.onopen = () => setConnected(true)
      ws.onclose = () => {
        setConnected(false)
        setTimeout(connect, 3000)
      }
      ws.onerror = () => ws.close()
      ws.onmessage = (e) => {
        try {
          const event: WsEvent = { ...JSON.parse(e.data), channel: 'inapp' as const }
          console.log('[ws] received:', event.type, event.priority, event.channel)
          setEvents(prev => [event, ...prev].slice(0, 100))
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
