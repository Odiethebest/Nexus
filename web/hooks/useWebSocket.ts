'use client'
import { useState, useEffect, useRef } from 'react'
import { getNotifications } from '@/lib/api'
import type { Notification, Priority, WsEvent } from '@/types'

const MAX_EVENTS = 100
const BACKFILL_LIMIT = 50
const RECONNECT_DELAY_MS = 3000

/**
 * A persisted Notification row carries the full broker envelope in its
 * `payload` JSONB column, so backfilled rows need one level of unwrapping to
 * reach the same shape the socket delivers.
 */
function fromNotification(n: Notification): WsEvent {
  const envelope = n.payload as { priority?: Priority; payload?: Record<string, unknown> } | null
  return {
    message_id: n.message_id,
    type:       n.event_type,
    priority:   envelope?.priority ?? 'normal',
    channel:    n.channel,
    status:     n.status,
    payload:    envelope?.payload ?? (n.payload as Record<string, unknown>) ?? {},
    timestamp:  n.created_at,
  }
}

export function useWebSocket() {
  const [events, setEvents] = useState<WsEvent[]>([])
  const [connected, setConnected] = useState(false)
  /**
   * True until the history backfill settles. Consumers show skeletons on it
   * instead of tracking a "mounted" flag: that flag went true the moment the
   * component mounted, so the empty state flashed while the backfill was
   * still in flight.
   */
  const [loading, setLoading] = useState(true)
  const wsRef = useRef<WebSocket | null>(null)
  const closedByUs = useRef(false)

  useEffect(() => {
    const WS_URL = process.env.NEXT_PUBLIC_WS_URL ?? 'ws://localhost:8080/ws'
    closedByUs.current = false
    let cancelled = false
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null

    // Pre-populate from history so the feed isn't empty on mount.
    getNotifications()
      .then((data: unknown) => {
        if (cancelled) return
        const list = Array.isArray(data) ? (data as Notification[]) : []
        setEvents(list.slice(0, BACKFILL_LIMIT).map(fromNotification))
      })
      .catch(() => {/* ignore — live events still arrive over the socket */})
      .finally(() => { if (!cancelled) setLoading(false) })

    const connect = () => {
      const ws = new WebSocket(WS_URL)
      wsRef.current = ws

      ws.onopen = () => setConnected(true)
      ws.onclose = () => {
        setConnected(false)
        if (!closedByUs.current) {
          reconnectTimer = setTimeout(connect, RECONNECT_DELAY_MS)
        }
      }
      ws.onerror = () => ws.close()
      ws.onmessage = (e) => {
        try {
          // The server sends channel and status; nothing is inferred here.
          const event = JSON.parse(e.data) as WsEvent
          setEvents(prev => [event, ...prev].slice(0, MAX_EVENTS))
        } catch {/* ignore malformed frame */}
      }
    }

    connect()
    return () => {
      cancelled = true
      closedByUs.current = true
      if (reconnectTimer) clearTimeout(reconnectTimer)
      wsRef.current?.close()
    }
  }, [])

  const clear = () => setEvents([])
  return { events, connected, loading, clear }
}
