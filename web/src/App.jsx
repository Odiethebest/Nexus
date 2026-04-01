import { useState, useEffect, useRef } from 'react'

const CHANNEL_COLORS = {
  email: '#3b82f6',
  inapp: '#8b5cf6',
  webhook: '#10b981',
}

function EventForm({ onPublished }) {
  const [form, setForm] = useState({ type: 'order', priority: 'high', payload: '{"user_id":"u123"}' })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  async function handleSubmit(e) {
    e.preventDefault()
    setLoading(true)
    setError(null)
    try {
      let payload
      try {
        payload = JSON.parse(form.payload)
      } catch {
        throw new Error('Payload must be valid JSON')
      }
      const res = await fetch('/events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: form.type, priority: form.priority, payload }),
      })
      if (!res.ok) throw new Error(await res.text())
      const data = await res.json()
      onPublished(data.message_id)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      <h2 style={{ margin: 0 }}>Publish Event</h2>
      <label>
        Type
        <input value={form.type} onChange={e => setForm(f => ({ ...f, type: e.target.value }))} required />
      </label>
      <label>
        Priority
        <select value={form.priority} onChange={e => setForm(f => ({ ...f, priority: e.target.value }))}>
          <option value="high">high</option>
          <option value="normal">normal</option>
          <option value="low">low</option>
        </select>
      </label>
      <label>
        Payload (JSON)
        <textarea
          rows={3}
          value={form.payload}
          onChange={e => setForm(f => ({ ...f, payload: e.target.value }))}
        />
      </label>
      {error && <p style={{ color: 'red', margin: 0 }}>{error}</p>}
      <button type="submit" disabled={loading}>{loading ? 'Publishing…' : 'Publish'}</button>
    </form>
  )
}

function NotificationFeed({ notifications }) {
  return (
    <div>
      <h2 style={{ margin: '0 0 8px' }}>Live Notifications</h2>
      {notifications.length === 0 && <p style={{ color: '#888' }}>Waiting for events…</p>}
      <ul style={{ listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: 6 }}>
        {notifications.map((n, i) => (
          <li key={`${n.message_id}-${n.channel}-${i}`} style={{
            padding: '8px 12px',
            borderRadius: 6,
            background: '#1e1e2e',
            borderLeft: `4px solid ${CHANNEL_COLORS[n.channel] ?? '#888'}`,
            fontFamily: 'monospace',
            fontSize: 13,
          }}>
            <span style={{ color: CHANNEL_COLORS[n.channel] ?? '#888' }}>[{n.channel}]</span>{' '}
            <strong>{n.event_type ?? n.type}</strong>{' '}
            <span style={{ color: '#aaa' }}>{n.message_id?.slice(0, 8)}…</span>{' '}
            <span style={{ color: n.status === 'delivered' ? '#10b981' : '#f59e0b', float: 'right' }}>
              {n.status ?? 'received'}
            </span>
          </li>
        ))}
      </ul>
    </div>
  )
}

export default function App() {
  const [notifications, setNotifications] = useState([])
  const [wsStatus, setWsStatus] = useState('connecting')
  const wsRef = useRef(null)

  useEffect(() => {
    function connect() {
      const ws = new WebSocket(`ws://${location.host}/ws`)
      wsRef.current = ws

      ws.onopen = () => setWsStatus('connected')
      ws.onclose = () => {
        setWsStatus('disconnected')
        setTimeout(connect, 3000) // reconnect
      }
      ws.onerror = () => setWsStatus('error')
      ws.onmessage = e => {
        try {
          const event = JSON.parse(e.data)
          setNotifications(prev => [
            { ...event, channel: 'inapp', status: 'received' },
            ...prev.slice(0, 99),
          ])
        } catch {}
      }
    }

    connect()
    return () => wsRef.current?.close()
  }, [])

  function handlePublished(messageId) {
    setNotifications(prev => [
      { message_id: messageId, channel: 'producer', event_type: 'published', status: 'queued' },
      ...prev.slice(0, 99),
    ])
  }

  return (
    <div style={{
      minHeight: '100vh',
      background: '#13131f',
      color: '#e2e8f0',
      fontFamily: 'system-ui, sans-serif',
      padding: 24,
    }}>
      <header style={{ marginBottom: 24 }}>
        <h1 style={{ margin: 0, fontSize: 24 }}>Nexus Dashboard</h1>
        <p style={{ margin: '4px 0 0', color: '#888', fontSize: 13 }}>
          WebSocket: <span style={{ color: wsStatus === 'connected' ? '#10b981' : '#f59e0b' }}>{wsStatus}</span>
        </p>
      </header>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: 24, alignItems: 'start' }}>
        <div style={{ background: '#1e1e2e', borderRadius: 10, padding: 20 }}>
          <EventForm onPublished={handlePublished} />
        </div>
        <div style={{ background: '#1e1e2e', borderRadius: 10, padding: 20 }}>
          <NotificationFeed notifications={notifications} />
        </div>
      </div>
    </div>
  )
}
