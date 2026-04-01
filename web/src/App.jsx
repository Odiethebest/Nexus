import { useState, useEffect, useRef } from 'react'

const CHANNEL_COLOR = {
  email:    '#3b82f6',
  inapp:    '#8b5cf6',
  webhook:  '#10b981',
  producer: '#f59e0b',
}

// ── EventForm ────────────────────────────────────────────────
function EventForm({ onPublished }) {
  const [form, setForm] = useState({
    type: 'order',
    priority: 'high',
    payload: '{"user_id":"u123","email":"user@example.com"}',
  })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  async function handleSubmit(e) {
    e.preventDefault()
    setLoading(true)
    setError(null)
    try {
      let payload
      try { payload = JSON.parse(form.payload) }
      catch { throw new Error('Payload must be valid JSON') }

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

  const set = key => e => setForm(f => ({ ...f, [key]: e.target.value }))

  return (
    <form className="form" onSubmit={handleSubmit}>
      <h2>Publish Event</h2>

      <label>
        Type
        <input value={form.type} onChange={set('type')} required />
      </label>

      <label>
        Priority
        <select value={form.priority} onChange={set('priority')}>
          <option value="high">high</option>
          <option value="normal">normal</option>
          <option value="low">low</option>
        </select>
      </label>

      <label>
        Payload (JSON)
        <textarea rows={4} value={form.payload} onChange={set('payload')} />
      </label>

      {error && <p className="error">{error}</p>}

      <button className="btn btn-primary" type="submit" disabled={loading}>
        {loading ? 'Publishing…' : 'Publish'}
      </button>
    </form>
  )
}

// ── Skeleton rows shown while feed is empty on first load ────
function SkeletonFeed() {
  return (
    <div className="feed">
      {[80, 65, 72].map(w => (
        <div key={w} className="skeleton" style={{ height: 36, width: `${w}%` }} />
      ))}
    </div>
  )
}

// ── Empty state after skeleton has been shown ────────────────
function EmptyState() {
  return (
    <div className="empty-state">
      <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
        <path strokeLinecap="round" strokeLinejoin="round"
          d="M14.857 17.082a23.848 23.848 0 0 0 5.454-1.31A8.967 8.967 0 0 1 18 9.75V9A6 6 0 0 0 6 9v.75a8.967 8.967 0 0 1-2.312 6.022c1.733.64 3.56 1.085 5.455 1.31m5.714 0a24.255 24.255 0 0 1-5.714 0m5.714 0a3 3 0 1 1-5.714 0" />
      </svg>
      <span>Waiting for events…</span>
      <span style={{ fontSize: '0.75rem' }}>Publish one using the form on the left</span>
    </div>
  )
}

// ── NotificationFeed ─────────────────────────────────────────
function NotificationFeed({ notifications, initialising }) {
  if (initialising) return <SkeletonFeed />
  if (notifications.length === 0) return <EmptyState />

  return (
    <ul className="feed" style={{ listStyle: 'none' }}>
      {notifications.map((n, i) => {
        const color = CHANNEL_COLOR[n.channel] ?? '#888'
        const ok    = n.status === 'delivered' || n.status === 'queued' || n.status === 'received'
        return (
          <li
            key={`${n.message_id}-${n.channel}-${i}`}
            className="feed-item"
            style={{ '--channel-color': color }}
          >
            <span className="feed-item__channel">[{n.channel}]</span>
            <span className="feed-item__type">{n.event_type ?? n.type ?? '—'}</span>
            <span className="feed-item__id">{n.message_id?.slice(0, 8)}…</span>
            <span className={`feed-item__status ${ok ? 'feed-item__status--ok' : 'feed-item__status--warn'}`}>
              {n.status ?? 'received'}
            </span>
          </li>
        )
      })}
    </ul>
  )
}

// ── App ──────────────────────────────────────────────────────
export default function App() {
  const [notifications, setNotifications] = useState([])
  const [wsStatus, setWsStatus]           = useState('connecting')
  const [initialising, setInitialising]   = useState(true)
  const wsRef = useRef(null)

  useEffect(() => {
    // Show skeleton for 600 ms then reveal empty state if still no events
    const t = setTimeout(() => setInitialising(false), 600)
    return () => clearTimeout(t)
  }, [])

  useEffect(() => {
    function connect() {
      const ws = new WebSocket(`ws://${location.host}/ws`)
      wsRef.current = ws

      ws.onopen  = () => setWsStatus('connected')
      ws.onclose = () => { setWsStatus('disconnected'); setTimeout(connect, 3000) }
      ws.onerror = () => setWsStatus('error')
      ws.onmessage = e => {
        try {
          const event = JSON.parse(e.data)
          setInitialising(false)
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
    setInitialising(false)
    setNotifications(prev => [
      { message_id: messageId, channel: 'producer', event_type: 'published', status: 'queued' },
      ...prev.slice(0, 99),
    ])
  }

  const wsBadge = wsStatus === 'connected' ? 'ws-badge--ok' : 'ws-badge--warn'

  return (
    <div className="app">
      <header className="app-header">
        <h1>Nexus Dashboard</h1>
        <p className="subtitle">
          WebSocket: <span className={`ws-badge ${wsBadge}`}>{wsStatus}</span>
        </p>
      </header>

      <div className="app-grid">
        <div className="card">
          <EventForm onPublished={handlePublished} />
        </div>
        <div className="card">
          <h2>Live Notifications</h2>
          <NotificationFeed notifications={notifications} initialising={initialising} />
        </div>
      </div>
    </div>
  )
}
