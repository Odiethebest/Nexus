import { useState, useEffect, useRef } from 'react'

// ── Helpers ───────────────────────────────────────────────────
const BADGE_CLASS = { high: 'badge-high', normal: 'badge-normal', low: 'badge-low' }

function relativeTime(date) {
  const mins = Math.floor((Date.now() - new Date(date)) / 60000)
  return mins < 1 ? 'just now' : `${mins}m ago`
}

function fmtPayload(payload) {
  if (!payload) return ''
  if (typeof payload === 'string') return payload
  try { return JSON.stringify(payload) } catch { return String(payload) }
}

// ── PriorityBadge ─────────────────────────────────────────────
function PriorityBadge({ priority }) {
  return (
    <span className={`priority-badge ${BADGE_CLASS[priority] ?? 'badge-normal'}`}>
      {priority}
    </span>
  )
}

// ── PublishPanel ──────────────────────────────────────────────
function PublishPanel({ onPublished }) {
  const [type, setType]       = useState('order')
  const [priority, setPriority] = useState('high')
  const [payload, setPayload] = useState('{"user_id":"u123","email":"user@example.com"}')
  const [loading, setLoading] = useState(false)
  const [error, setError]     = useState(null)

  async function handleSubmit(e) {
    e.preventDefault()
    setError(null)
    setLoading(true)
    try {
      let parsed
      try { parsed = JSON.parse(payload) }
      catch { throw new Error('Payload must be valid JSON') }

      const res = await fetch('/events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type, priority, payload: parsed }),
      })
      if (!res.ok) throw new Error(await res.text())
      const data = await res.json()
      onPublished({ message_id: data.message_id, type, priority, payload: parsed, timestamp: new Date() })
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="panel">
      <span className="panel-title">Publish Event</span>

      <form onSubmit={handleSubmit} style={{ display: 'contents' }}>
        <div className="field-group">
          <label className="field-label">Type</label>
          <input
            type="text"
            value={type}
            onChange={e => setType(e.target.value)}
            placeholder="e.g. order, payment, alert"
            required
          />
        </div>

        <div className="field-group">
          <label className="field-label">
            Priority
            <PriorityBadge priority={priority} />
          </label>
          <select value={priority} onChange={e => setPriority(e.target.value)}>
            <option value="high">High</option>
            <option value="normal">Normal</option>
            <option value="low">Low</option>
          </select>
        </div>

        <div className="field-group">
          <label className="field-label">Payload (JSON)</label>
          <textarea
            value={payload}
            onChange={e => setPayload(e.target.value)}
            rows={4}
          />
        </div>

        {error && <p className="form-error">{error}</p>}

        <button className="publish-btn" type="submit" disabled={loading}>
          {loading ? 'Publishing…' : 'Publish'}
        </button>
      </form>
    </div>
  )
}

// ── EmptyState ────────────────────────────────────────────────
function EmptyState() {
  return (
    <div className="empty-state">
      <div className="empty-icon">
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
          <path
            d="M8 1.5C5 1.5 3 3.5 3 6.5V10l-1.5 1.5v.5h13V11.5L13 10V6.5C13 3.5 11 1.5 8 1.5Z"
            stroke="#b4b2a9" strokeWidth="1.2" strokeLinejoin="round"
          />
          <path
            d="M6.5 12.5a1.5 1.5 0 003 0"
            stroke="#b4b2a9" strokeWidth="1.2" strokeLinecap="round"
          />
        </svg>
      </div>
      <span className="empty-title">Waiting for events</span>
      <span className="empty-sub">Publish one using the form on the left</span>
    </div>
  )
}

// ── SkeletonFeed ──────────────────────────────────────────────
function SkeletonFeed() {
  return (
    <div className="skeleton-feed">
      {[80, 65, 72].map(w => (
        <div key={w} className="skeleton" style={{ height: 64, width: `${w}%` }} />
      ))}
    </div>
  )
}

// ── NotificationsPanel ────────────────────────────────────────
function NotificationsPanel({ notifications, initialising }) {
  const count = notifications.length

  return (
    <div className="panel" style={{ gap: 0 }}>
      <div className="notif-header" style={{ marginBottom: 16 }}>
        <span className="panel-title">Live Notifications</span>
        <span className="notif-count">
          {count} {count === 1 ? 'event' : 'events'}
        </span>
      </div>

      {initialising ? (
        <SkeletonFeed />
      ) : count === 0 ? (
        <EmptyState />
      ) : (
        <div className="notif-list">
          {notifications.map((n, i) => (
            <div className="notif-card" key={`${n.message_id}-${i}`}>
              <div className="notif-card__header">
                <span className="notif-card__type">{n.type}</span>
                <div className="notif-card__meta">
                  <PriorityBadge priority={n.priority} />
                  <span className="notif-card__time">{relativeTime(n.timestamp)}</span>
                </div>
              </div>
              <span className="notif-card__payload">{fmtPayload(n.payload)}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ── App ───────────────────────────────────────────────────────
export default function App() {
  const [notifications, setNotifications] = useState([])
  const [wsStatus, setWsStatus]           = useState('disconnected')
  const [initialising, setInitialising]   = useState(true)
  const wsRef = useRef(null)

  useEffect(() => {
    const t = setTimeout(() => setInitialising(false), 600)
    return () => clearTimeout(t)
  }, [])

  useEffect(() => {
    function connect() {
      const ws = new WebSocket(`ws://${location.host}/ws`)
      wsRef.current = ws

      ws.onopen = () => setWsStatus('connected')
      ws.onclose = () => {
        setWsStatus('disconnected')
        setTimeout(connect, 3000)
      }
      ws.onerror = () => setWsStatus('disconnected')
      ws.onmessage = e => {
        try {
          const ev = JSON.parse(e.data)
          setInitialising(false)
          setNotifications(prev => [
            { ...ev, timestamp: ev.timestamp ? new Date(ev.timestamp) : new Date() },
            ...prev.slice(0, 99),
          ])
        } catch {}
      }
    }
    connect()
    return () => wsRef.current?.close()
  }, [])

  function handlePublished(event) {
    setInitialising(false)
    setNotifications(prev => [event, ...prev.slice(0, 99)])
  }

  const connected = wsStatus === 'connected'

  return (
    <>
      <header className="page-header">
        <span className="page-title">Nexus Dashboard</span>
        <div className="status-row">
          <div className={`status-dot${connected ? ' status-dot--connected' : ''}`} />
          <span className="status-text">{wsStatus}</span>
        </div>
      </header>

      <main className="dash">
        <PublishPanel onPublished={handlePublished} />
        <NotificationsPanel notifications={notifications} initialising={initialising} />
      </main>
    </>
  )
}
