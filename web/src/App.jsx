import { useState, useEffect, useRef } from 'react'

// ── Helpers ───────────────────────────────────────────────────
const PRIORITY_COLOR = { high: '#ff2d6b', normal: '#f5e642', low: '#00d4ff' }
const BADGE_CLASS    = { high: 'badge-high', normal: 'badge-normal', low: 'badge-low' }

function sysTime(date) {
  const mins = Math.floor((Date.now() - new Date(date)) / 60000)
  return `SYS_TIME: ${mins < 1 ? 'NOW' : `${mins}M`}`
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
  const [type, setPriority_type] = useState('order')
  const [priority, setPriority]  = useState('high')
  const [payload, setPayload]    = useState('{"user_id":"u123","email":"user@example.com"}')
  const [loading, setLoading]    = useState(false)
  const [error, setError]        = useState(null)

  async function handleSubmit(e) {
    e.preventDefault()
    setError(null)
    setLoading(true)
    try {
      let parsed
      try { parsed = JSON.parse(payload) }
      catch { throw new Error('PAYLOAD IS NOT VALID JSON') }

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
      <span className="panel-title">
        <span className="slash">╱</span> Publish Event
      </span>

      <form onSubmit={handleSubmit} style={{ display: 'contents' }}>
        <div className="field-group">
          <label className="field-label">Type</label>
          <input
            type="text"
            value={type}
            onChange={e => setPriority_type(e.target.value)}
            placeholder="order / payment / alert"
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

        {error && <p className="form-error">// ERR: {error}</p>}

        <button className="publish-btn" type="submit" disabled={loading}>
          {loading ? '[ TRANSMITTING... ]' : '[ PUBLISH ]'}
        </button>
      </form>
    </div>
  )
}

// ── EmptyState ────────────────────────────────────────────────
function EmptyState() {
  return (
    <div className="empty-state">
      <div className="empty-icon">▓▓▓</div>
      <span className="empty-title">Awaiting Signal</span>
      <span className="empty-sub">No events detected. Publish one using the form to initiate transmission.</span>
    </div>
  )
}

// ── SkeletonFeed ──────────────────────────────────────────────
function SkeletonFeed() {
  return (
    <div className="skeleton-feed">
      {[85, 70, 78].map(w => (
        <div key={w} className="skeleton" style={{ height: 68, width: `${w}%` }} />
      ))}
    </div>
  )
}

// ── NotificationsPanel ────────────────────────────────────────
function NotificationsPanel({ notifications, initialising }) {
  const count = notifications.length

  return (
    <div className="panel notif-panel">
      <div className="notif-header">
        <span className="panel-title">
          <span className="slash">╱</span> Live Notifications
        </span>
        <span className="notif-count">{count} EVT</span>
      </div>

      <div className="notif-body">
        {initialising ? (
          <SkeletonFeed />
        ) : count === 0 ? (
          <EmptyState />
        ) : (
          <div className="notif-list">
            {notifications.map((n, i) => {
              const color = PRIORITY_COLOR[n.priority] ?? '#f5e642'
              return (
                <div
                  className="notif-card"
                  key={`${n.message_id}-${i}`}
                  style={{ '--priority-color': color }}
                >
                  <div className="notif-card__header">
                    <span className="notif-card__type">{n.type}</span>
                    <div className="notif-card__meta">
                      <PriorityBadge priority={n.priority} />
                      <span className="notif-card__time">{sysTime(n.timestamp)}</span>
                    </div>
                  </div>
                  <span className="notif-card__payload">{fmtPayload(n.payload)}</span>
                </div>
              )
            })}
          </div>
        )}
      </div>
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
      <header className="topbar">
        <span className="topbar-title">
          <span className="bar-glyph">▋</span>Nexus Dashboard
        </span>
        <div className="status-row">
          <span className={`status-square ${connected ? 'status-square--connected' : 'status-square--disconnected'}`}>
            ■
          </span>
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
