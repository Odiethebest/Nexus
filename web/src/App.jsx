import { useState, useEffect, useRef } from 'react'

// ── Helpers ───────────────────────────────────────────────────
const PRIORITY_COLOR = { high: '#ff2d6b', normal: '#f5e642', low: '#00d4ff' }
const BADGE_CLASS    = { high: 'badge-high', normal: 'badge-normal', low: 'badge-low' }
const CUSTOM_PRESET_ID = 'custom'
const EVENT_PRESETS = [
  {
    id: 'order_created',
    label: 'Order Created',
    type: 'order.created',
    priority: 'high',
    payload: {
      order_id: 'ord_1001',
      user_id: 'u123',
      amount: 99.5,
      currency: 'USD',
    },
    hint: 'Common purchase event with order amount and currency.',
  },
  {
    id: 'payment_failed',
    label: 'Payment Failed',
    type: 'payment.failed',
    priority: 'high',
    payload: {
      payment_id: 'pay_293',
      user_id: 'u123',
      reason: 'card_declined',
      retryable: true,
    },
    hint: 'High priority payment failure with a retry signal.',
  },
  {
    id: 'security_alert',
    label: 'Security Alert',
    type: 'security.alert',
    priority: 'high',
    payload: {
      user_id: 'u123',
      location: 'Los Angeles, US',
      ip: '203.0.113.8',
      action: 'new_device_login',
    },
    hint: 'Security warning template for suspicious activity.',
  },
  {
    id: 'welcome_user',
    label: 'Welcome User',
    type: 'user.welcome',
    priority: 'normal',
    payload: {
      user_id: 'u123',
      email: 'user@example.com',
      plan: 'starter',
    },
    hint: 'Normal priority onboarding message for new users.',
  },
  {
    id: 'digest_ready',
    label: 'Digest Ready',
    type: 'digest.ready',
    priority: 'low',
    payload: {
      user_id: 'u123',
      range: 'daily',
      unread: 4,
    },
    hint: 'Low priority informational digest notification.',
  },
]
const EVENT_TYPE_OPTIONS = Array.from(new Set(EVENT_PRESETS.map(item => item.type))).sort()

// Per-priority card styles (border-left width + bg + optional extra shadow)
const CARD_STYLE = {
  high: {
    borderLeftWidth: '6px',
    borderLeftColor: '#ff2d6b',
    background: 'rgba(255,45,107,0.05)',
    boxShadow: 'inset -2px 0 20px rgba(255,45,107,0.08)',
  },
  normal: {
    borderLeftWidth: '4px',
    borderLeftColor: '#f5e642',
    background: 'rgba(245,230,66,0.03)',
  },
  low: {
    borderLeftWidth: '2px',
    borderLeftColor: '#00d4ff',
    background: 'transparent',
  },
}

function sysTime(date, now) {
  const mins = Math.floor((now - new Date(date)) / 60000)
  return `SYS_TIME: ${mins < 1 ? 'NOW' : `${mins}M`}`
}

function fmtPayload(payload) {
  if (!payload) return ''
  if (typeof payload === 'string') return payload
  try { return JSON.stringify(payload) } catch { return String(payload) }
}

function payloadToText(payload) {
  return JSON.stringify(payload, null, 2)
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
  const defaultPreset = EVENT_PRESETS[0]
  const [presetId, setPresetId]      = useState(defaultPreset.id)
  const [type, setType]              = useState(defaultPreset.type)
  const [priority, setPriority]      = useState(defaultPreset.priority)
  const [payload, setPayload]        = useState(payloadToText(defaultPreset.payload))
  const [loading, setLoading]        = useState(false)
  const [transmitted, setTransmitted] = useState(false)
  const [error, setError]            = useState(null)
  const currentPreset = EVENT_PRESETS.find(item => item.id === presetId)

  function markCustomPreset() {
    if (presetId !== CUSTOM_PRESET_ID) setPresetId(CUSTOM_PRESET_ID)
  }

  function applyPreset(nextPresetId) {
    const preset = EVENT_PRESETS.find(item => item.id === nextPresetId)
    if (!preset) {
      setPresetId(CUSTOM_PRESET_ID)
      return
    }
    setPresetId(preset.id)
    setType(preset.type)
    setPriority(preset.priority)
    setPayload(payloadToText(preset.payload))
    setError(null)
  }

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

      // Flash "transmitted" feedback for 300ms
      setTransmitted(true)
      setTimeout(() => setTransmitted(false), 300)

      onPublished({ message_id: data.message_id, type, priority, payload: parsed, timestamp: new Date() })
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  let btnLabel = '[ PUBLISH ]'
  if (loading)      btnLabel = '[ TRANSMITTING... ]'
  if (transmitted)  btnLabel = '[ TRANSMITTED ]'

  return (
    <div className="panel publish-panel">
      <form onSubmit={handleSubmit} style={{ display: 'contents' }}>
        <div className="publish-fields">
          <span className="panel-title">
            <span className="slash">╱</span> Publish Event
          </span>

          <div className="field-group">
            <label className="field-label">Preset</label>
            <select value={presetId} onChange={e => applyPreset(e.target.value)}>
              <option value={CUSTOM_PRESET_ID}>Custom</option>
              {EVENT_PRESETS.map(preset => (
                <option key={preset.id} value={preset.id}>{preset.label}</option>
              ))}
            </select>
            <p className="preset-hint">
              {currentPreset
                ? currentPreset.hint
                : 'Pick a preset to auto-fill the form, then tweak what you need.'}
            </p>
            <div className="preset-list">
              {EVENT_PRESETS.map(preset => (
                <button
                  key={preset.id}
                  type="button"
                  className={`preset-chip${preset.id === presetId ? ' preset-chip--active' : ''}`}
                  onClick={() => applyPreset(preset.id)}
                >
                  {preset.label}
                </button>
              ))}
            </div>
          </div>

          <div className="field-group">
            <label className="field-label">Type</label>
            <input
              type="text"
              list="event-type-options"
              value={type}
              onChange={e => {
                setType(e.target.value)
                markCustomPreset()
              }}
              placeholder="order / payment / alert"
              required
            />
            <datalist id="event-type-options">
              {EVENT_TYPE_OPTIONS.map(option => (
                <option key={option} value={option} />
              ))}
            </datalist>
            <p className="field-help">Use one of the common types or enter your own.</p>
          </div>

          <div className="field-group">
            <label className="field-label">
              Priority
              <PriorityBadge priority={priority} />
            </label>
            <select
              value={priority}
              onChange={e => {
                setPriority(e.target.value)
                markCustomPreset()
              }}
            >
              <option value="high">High</option>
              <option value="normal">Normal</option>
              <option value="low">Low</option>
            </select>
          </div>

          <div className="field-group">
            <label className="field-label">Payload (JSON)</label>
            <textarea
              value={payload}
              onChange={e => {
                setPayload(e.target.value)
                markCustomPreset()
              }}
              rows={4}
            />
          </div>

          {error && <p className="form-error">// ERR: {error}</p>}
        </div>

        <button
          className={`publish-btn${transmitted ? ' publish-btn--transmitted' : ''}`}
          type="submit"
          disabled={loading}
        >
          {btnLabel}
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
      <span className="empty-sub">
        No events detected. Publish one using the form to initiate transmission.
      </span>
    </div>
  )
}

// ── SkeletonFeed ──────────────────────────────────────────────
function SkeletonFeed() {
  return (
    <div className="skeleton-feed">
      {[85, 70, 78].map(w => (
        <div key={w} className="skeleton" style={{ height: 72, width: `${w}%` }} />
      ))}
    </div>
  )
}

const PRIORITY_ORDER = { high: 0, normal: 1, low: 2 }

// ── NotificationsPanel ────────────────────────────────────────
function NotificationsPanel({ notifications, initialising }) {
  const [now, setNow] = useState(Date.now())

  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 30000)
    return () => clearInterval(t)
  }, [])

  const sorted = [...notifications].sort((a, b) => {
    const pa = PRIORITY_ORDER[a.priority] ?? 1
    const pb = PRIORITY_ORDER[b.priority] ?? 1
    if (pa !== pb) return pa - pb
    return new Date(b.timestamp) - new Date(a.timestamp)
  })

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
            {sorted.map((n, i) => {
              const cardStyle = CARD_STYLE[n.priority] ?? CARD_STYLE.normal
              return (
                <div
                  className="notif-card"
                  key={`${n.message_id}-${i}`}
                  style={cardStyle}
                >
                  <div className="notif-card__header">
                    <span className="notif-card__type">{n.type}</span>
                    <div className="notif-card__meta">
                      <PriorityBadge priority={n.priority} />
                      <span className="notif-card__time">{sysTime(n.timestamp, now)}</span>
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
      const protocol = location.protocol === 'https:' ? 'wss' : 'ws'
      let ws
      try {
        ws = new WebSocket(`${protocol}://${location.host}/ws`)
      } catch {
        setWsStatus('disconnected')
        setTimeout(connect, 3000)
        return
      }
      wsRef.current = ws
      ws.onopen    = () => setWsStatus('connected')
      ws.onclose   = () => { setWsStatus('disconnected'); setTimeout(connect, 3000) }
      ws.onerror   = () => setWsStatus('disconnected')
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
