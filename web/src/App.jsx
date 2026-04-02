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
const LOADTEST_FLOW = ['created', 'queued', 'initializing', 'running', 'processing_metrics', 'completed']
const LOADTEST_PHASE = Object.freeze({
  IDLE: 'idle',
  STARTING: 'starting',
  RUNNING: 'running',
  ANALYZING: 'analyzing',
  COMPLETED: 'completed',
  FAILED: 'failed',
})
const DEFAULT_POLL_MS = 3000
const BACKOFF_POLL_MS = [5000, 6500, 8000]
const P95_THRESHOLD_MS = 120
const ERROR_SPIKE_PCT = 2
const FLOW_LABEL = Object.freeze({
  created: 'Created',
  queued: 'Queued',
  initializing: 'Init',
  running: 'Running',
  processing_metrics: 'Analyze',
  completed: 'Done',
})

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

async function readErrorMessage(res) {
  const fallback = `REQUEST FAILED (${res.status})`
  const raw = (await res.text()).trim()
  if (!raw) return fallback
  try {
    const parsed = JSON.parse(raw)
    return String(parsed?.error || raw).trim()
  } catch {
    return raw
  }
}

function fmtMetric(value, digits = 1, suffix = '') {
  const n = Number(value)
  if (!Number.isFinite(n)) return '--'
  return `${n.toFixed(digits)}${suffix}`
}

function scoreClass(score) {
  if (score >= 80) return 'stress-score--high'
  if (score >= 50) return 'stress-score--medium'
  return 'stress-score--low'
}

function phaseClass(phase) {
  switch (phase) {
    case LOADTEST_PHASE.STARTING:
      return 'stress-phase--starting'
    case LOADTEST_PHASE.RUNNING:
      return 'stress-phase--running'
    case LOADTEST_PHASE.ANALYZING:
      return 'stress-phase--analyzing'
    case LOADTEST_PHASE.COMPLETED:
      return 'stress-phase--completed'
    case LOADTEST_PHASE.FAILED:
      return 'stress-phase--failed'
    default:
      return 'stress-phase--idle'
  }
}

function statusIndex(status) {
  const mapped = status === 'aborted' ? 'running' : status
  return LOADTEST_FLOW.indexOf(mapped)
}

function tupleSeriesToValues(series, limit = 32) {
  if (!Array.isArray(series)) return []
  const values = series
    .map(point => {
      if (!Array.isArray(point)) return NaN
      return Number(point[1])
    })
    .filter(Number.isFinite)
  if (values.length <= limit) return values
  return values.slice(values.length - limit)
}

function sparklinePath(values, width = 320, height = 64, pad = 6) {
  const nums = values.map(Number).filter(Number.isFinite)
  if (nums.length === 0) {
    const y = (height / 2).toFixed(1)
    return `M ${pad} ${y} L ${width - pad} ${y}`
  }
  if (nums.length === 1) {
    const y = (height / 2).toFixed(1)
    return `M ${pad} ${y} L ${width - pad} ${y}`
  }

  const min = Math.min(...nums)
  const max = Math.max(...nums)
  const span = max - min || 1
  return nums
    .map((value, index) => {
      const x = nums.length === 1
        ? width / 2
        : pad + (index * (width - pad * 2)) / (nums.length - 1)
      const normalized = (value - min) / span
      const y = height - pad - normalized * (height - pad * 2)
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`
    })
    .join(' ')
}

function pushInsight(bucket, seen, value) {
  const text = String(value ?? '').trim()
  if (!text) return
  const key = text.toLowerCase()
  if (seen.has(key)) return
  seen.add(key)
  bucket.push(text.endsWith('.') ? text : `${text}.`)
}

function buildFinalInsights({ signals, snapshotInsight, snapshot, warnings, runStatus }) {
  const lines = []
  const seen = new Set()

  if (Array.isArray(signals)) {
    for (const signal of signals) pushInsight(lines, seen, signal)
  }
  pushInsight(lines, seen, snapshotInsight)
  if (Array.isArray(warnings) && warnings[0]) {
    pushInsight(lines, seen, `signal warning: ${warnings[0]}`)
  }

  if (snapshot.rps > 0) {
    pushInsight(lines, seen, `final throughput reached ${fmtMetric(snapshot.rps, 1)} RPS`)
  } else {
    pushInsight(lines, seen, 'no steady throughput sample was captured before the run ended')
  }
  if (snapshot.p95_ms > 0) {
    pushInsight(lines, seen, `final p95 latency was ${fmtMetric(snapshot.p95_ms, 1, ' ms')}`)
  }
  if (snapshot.error_rate_pct >= 0) {
    pushInsight(lines, seen, `final error rate was ${fmtMetric(snapshot.error_rate_pct, 2, '%')}`)
  }
  if (snapshot.vus > 0) {
    pushInsight(lines, seen, `active virtual users reached ${fmtMetric(snapshot.vus, 0)}`)
  }
  if (runStatus === 'aborted') {
    pushInsight(lines, seen, 'run ended in aborted state')
  }

  while (lines.length < 3) {
    pushInsight(lines, seen, 'metrics are limited, rerun once traffic is ready for a fuller signal')
  }
  return lines.slice(0, 3)
}

function retryHint(error) {
  const msg = String(error ?? '').toLowerCase()
  if (msg.includes('origin not allowed')) {
    return 'Origin is blocked. If you use Vite dev server, add http://localhost:5173 to LOADTEST_ALLOWED_ORIGINS and restart producer.'
  }
  if (msg.includes('upstream')) {
    return 'Upstream load test provider failed. Wait 20 to 30 seconds, then press Start Load Test again.'
  }
  if (msg.includes('already running')) {
    return 'Another run is active now. Wait for it to finish, then retry.'
  }
  if (msg.includes('cooldown')) {
    return 'Cooldown is active. Retry after the cooldown window expires.'
  }
  if (msg.includes('unauthorized') || msg.includes('forbidden')) {
    return 'Admin Key is invalid. Update the key and retry.'
  }
  return 'Retry once. If it still fails, check producer logs and LOADTEST environment variables.'
}

function parseRetrySeconds(error) {
  const msg = String(error ?? '').toLowerCase()
  const matched = msg.match(/retry in ([0-9hms]+)/i)
  if (!matched) return 0
  const token = matched[1]
  const parts = [...token.matchAll(/(\d+)([hms])/g)]
  if (parts.length === 0) return 0

  let seconds = 0
  for (const [, value, unit] of parts) {
    const n = Number(value)
    if (!Number.isFinite(n) || n <= 0) continue
    if (unit === 'h') seconds += n * 3600
    if (unit === 'm') seconds += n * 60
    if (unit === 's') seconds += n
  }
  return seconds
}

function fmtCountdown(totalSeconds) {
  const safe = Math.max(0, Number(totalSeconds) || 0)
  const hours = Math.floor(safe / 3600)
  const mins = Math.floor((safe % 3600) / 60)
  const secs = safe % 60
  if (hours > 0) {
    return `${String(hours).padStart(2, '0')}:${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`
  }
  return `${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`
}

function phaseFromRunStatus(status) {
  switch (status) {
    case 'created':
    case 'queued':
    case 'initializing':
    case 'running':
      return LOADTEST_PHASE.RUNNING
    case 'processing_metrics':
      return LOADTEST_PHASE.ANALYZING
    case 'completed':
      return LOADTEST_PHASE.COMPLETED
    case 'aborted':
      return LOADTEST_PHASE.FAILED
    default:
      return LOADTEST_PHASE.RUNNING
  }
}

function isPollingPhase(phase) {
  return phase === LOADTEST_PHASE.RUNNING || phase === LOADTEST_PHASE.ANALYZING
}

function isTemporaryPollError(message) {
  const msg = String(message ?? '').toLowerCase()
  if (/request failed \((429|502|503|504)\)/i.test(msg)) return true
  if (msg.includes('upstream')) return true
  if (msg.includes('timeout')) return true
  if (msg.includes('temporarily')) return true
  if (msg.includes('service unavailable')) return true
  if (msg.includes('networkerror')) return true
  if (msg.includes('failed to fetch')) return true
  return false
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

// ── StressLabPanel ───────────────────────────────────────────
export function StressLabPanel() {
  const [adminKey, setAdminKey] = useState('')
  const [runId, setRunId] = useState(null)
  const [runStatus, setRunStatus] = useState('idle')
  const [phase, setPhase] = useState(LOADTEST_PHASE.IDLE)
  const [healthScore, setHealthScore] = useState(0)
  const [rpsSeries, setRpsSeries] = useState([])
  const [signals, setSignals] = useState([])
  const [warnings, setWarnings] = useState([])
  const [snapshotInsight, setSnapshotInsight] = useState('')
  const [snapshot, setSnapshot] = useState({
    rps: 0,
    p95_ms: 0,
    error_rate_pct: 0,
    vus: 0,
  })
  const [tempPollErrors, setTempPollErrors] = useState(0)
  const [cooldownSeconds, setCooldownSeconds] = useState(0)
  const [throughputShift, setThroughputShift] = useState(0)
  const [error, setError] = useState(null)
  const lastRpsRef = useRef(0)

  useEffect(() => {
    if (cooldownSeconds <= 0) return
    const t = setTimeout(() => {
      setCooldownSeconds(prev => Math.max(0, prev - 1))
    }, 1000)
    return () => clearTimeout(t)
  }, [cooldownSeconds])

  useEffect(() => {
    if (cooldownSeconds > 0) return
    if (!error) return
    const msg = error.toLowerCase()
    if (msg.includes('cooldown') || msg.includes('start throttled')) {
      setError(null)
    }
  }, [cooldownSeconds, error])

  async function syncRun(targetRunId) {
    if (!targetRunId) return
    try {
      const res = await fetch(`/ops/loadtest/${targetRunId}`)
      if (!res.ok) throw new Error(await readErrorMessage(res))
      const data = await res.json()

      const nextStatus = data?.run?.status || 'running'
      const nextSnapshot = {
        rps: Number(data?.snapshot?.rps ?? 0),
        p95_ms: Number(data?.snapshot?.p95_ms ?? 0),
        error_rate_pct: Number(data?.snapshot?.error_rate_pct ?? 0),
        vus: Number(data?.snapshot?.vus ?? 0),
      }
      const nextSeries = tupleSeriesToValues(data?.series?.rps)
      const nextPhase = phaseFromRunStatus(nextStatus)

      setRunStatus(nextStatus)
      setPhase(nextPhase)
      setHealthScore(Number(data?.health_score ?? 0))
      setSnapshot(nextSnapshot)
      setRpsSeries(nextSeries)
      setSignals(Array.isArray(data?.signals) ? data.signals.filter(Boolean) : [])
      setWarnings(Array.isArray(data?.warnings) ? data.warnings.filter(Boolean) : [])
      setSnapshotInsight(typeof data?.snapshot?.insight === 'string' ? data.snapshot.insight : '')

      const delta = Math.abs(nextSnapshot.rps - lastRpsRef.current)
      setThroughputShift(delta)
      lastRpsRef.current = nextSnapshot.rps

      setTempPollErrors(0)
      setCooldownSeconds(0)
      setError(null)
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      if (isTemporaryPollError(message)) {
        setTempPollErrors(prev => Math.min(prev + 1, BACKOFF_POLL_MS.length))
        setPhase(prev => (
          prev === LOADTEST_PHASE.STARTING && targetRunId
            ? LOADTEST_PHASE.RUNNING
            : prev
        ))
        setError(message)
        return
      }

      setTempPollErrors(0)
      setPhase(LOADTEST_PHASE.FAILED)
      setCooldownSeconds(parseRetrySeconds(message))
      setError(message)
    }
  }

  async function handleStartLoadtest() {
    if (!adminKey.trim()) {
      setError('admin key is required before starting a load test')
      return
    }

    setError(null)
    setPhase(LOADTEST_PHASE.STARTING)
    setTempPollErrors(0)
    setCooldownSeconds(0)
    try {
      const res = await fetch('/ops/loadtest/start', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(adminKey ? { 'X-Admin-Key': adminKey } : {}),
        },
        body: JSON.stringify({
          scenario: 'default',
          preset: 'quick',
          note: 'dashboard one-click run',
        }),
      })
      if (!res.ok) throw new Error(await readErrorMessage(res))
      const data = await res.json()

      const nextRunId = Number(data?.run_id)
      if (!Number.isFinite(nextRunId) || nextRunId <= 0) {
        throw new Error('loadtest start response missing run_id')
      }
      const initialStatus = data?.status || 'created'
      setRunId(nextRunId)
      setRunStatus(initialStatus)
      setPhase(phaseFromRunStatus(initialStatus))
      setRpsSeries([])
      setSignals([])
      setWarnings([])
      setSnapshotInsight('')
      setSnapshot({ rps: 0, p95_ms: 0, error_rate_pct: 0, vus: 0 })
      setThroughputShift(0)
      lastRpsRef.current = 0
      setTempPollErrors(0)
      setCooldownSeconds(0)
      await syncRun(nextRunId)
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      setPhase(LOADTEST_PHASE.FAILED)
      setCooldownSeconds(parseRetrySeconds(message))
      setError(message)
    }
  }

  const pollDelayMs = tempPollErrors > 0
    ? BACKOFF_POLL_MS[Math.min(tempPollErrors, BACKOFF_POLL_MS.length) - 1]
    : DEFAULT_POLL_MS

  useEffect(() => {
    if (!runId) return
    if (!isPollingPhase(phase)) return
    const t = setTimeout(() => {
      syncRun(runId)
    }, pollDelayMs)
    return () => clearTimeout(t)
  }, [runId, phase, pollDelayMs])

  const statusStep = statusIndex(runStatus)
  const score = Number.isFinite(healthScore) ? Math.max(0, Math.min(100, Math.round(healthScore))) : 0
  const activePhase = isPollingPhase(phase)
  const missingAdminKey = adminKey.trim().length === 0
  const startLocked = phase === LOADTEST_PHASE.STARTING || activePhase || cooldownSeconds > 0 || missingAdminKey
  const showFinalSummary = !!runId && (phase === LOADTEST_PHASE.COMPLETED || runStatus === 'aborted')
  const hasMetrics = (
    rpsSeries.length > 0 ||
    snapshot.rps > 0 ||
    snapshot.p95_ms > 0 ||
    snapshot.error_rate_pct > 0 ||
    snapshot.vus > 0
  )
  const warmingUp = activePhase && !hasMetrics
  const waveformValues = rpsSeries.length > 0 ? rpsSeries : (snapshot.rps > 0 ? [snapshot.rps] : [])
  const waveformPath = sparklinePath(waveformValues.length > 0 ? waveformValues : [0, 0])
  const beamStrength = Math.max(0, Math.min(1, snapshot.rps / 200 + throughputShift / 80))
  const beamDurationMs = Math.max(460, 1300 - Math.round(beamStrength * 700))
  const beamOpacity = activePhase && snapshot.rps > 0 ? Math.max(0.24, beamStrength) : 0.16
  const p95Hot = snapshot.p95_ms >= P95_THRESHOLD_MS
  const errorSpike = snapshot.error_rate_pct >= ERROR_SPIKE_PCT
  const finalInsights = showFinalSummary
    ? buildFinalInsights({ signals, snapshotInsight, snapshot, warnings, runStatus })
    : []
  const primaryWarning = warnings[0] ? `Signal warning: ${warnings[0]}` : null
  const temporaryPollingIssue = tempPollErrors > 0 && activePhase
  const backoffSeconds = (pollDelayMs / 1000).toFixed(1)
  const cooldownCopy = cooldownSeconds > 0 ? `Try again in ${fmtCountdown(cooldownSeconds)}` : ''
  const errorHint = cooldownCopy || (temporaryPollingIssue
    ? `Temporary sync issue. Auto retry in ${backoffSeconds}s.`
    : retryHint(error))

  let startBtnLabel = 'Start Load Test'
  if (missingAdminKey && !activePhase && cooldownSeconds <= 0) {
    startBtnLabel = 'Enter Admin Key'
  } else if (phase === LOADTEST_PHASE.STARTING || phase === LOADTEST_PHASE.RUNNING || phase === LOADTEST_PHASE.ANALYZING) {
    startBtnLabel = 'Load Test Running'
  } else if (phase === LOADTEST_PHASE.COMPLETED) {
    startBtnLabel = 'Run Completed'
  } else if (phase === LOADTEST_PHASE.FAILED && runStatus === 'aborted') {
    startBtnLabel = 'Run Failed'
  }

  return (
    <div className="panel stress-panel">
      <div className="stress-header">
        <span className="panel-title">
          <span className="slash">╱</span> Stress Lab
        </span>
        <span className={`stress-score ${scoreClass(score)}`}>{score}</span>
      </div>

      <div className="field-group">
        <label className="field-label">Admin Key</label>
        <input
          type="password"
          value={adminKey}
          onChange={e => setAdminKey(e.target.value)}
          placeholder="X-Admin-Key"
          autoComplete="off"
        />
        <p className="field-help stress-key-help">
          Required for start requests. Key is not stored after page refresh.
        </p>
      </div>

      <div className="stress-run-meta">
        <span>RUN: {runId ?? '--'}</span>
        <span>STATUS: {runStatus}</span>
      </div>
      <p className={`stress-phase ${phaseClass(phase)}`}>PHASE: {phase}</p>
      {missingAdminKey && !activePhase && cooldownSeconds <= 0 && !error && (
        <p className="stress-guidance">Paste the server-side admin key, then start the run.</p>
      )}
      {!missingAdminKey && phase === LOADTEST_PHASE.IDLE && !error && (
        <p className="stress-guidance">Ready. Press Start Load Test to launch a cloud run.</p>
      )}
      <p className="stress-flow-label">Run lifecycle</p>
      <div className="stress-flow">
        {LOADTEST_FLOW.map((step, i) => (
          <span
            key={step}
            className={`stress-step${i <= statusStep ? ' stress-step--active' : ''}`}
            title={step}
          >
            <span className="stress-step__name">{FLOW_LABEL[step] ?? step}</span>
          </span>
        ))}
      </div>

      <div className="stress-wave-wrap">
        <div className="stress-wave-meta">
          <span>RPS WAVEFORM</span>
          <span>{hasMetrics ? `${fmtMetric(snapshot.rps, 1)} LIVE` : 'WARMING UP'}</span>
        </div>
        <div className={`stress-wave${waveformValues.length > 1 ? ' stress-wave--active' : ''}`}>
          <svg viewBox="0 0 320 64" preserveAspectRatio="none" role="img" aria-label="RPS waveform">
            <path className="stress-wave-midline" d="M 0 32 L 320 32" />
            <path className="stress-wave-line" d={waveformPath} />
          </svg>
        </div>
      </div>

      <div
        className={`stress-beam${activePhase && snapshot.rps > 0 ? ' stress-beam--active' : ''}`}
        style={{
          '--beam-duration': `${beamDurationMs}ms`,
          '--beam-opacity': beamOpacity,
        }}
      >
        {[0, 1, 2, 3, 4].map(i => (
          <span
            key={i}
            className="stress-beam__line"
            style={{ '--beam-delay': `${i * 70}ms` }}
          />
        ))}
      </div>

      {warmingUp ? (
        <p className="stress-warmup">Warming up: waiting for first metrics sample.</p>
      ) : (
        <p className="stress-runtime-insight">{snapshotInsight || 'Collecting runtime signal.'}</p>
      )}

      <div className="stress-cards">
        <div className="stress-card">
          <span className="stress-card__label">RPS</span>
          <span className="stress-card__value">{fmtMetric(snapshot.rps, 1)}</span>
        </div>
        <div className={`stress-card${p95Hot ? ' stress-card--p95-hot' : ''}`}>
          <span className="stress-card__label">P95 (MS)</span>
          <span className="stress-card__value">{fmtMetric(snapshot.p95_ms, 1)}</span>
        </div>
        <div className={`stress-card${errorSpike ? ' stress-card--error-spike' : ''}`}>
          <span className="stress-card__label">ERROR %</span>
          <span className="stress-card__value">{fmtMetric(snapshot.error_rate_pct, 2, '%')}</span>
        </div>
        <div className="stress-card">
          <span className="stress-card__label">VUS</span>
          <span className="stress-card__value">{fmtMetric(snapshot.vus, 0)}</span>
        </div>
      </div>

      {showFinalSummary && (
        <div key={`${runId}-${runStatus}-${score}`} className="stress-summary">
          <div className="stress-summary__header">
            <span>FINAL SCORE</span>
            <strong>{score}</strong>
          </div>
          <ul className="stress-summary__list">
            {finalInsights.map((line, i) => (
              <li key={`${runId}-${i}`}>{line}</li>
            ))}
          </ul>
        </div>
      )}

      {error && <p className="form-error">// ERR: {error}</p>}
      {error && <p className="stress-hint">{errorHint}</p>}
      {!error && primaryWarning && <p className="stress-hint">{primaryWarning}</p>}

      <button
        type="button"
        className="publish-btn stress-start-btn"
        onClick={handleStartLoadtest}
        disabled={startLocked}
      >
        {startBtnLabel}
      </button>
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
        <div className="left-stack">
          <PublishPanel onPublished={handlePublished} />
          <StressLabPanel />
        </div>
        <NotificationsPanel notifications={notifications} initialising={initialising} />
      </main>
    </>
  )
}
