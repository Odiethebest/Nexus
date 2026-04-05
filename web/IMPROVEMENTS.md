# Nexus Frontend — Improvement Roadmap

> Read CLAUDE.md before starting any task. All API contracts, type definitions, and component rules are specified there.
> Complete tasks in order. Run `npm run build` after each major section and fix TypeScript errors before moving on.

---

## Section 1 — Dashboard Fixes

### 1.1 Chart line colors
`components/chart-area-interactive.tsx` currently renders all three area series in the same gray color. Fix:
- email series: `#4ade80` (green)
- inapp series: `#60a5fa` (blue)
- webhook series: `#f472b6` (pink)
- Each series should have a matching fill with 10% opacity
- Add a legend above the chart showing the three colored lines with labels

### 1.2 Metric card number precision
Currently showing `0.0 msg/s` and `0.0 ms`. Fix formatting:
- `publish_rate_per_sec`: round to 1 decimal, e.g. `14.2`
- `processing_latency_p99_ms`: round to 1 decimal, e.g. `38.1`
- `dlq_count`: integer, no decimal
- `active_ws_connections`: integer, no decimal

### 1.3 Empty chart state
When `history` has fewer than 2 data points, the chart renders a flat line with no labels.
Show a centered overlay text: `"Waiting for data — metrics update every 5s"` in muted color.
Do not hide the chart axes — keep the structure visible.

### 1.4 Notifications table — show all channels per event
Currently each row is one `(message_id, channel)` pair, so the same event appears 3 times with different channels.
This is correct behavior — do not change the data model.
Instead, add a subtle visual grouping: rows with the same `message_id` should share a left border accent (`border-l-2 border-muted`) to make the grouping obvious.

---

## Section 2 — Publish Page (`/publish`)

File: `app/publish/page.tsx`
Build a fully functional event publisher form. No placeholder — complete implementation.

### Layout
Use the same `SidebarProvider + AppSidebar + SiteHeader` layout as dashboard.
Page title: "Publish Event"
Subtitle: "Send a test event into the Nexus pipeline"

### Form fields
Use shadcn `Card` to wrap the form. All inputs use shadcn components.

**Event Type** — `Select` dropdown with these options:
```
payment.completed
payment.failed  
order.shipped
order.cancelled
user.signup
user.deleted
alert.critical
alert.warning
```
Default: `payment.completed`

**Priority** — three shadcn `Button` toggles in a row (not a dropdown):
`high` | `normal` | `low`
Active button uses `default` variant, inactive use `outline`.
Default: `normal`

**Payload** — `Textarea` (6 rows) with monospace font (`font-mono text-sm`).
Pre-fill with valid JSON matching the selected event type:
- `payment.completed` → `{ "amount": 99.99, "currency": "USD", "customer_id": "cust_123" }`
- `order.shipped` → `{ "order_id": "ORD-456", "tracking": "1Z999AA10123456784" }`
- `user.signup` → `{ "email": "user@example.com", "plan": "free" }`
- others → `{}`

When event type changes, auto-update the payload textarea with the matching template.

**Submit button** — full width, label "Publish Event", uses `postEvent()` from `lib/api.ts`.

### After submit
- Show a success toast (sonner): `"Event published — message_id: {id}"`
- Add a "View in Live Feed →" link inside the toast that navigates to `/live`
- If the fetch fails, show an error toast with the error message
- Reset the form to defaults after success

### Validation
- Validate payload is valid JSON before submitting. If invalid, show inline error under textarea: `"Invalid JSON"` in red.
- Disable submit button while request is in flight, show spinner inside button.

---

## Section 3 — Live Feed Page (`/live`)

File: `app/live/page.tsx`
Real-time WebSocket event stream. This is the visual showpiece of the project.

### WebSocket hook
Create `hooks/useWebSocket.ts`:
```typescript
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
```

### Page layout
Same sidebar layout. Title: "Live Feed"

**Connection status bar** — full width bar below the header:
- Connected: green dot + "Connected · receiving events"
- Disconnected: yellow dot + "Reconnecting..."

**Filter controls** (above the event list):
- Channel filter: `All` | `email` | `inapp` | `webhook` — toggle buttons
- Priority filter: `All` | `high` | `normal` | `low` — toggle buttons
- "Clear" button (right-aligned): calls `clear()`
- Event count badge: `"{n} events"`

Filters are client-side only — filter the `events` array before rendering.

**Event feed** — scrollable list, new events enter from the top with a subtle fade-in animation (`animate-in fade-in duration-300` from tailwind-animate).

Each event card (`components/live/EventCard.tsx`):
```
┌─────────────────────────────────────────────────────┐
│ [inapp]  payment.completed          [high]  2s ago  │
│ message_id: 019d5f38-...                            │
│ { "amount": 99.99, "currency": "USD" }              │
└─────────────────────────────────────────────────────┘
```
- Channel badge: email=blue, inapp=green, webhook=pink (same as dashboard)
- Priority badge: high=red, normal=yellow, low=gray
- Payload: rendered as formatted JSON in a `<pre>` block, `text-xs font-mono`, collapsed to 3 lines, expandable on click
- Timestamp: relative time (e.g. "2s ago"), updates every second

**Empty state** (no events yet):
```
[Activity icon]
Waiting for events
Open another tab and publish an event to see it appear here in real time
[→ Go to Publish]  ← button that navigates to /publish
```

---

## Section 4 — Load Test Page (`/loadtest`)

File: `app/loadtest/page.tsx`
Load test console. This is the portfolio demo centerpiece — start demo mode and watch the system go from 0 to peak throughput.

### Load test hook
Create `hooks/useLoadTest.ts`:
```typescript
'use client'
import { useState, useEffect, useRef } from 'react'

export type LoadTestStatus = 'idle' | 'running' | 'completed' | 'error'

export interface LoadTestResult {
  run_id: string
  status: LoadTestStatus
  started_at: string
  completed_at?: string
  // extend as backend returns more fields
  [key: string]: unknown
}

export function useLoadTest() {
  const [status, setStatus] = useState<LoadTestStatus>('idle')
  const [result, setResult] = useState<LoadTestResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const pollRef = useRef<NodeJS.Timeout | null>(null)
  
  const BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080'

  const start = async () => {
    try {
      setStatus('running')
      setError(null)
      const res = await fetch(`${BASE}/ops/loadtest/start`, { method: 'POST' })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      // start polling for status
      pollRef.current = setInterval(async () => {
        try {
          const r = await fetch(`${BASE}/ops/loadtest/latest`)
          if (r.ok) {
            const d = await r.json()
            setResult(d)
            if (d.status === 'completed' || d.status === 'error') {
              setStatus(d.status)
              clearInterval(pollRef.current!)
            }
          }
        } catch {}
      }, 2000)
    } catch (e) {
      setStatus('error')
      setError(String(e))
    }
  }

  useEffect(() => () => { if (pollRef.current) clearInterval(pollRef.current) }, [])

  return { status, result, error, start }
}
```

### Page layout
Same sidebar layout. Title: "Load Test"
Subtitle: "Run a demo load test to see the system under pressure"

**Control panel** — shadcn Card:
- Description: "Demo mode simulates ~55 seconds of sustained traffic across all channels and priority levels."
- "Start Demo" button — large, prominent. Disabled while running.
- While running: show a progress bar (indeterminate) + elapsed time counter (counts up from 0s)
- On complete: show "Test completed" with green checkmark

**Live metrics during test** — use `useMetrics` hook (already exists), show 4 metric cards identical to dashboard but updating in real time:
- Publish Rate, P99 Latency, DLQ Backlog, WS Connections

**Throughput chart** — reuse `ChartAreaInteractive` component, pass the `history` from `useMetrics`.
This chart will show the burst of traffic during the test — this is the visual payoff.

**Results panel** — appears after test completes (shadcn Card):
- Show raw JSON from `GET /ops/loadtest/latest` in a `<pre>` block
- "Run Again" button to reset

---

## Section 5 — Notifications Page (`/notifications`)

File: `app/notifications/page.tsx`
Full notification list with filters. Already has `useNotifications` and `DataTable` wired up.

### Add filter bar
Create `components/notifications/FilterBar.tsx`:

Three filter controls in a horizontal row:

**Channel** — toggle group: `All` | `email` | `inapp` | `webhook`
**Status** — toggle group: `All` | `delivered` | `failed` | `duplicate` | `dlq`
**Search** — text input: filters by `event_type` substring (client-side)

All filters are client-side. Filter the `notifications` array before passing to DataTable.

**Clear All** button — appears only when any filter is active. Resets all to default.

**Record count** — `"{n} notifications"` text, right-aligned, updates as filters change.

Also add a **"Clear All Notifications"** button (destructive, top-right of page) that calls `POST /notifications/clear` then refreshes the list. Show confirmation dialog before executing (use shadcn `AlertDialog`).

---

## Section 6 — DLQ Page (`/dlq`)

File: `app/dlq/page.tsx`

### Layout
Same sidebar layout. Title: "Dead-Letter Queue"
Subtitle: "Messages that failed processing after maximum retries"

### DLQ summary cards
Fetch from `GET /api/metrics/summary` (use `useMetrics` hook).
Show 3 cards for total DLQ count per channel, derived from queue_depth:
- Email DLQ: sum of `email_high` + `email_normal` + `email_low` from queue_depth (TODO: backend doesn't expose DLQ depth separately yet — show `dlq_count` total for now with a note)
- Total DLQ: `metrics.dlq_count` — large number, red if > 0

### Replay panel — shadcn Card
Description: "Replay all messages currently in the dead-letter queues back into the main processing pipeline."
Warning text (yellow): "Replayed messages will be re-processed. Ensure the underlying issue has been resolved before replaying."

**"Replay All"** button — destructive variant.
On click: show shadcn `AlertDialog` confirmation: "Are you sure? This will replay {n} DLQ messages."
On confirm: call `POST /dlq/replay`, show success/error toast.
After success: refresh metrics after 3s.

### DLQ queue breakdown table
Show a table with columns: Queue Name | Depth | Action
Rows: all 9 DLQ queues (email/inapp/webhook × high/normal/low)
Values come from `metrics.queue_depth` — show DLQ queues only.
Note: queue_depth currently returns main queue depths, not DLQ depths. Display what's available and add a comment `// TODO: backend should expose dlq_depth separately`.

---

## Section 7 — Global Polish

### 7.1 Active nav link highlighting
In `components/app-sidebar.tsx`, the current page should be highlighted in the sidebar.
Use `usePathname()` from `next/navigation` to compare against each nav item's url.
Active item: use the existing shadcn sidebar active styles (check `components/ui/sidebar.tsx` for the correct `isActive` prop).

### 7.2 Page titles in browser tab
Each page should set a unique `<title>` via Next.js metadata.
Add to each page file:
```typescript
export const metadata = { title: 'Dashboard — Nexus' }
export const metadata = { title: 'Live Feed — Nexus' }
// etc.
```
Note: metadata export only works in Server Components. Pages using hooks must be Client Components — use a `<title>` tag inside a `useEffect` instead, or split into a server wrapper + client component.

### 7.3 Loading skeletons
Every page that fetches data should show shadcn `Skeleton` components while loading, not blank space.
- Dashboard metric cards: 4 skeleton cards same size as real cards
- Notifications table: 5 skeleton rows
- Live feed: 3 skeleton event cards

### 7.4 Error states
Every page that fetches data should handle fetch errors gracefully.
Show a shadcn `Alert` (destructive variant) when `error !== null`:
```
[AlertCircle icon] Failed to load data
Could not connect to the Nexus backend at localhost:8080. Make sure the producer service is running.
[Retry] button
```

### 7.5 Relative timestamps
All `created_at` timestamps in tables and event cards should display as relative time ("2s ago", "5m ago", "1h ago").
Install and use `date-fns`:
```bash
npm install date-fns
```
Use `formatDistanceToNow(new Date(created_at), { addSuffix: true })`.
Update the timestamp every 30 seconds using a `setInterval` in a `useEffect`.

### 7.6 Remove unused shadcn demo content
- In `app-sidebar.tsx`: remove the team switcher dropdown (replace with a static "Nexus" logo/text)
- Remove "shadcn" username at the bottom — replace with a neutral "System" label or remove entirely
- Remove "Settings", "Get Help", "Search" from the bottom nav — or keep only if wired up

---

## Final verification

After completing all sections:

```bash
cd web && npm run build
```

Expected: 0 TypeScript errors, 0 warnings about missing props.

Then do a full demo run:
1. Open `/dashboard` — metrics cards show live data, chart updates every 5s
2. Open `/publish` — publish a `payment.completed` event, see success toast
3. Open `/live` — see the event appear in real time
4. Open `/loadtest` — start demo mode, watch throughput chart spike over 55s
5. Open `/notifications` — filter by channel, see correct results
6. Open `/dlq` — replay button works, toast appears