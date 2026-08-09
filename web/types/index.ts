export type Channel  = 'email' | 'inapp' | 'webhook'
export type Priority = 'high' | 'normal' | 'low'

/**
 * Row statuses actually written to `notifications.status`, per
 * internal/kworker/runner.go:
 *   delivered — dispatched successfully
 *   skipped   — nothing to do for this channel (e.g. no webhook_url)
 *   failed    — a transient attempt failed; a retry is in flight
 *   dlq       — permanently failed, or the retry budget ran out
 *
 * Note there is no 'duplicate': a duplicate is committed without writing a
 * row, so it only ever exists as the
 * nexus_messages_processed_total{status="duplicate"} counter label.
 */
export type Status = 'delivered' | 'skipped' | 'failed' | 'dlq'

export interface Notification {
  message_id: string
  channel:    Channel
  event_type: string
  // priority field not yet in DB schema — re-enable after migration
  status:     Status
  payload:    Record<string, unknown>
  created_at: string
}

export interface WsEvent {
  message_id: string
  type:       string      // NOT event_type
  priority:   Priority
  channel?:   Channel     // optional — live WS events are always 'inapp'
  payload:    Record<string, unknown>
  timestamp:  string      // NOT created_at
}

/**
 * Mirrors metrics.SummarySnapshot (internal/metrics/summary.go).
 * Every field is always present in the response — none are `omitempty` on
 * the Go side. Components still guard against `undefined` so a browser
 * holding an old bundle against a newer/older producer degrades to "—"
 * rather than throwing.
 */
export interface MetricsSummary {
  publish_rate_per_sec:      number
  /** Worker-side completion rate. Diverges from publish under backpressure. */
  processed_rate_per_sec:    number
  /** Per-channel completion rate, keyed by Channel. Real counter deltas. */
  processed_rate_per_sec_by_channel: Record<Channel, number>
  processing_latency_p99_ms: number
  /** p99 of (now − x-produced-at) at pick-up. The "lag < 1.5s" SLO metric. */
  e2e_lag_p99_seconds:       number
  queue_depth:               Record<string, number>
  delivery_success_rate:     number
  dlq_count:                 number
  active_ws_connections:     number
  uptime_seconds:            number
}

/** A summary reading plus the wall-clock time the browser received it. */
export interface MetricsSample extends MetricsSummary {
  received_at: number
}
