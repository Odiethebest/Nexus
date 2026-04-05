export type Channel  = 'email' | 'inapp' | 'webhook'
export type Priority = 'high' | 'normal' | 'low'
export type Status   = 'delivered' | 'failed' | 'duplicate' | 'dlq'

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

export interface MetricsSummary {
  publish_rate_per_sec:      number
  processing_latency_p99_ms: number
  queue_depth:               Record<string, number>
  delivery_success_rate:     number
  dlq_count:                 number
  active_ws_connections:     number
  uptime_seconds:            number
}
