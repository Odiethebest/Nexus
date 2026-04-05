export type Channel  = 'email' | 'inapp' | 'webhook'
export type Priority = 'high' | 'normal' | 'low'
export type Status   = 'delivered' | 'failed' | 'duplicate' | 'dlq'

export interface Notification {
  message_id: string
  channel:    Channel
  event_type: string
  // priority 字段 DB 暂无此列，待 migration 后启用
  status:     Status
  payload:    Record<string, unknown>
  created_at: string
}

export interface WsEvent {
  message_id: string
  type:       string    // 后端字段名，非 event_type
  priority:   Priority
  payload:    Record<string, unknown>
  timestamp:  string    // 后端字段名，非 created_at
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
