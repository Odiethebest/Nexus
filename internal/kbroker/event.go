package kbroker

import "time"

// Event is the JSON envelope published to Redpanda topics and forwarded
// verbatim to WebSocket clients by the in-app worker. The field layout is a
// wire contract with the frontend (types/index.ts WsEvent) — do not rename
// or reorder existing fields.
type Event struct {
	MessageID string         `json:"message_id"`
	Type      string         `json:"type"`
	Priority  string         `json:"priority"`
	Payload   map[string]any `json:"payload"`
	Timestamp time.Time      `json:"timestamp"`
}

// Kafka record header keys. Producers set them on every record; consumers
// read them for idempotency, priority routing, retry budgeting, and the
// end-to-end lag histogram (now − x-produced-at).
const (
	HeaderMsgID      = "x-msg-id"
	HeaderEventType  = "x-event-type"
	HeaderPriority   = "x-priority"
	HeaderProducedAt = "x-produced-at" // decimal string, unix nanoseconds
	HeaderRetryCount = "x-retry-count" // decimal string, starts at 0
)
