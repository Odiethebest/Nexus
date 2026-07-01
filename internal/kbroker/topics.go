// Package kbroker holds the Redpanda (Kafka-protocol) implementation of the
// event bus that Nexus uses in place of RabbitMQ. Redpanda is a single-binary
// KRaft broker fully wire-compatible with the Kafka protocol; we choose it
// because it starts in seconds locally and offers a managed dev cluster
// (Redpanda Cloud) for Railway.
package kbroker

import (
	"fmt"
	"strings"
)

// Priority mirrors the three delivery tiers Nexus supports. High-priority
// events live on their own topic so a low-priority backlog can never block
// them.
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// Channel identifies a fan-out destination (email / in-app WebSocket /
// outbound webhook). Kept in sync with the notifications.channel column.
type Channel string

const (
	ChannelEmail   Channel = "email"
	ChannelInApp   Channel = "inapp"
	ChannelWebhook Channel = "webhook"
)

// Priorities lists tiers in descending priority order. Consumer goroutines
// use this order to give high-priority lanes larger worker pools.
var Priorities = []Priority{PriorityHigh, PriorityNormal, PriorityLow}

// Channels lists the fan-out destinations.
var Channels = []Channel{ChannelEmail, ChannelInApp, ChannelWebhook}

// TopicName returns the primary topic for a (channel, priority) pair, e.g.
// "nexus.email.high". Each pair gets its own topic so consumer groups can
// scale independently per lane.
func TopicName(ch Channel, p Priority) string {
	return fmt.Sprintf("nexus.%s.%s", ch, p)
}

// DLQTopic returns the dead-letter topic for a (channel, priority) pair, e.g.
// "nexus.dlq.email.high". A message lands here after the retry budget is
// exhausted or on a permanent-failure verdict.
func DLQTopic(ch Channel, p Priority) string {
	return fmt.Sprintf("nexus.dlq.%s.%s", ch, p)
}

// ConsumerGroup returns the Kafka consumer-group id for a lane. Using a
// distinct group per (channel, priority) means committed offsets in one lane
// never gate progress in another — a stuck low lane can't slow high.
func ConsumerGroup(ch Channel, p Priority) string {
	return fmt.Sprintf("nexus.%s.%s", ch, p)
}

// AllTopics returns every primary topic managed by kbroker (channels ×
// priorities). Used at startup to auto-create topics with the configured
// partition count.
func AllTopics() []string {
	out := make([]string, 0, len(Channels)*len(Priorities))
	for _, ch := range Channels {
		for _, p := range Priorities {
			out = append(out, TopicName(ch, p))
		}
	}
	return out
}

// AllDLQTopics returns every dead-letter topic.
func AllDLQTopics() []string {
	out := make([]string, 0, len(Channels)*len(Priorities))
	for _, ch := range Channels {
		for _, p := range Priorities {
			out = append(out, DLQTopic(ch, p))
		}
	}
	return out
}

// NormalizeDLQTopic accepts the legacy AMQP DLQ name form used by the
// current /dlq/replay HTTP body ("nexus.email.dlq.high") and returns the
// Kafka-native form ("nexus.dlq.email.high"). Callers can pass either form.
func NormalizeDLQTopic(name string) string {
	name = strings.TrimSpace(name)
	if strings.HasPrefix(name, "nexus.dlq.") {
		return name
	}
	// legacy form: nexus.<channel>.dlq.<priority>
	const marker = ".dlq."
	if idx := strings.Index(name, marker); idx > 0 && strings.HasPrefix(name, "nexus.") {
		channel := name[len("nexus.") : idx]
		priority := name[idx+len(marker):]
		if channel != "" && priority != "" {
			return fmt.Sprintf("nexus.dlq.%s.%s", channel, priority)
		}
	}
	return name
}

// PrimaryFromDLQ inverts DLQTopic: given "nexus.dlq.webhook.normal" it
// returns "nexus.webhook.normal" so a replayer can re-produce to the lane
// that fed the DLQ.
func PrimaryFromDLQ(dlq string) (string, bool) {
	const prefix = "nexus.dlq."
	if !strings.HasPrefix(dlq, prefix) {
		return "", false
	}
	tail := dlq[len(prefix):]
	// tail = "<channel>.<priority>"
	dot := strings.IndexByte(tail, '.')
	if dot < 0 {
		return "", false
	}
	return "nexus." + tail, true
}
