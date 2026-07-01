package kbroker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"nexus/internal/metrics"
)

// Publisher writes events to Redpanda. Its Publish method keeps the exact
// signature the AMQP Publisher exposed, so HTTP handlers and gRPC ingest
// need no changes.
//
// Reliability model:
//   - idempotent producer (default in franz-go) prevents duplicates on
//     retries
//   - RequiredAcks=all(-1) so every ack is confirmed by every in-sync
//     replica before Publish returns success
//   - async Produce with per-record callback so a single publish never
//     blocks the whole client on inflight confirms (the AMQP single-flight
//     was the throughput ceiling we removed)
type Publisher struct {
	client *kgo.Client
}

// NewPublisher constructs a Publisher. The returned client is safe for
// concurrent use — franz-go batches records internally per partition.
func NewPublisher(cfg Config) (*Publisher, error) {
	opts := append(cfg.BaseOpts(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// The default partitioner already keys off record.Key, but we set it
		// explicitly so message_id-based routing is documented in code.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		// Room for high-throughput bursts; kgo backpressures beyond this.
		kgo.MaxBufferedRecords(200_000),
		kgo.ProducerBatchMaxBytes(1<<20), // 1 MiB
	)
	c, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kbroker: new producer client: %w", err)
	}
	return &Publisher{client: c}, nil
}

// Close flushes buffered records and shuts the client down.
func (p *Publisher) Close(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return err
	}
	p.client.Close()
	return nil
}

// Publish fans a single event out to every channel's lane topic
// (nexus.email.<prio>, nexus.inapp.<prio>, nexus.webhook.<prio>). This
// preserves the exact behavior of the previous AMQP topic exchange, which
// bound each channel's queue to `event.*.<priority>` — i.e. every event
// reached every channel and the worker itself decided whether to deliver
// (e.g. webhook worker no-ops when the payload has no webhook_url).
//
// Returns as soon as every fan-out target is acked (all-ISR). The record
// key is the generated message_id so retries land on the same partition
// and the idempotent producer can deduplicate broker-side.
func (p *Publisher) Publish(ctx context.Context, eventType, priority string, payload map[string]any) (string, error) {
	start := time.Now()

	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("publisher: generate uuid: %w", err)
	}
	msgID := id.String()

	prio := normalizePriority(priority)
	channels := Channels // fan out to email + inapp + webhook

	event := Event{
		MessageID: msgID,
		Type:      eventType,
		Priority:  string(prio),
		Payload:   payload,
		Timestamp: time.Now().UTC(),
	}
	body, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("publisher: marshal: %w", err)
	}

	producedAt := strconv.FormatInt(event.Timestamp.UnixNano(), 10)

	// Fire one record per target channel. We collect their acks via a
	// bounded channel so all produces run in parallel while Publish still
	// returns synchronously to the HTTP caller.
	errs := make(chan error, len(channels))
	for _, ch := range channels {
		rec := &kgo.Record{
			Topic: TopicName(ch, prio),
			Key:   []byte(msgID),
			Value: body,
			Headers: []kgo.RecordHeader{
				{Key: HeaderMsgID, Value: []byte(msgID)},
				{Key: HeaderEventType, Value: []byte(eventType)},
				{Key: HeaderPriority, Value: []byte(prio)},
				{Key: HeaderProducedAt, Value: []byte(producedAt)},
				{Key: HeaderRetryCount, Value: []byte("0")},
			},
		}
		ch := ch // capture
		p.client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if err == nil {
				metrics.StageIngestDuration.WithLabelValues(string(ch), string(prio)).Observe(time.Since(start).Seconds())
				metrics.EventsPublished.WithLabelValues(string(ch), string(prio)).Inc()
				metrics.MessagesPublished.WithLabelValues(eventType, string(prio)).Inc()
			}
			errs <- err
		})
	}

	// Wait for every fan-out target to ack. If any fails we surface the
	// first error; the idempotent producer already handled internal retries.
	var firstErr error
	for i := 0; i < len(channels); i++ {
		select {
		case err := <-errs:
			if err != nil && firstErr == nil {
				firstErr = err
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if firstErr != nil {
		return "", fmt.Errorf("publisher: produce: %w", firstErr)
	}

	return msgID, nil
}

// normalizePriority coerces incoming priority strings to a known lane, so
// the router never lands on a topic that doesn't exist.
func normalizePriority(p string) Priority {
	switch strings.ToLower(strings.TrimSpace(p)) {
	case "high":
		return PriorityHigh
	case "low":
		return PriorityLow
	default:
		return PriorityNormal
	}
}
