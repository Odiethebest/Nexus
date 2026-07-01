package kworker

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"

	"nexus/internal/kbroker"
)

// KafkaRepublisher writes to the primary topic (for retries) and DLQ topics
// (for permanent failures). It uses its own kgo.Client so producers and
// consumers stay isolated — franz-go allows sharing but the roles have
// very different tuning knobs and mixing them makes shutdown ordering
// fragile.
type KafkaRepublisher struct {
	client *kgo.Client
}

// NewKafkaRepublisher wires a producer with acks=all + idempotence.
func NewKafkaRepublisher(cfg kbroker.Config) (*KafkaRepublisher, error) {
	opts := append(cfg.BaseOpts(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.MaxBufferedRecords(100_000),
	)
	c, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kworker: republisher client: %w", err)
	}
	return &KafkaRepublisher{client: c}, nil
}

// Close flushes buffered records and shuts down.
func (r *KafkaRepublisher) Close(ctx context.Context) error {
	if err := r.client.Flush(ctx); err != nil {
		return err
	}
	r.client.Close()
	return nil
}

// Retry re-produces rec back onto its original topic with an incremented
// x-retry-count header. Keeps the original x-produced-at so the e2e lag
// histogram reflects the true age from the caller's perspective, not the
// latest retry.
func (r *KafkaRepublisher) Retry(ctx context.Context, rec *kgo.Record, retryCount int) error {
	newRec := cloneWithRetry(rec, rec.Topic, retryCount)
	return r.produce(ctx, newRec)
}

// DLQ produces a copy of rec to the given DLQ topic. Headers, key, and
// value are preserved; retry counter is stamped so downstream tooling can
// see how many attempts it took.
func (r *KafkaRepublisher) DLQ(ctx context.Context, rec *kgo.Record, dlqTopic string) error {
	newRec := cloneWithRetry(rec, dlqTopic, MaxRetries)
	return r.produce(ctx, newRec)
}

func (r *KafkaRepublisher) produce(ctx context.Context, rec *kgo.Record) error {
	done := make(chan error, 1)
	r.client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		done <- err
	})
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func cloneWithRetry(rec *kgo.Record, topic string, retryCount int) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(rec.Headers))
	sawRetry := false
	for _, h := range rec.Headers {
		if h.Key == kbroker.HeaderRetryCount {
			headers = append(headers, kgo.RecordHeader{
				Key:   kbroker.HeaderRetryCount,
				Value: []byte(strconv.Itoa(retryCount)),
			})
			sawRetry = true
			continue
		}
		headers = append(headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	if !sawRetry {
		headers = append(headers, kgo.RecordHeader{
			Key:   kbroker.HeaderRetryCount,
			Value: []byte(strconv.Itoa(retryCount)),
		})
	}
	return &kgo.Record{
		Topic:   topic,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: headers,
	}
}
