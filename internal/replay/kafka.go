// Package replay provides DLQ message replay — reads dead-lettered messages
// from a Kafka DLQ topic and republishes them to the primary lane topic
// for reprocessing. Every successfully replayed record has its
// x-retry-count header reset to 0 so downstream workers give it a fresh
// budget.
package replay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"nexus/internal/kbroker"
)

// Replayer is a small facade that dispatches to either the Kafka or the
// AMQP backend depending on which was wired at startup. This keeps the
// public method signature identical to the pre-migration AMQP replayer so
// the HTTP handler in cmd/producer/main.go does not care which backend
// is active.
type Replayer struct {
	// Exactly one of these is non-nil.
	kafka *kafkaReplayer
	amqp  *AMQPReplayer
}

// Replay routes to the configured backend.
func (r *Replayer) Replay(ctx context.Context, target string, max int) (int, error) {
	switch {
	case r.kafka != nil:
		return r.kafka.replay(ctx, target, max)
	case r.amqp != nil:
		return r.amqp.replay(ctx, target, max)
	default:
		return 0, fmt.Errorf("replay: no backend configured")
	}
}

// kafkaReplayer holds the Kafka-side state for the facade.
type kafkaReplayer struct {
	cfg kbroker.Config
	log *slog.Logger
}

// New constructs a Kafka-backed Replayer.
func New(cfg kbroker.Config, log *slog.Logger) *Replayer {
	if log == nil {
		log = slog.Default()
	}
	return &Replayer{kafka: &kafkaReplayer{cfg: cfg, log: log}}
}

// replay pulls up to max records from the DLQ topic (target) and
// re-produces each to the corresponding primary topic. `target` accepts
// both Kafka-native names ("nexus.dlq.email.high") and the legacy AMQP
// form ("nexus.email.dlq.high") — the latter is normalized so the
// frontend keeps working across the migration.
//
// Uses a dedicated consumer group ("nexus.replay") with manual commits, so
// each DLQ record is replayed once per operator invocation; a re-run of
// this call after everything drains is a no-op (idempotency also ensures
// worker-side dedupe).
func (r *kafkaReplayer) replay(ctx context.Context, target string, max int) (int, error) {
	dlqTopic := kbroker.NormalizeDLQTopic(target)
	primary, ok := kbroker.PrimaryFromDLQ(dlqTopic)
	if !ok {
		return 0, fmt.Errorf("replay: %q does not look like a DLQ topic", target)
	}
	if max <= 0 {
		return 0, errors.New("replay: max must be > 0")
	}

	consumerOpts := append(r.cfg.BaseOpts(),
		kgo.ConsumeTopics(dlqTopic),
		kgo.ConsumerGroup("nexus.replay"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(30*time.Second),
	)
	consumer, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		return 0, fmt.Errorf("replay: open consumer: %w", err)
	}
	defer consumer.Close()

	producerOpts := append(r.cfg.BaseOpts(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	producer, err := kgo.NewClient(producerOpts...)
	if err != nil {
		return 0, fmt.Errorf("replay: open producer: %w", err)
	}
	defer producer.Close()

	// Bound the fetch loop with a short deadline. If the DLQ is empty
	// PollFetches would otherwise block forever waiting for new records.
	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	replayed := 0
	for replayed < max {
		fetches := consumer.PollFetches(fetchCtx)
		if err := ctx.Err(); err != nil {
			return replayed, err
		}
		if fetches.IsClientClosed() {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			// Timeout / no records — treat as end-of-batch, not an error.
			// Any other error we surface after processing what we have.
			allTimeout := true
			for _, e := range errs {
				if !errors.Is(e.Err, context.DeadlineExceeded) && !errors.Is(e.Err, context.Canceled) {
					allTimeout = false
					r.log.Warn("replay: poll error", "topic", e.Topic, "err", e.Err)
				}
			}
			if allTimeout {
				break
			}
		}
		if fetches.Empty() {
			break
		}

		iter := fetches.RecordIter()
		for !iter.Done() && replayed < max {
			rec := iter.Next()
			newRec := rebuildForPrimary(rec, primary)

			done := make(chan error, 1)
			producer.Produce(ctx, newRec, func(_ *kgo.Record, err error) {
				done <- err
			})
			select {
			case err := <-done:
				if err != nil {
					r.log.Error("replay: republish failed",
						"dlq", dlqTopic, "primary", primary, "err", err)
					return replayed, fmt.Errorf("replay: republish: %w", err)
				}
			case <-ctx.Done():
				return replayed, ctx.Err()
			}
			if err := consumer.CommitRecords(ctx, rec); err != nil {
				r.log.Warn("replay: commit offset", "err", err)
			}
			replayed++
		}
	}

	// Ensure any lingering buffered records land before we exit.
	if err := producer.Flush(ctx); err != nil {
		r.log.Warn("replay: flush producer", "err", err)
	}
	r.log.Info("replay: done", "dlq", dlqTopic, "primary", primary, "replayed", replayed)
	return replayed, nil
}

// rebuildForPrimary produces a fresh record targeting the primary topic
// with x-retry-count reset to 0. Other headers (x-msg-id, x-event-type,
// x-priority, x-produced-at) are preserved — the e2e lag histogram will
// then reflect "time from the original publish to when the replayed
// record was picked up," which is a truthful representation of the DLQ
// delay for the operator.
func rebuildForPrimary(rec *kgo.Record, primary string) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(rec.Headers))
	sawRetry := false
	for _, h := range rec.Headers {
		if h.Key == kbroker.HeaderRetryCount {
			headers = append(headers, kgo.RecordHeader{
				Key:   kbroker.HeaderRetryCount,
				Value: []byte(strconv.Itoa(0)),
			})
			sawRetry = true
			continue
		}
		headers = append(headers, kgo.RecordHeader{Key: h.Key, Value: append([]byte(nil), h.Value...)})
	}
	if !sawRetry {
		headers = append(headers, kgo.RecordHeader{
			Key:   kbroker.HeaderRetryCount,
			Value: []byte("0"),
		})
	}
	return &kgo.Record{
		Topic:   primary,
		Key:     append([]byte(nil), rec.Key...),
		Value:   append([]byte(nil), rec.Value...),
		Headers: headers,
	}
}
