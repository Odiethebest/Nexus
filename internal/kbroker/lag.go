package kbroker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"nexus/internal/metrics"
)

// LagReader periodically samples consumer-group offsets and DLQ topic end
// offsets and pushes them into Prometheus gauges. This is what turns the
// summary endpoint's queue_depth / dlq_count fields from hardcoded zeros
// (metrics/summary.go pre-Step-4) into real numbers.
type LagReader struct {
	admin *kadm.Client
	log   *slog.Logger
}

// NewLagReader opens its own kgo.Client (admin operations only — no
// consuming, no producing) so its shutdown ordering is independent of the
// publisher and workers.
func NewLagReader(cfg Config, log *slog.Logger) (*LagReader, error) {
	client, err := kgo.NewClient(cfg.BaseOpts()...)
	if err != nil {
		return nil, fmt.Errorf("kbroker: lag reader client: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	return &LagReader{admin: kadm.NewClient(client), log: log}, nil
}

// Close releases the underlying client.
func (r *LagReader) Close() { r.admin.Close() }

// Run samples every interval until ctx is cancelled. A single sample failure
// is logged at debug level (Redpanda restart / transient network) — the
// gauges keep their previous values.
func (r *LagReader) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 3 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	// Sample immediately so the first scrape has non-zero data.
	r.sampleOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.sampleOnce(ctx)
		}
	}
}

func (r *LagReader) sampleOnce(ctx context.Context) {
	if err := r.sampleConsumerLag(ctx); err != nil {
		r.log.Debug("lag reader: sample consumer lag", "err", err)
	}
	if err := r.sampleDLQ(ctx); err != nil {
		r.log.Debug("lag reader: sample DLQ", "err", err)
	}
}

func (r *LagReader) sampleConsumerLag(ctx context.Context) error {
	// One admin call per lane keeps the code straight-line simple; there are
	// only 9 lanes so the overhead is negligible next to the 3s interval.
	for _, ch := range Channels {
		for _, p := range Priorities {
			topic := TopicName(ch, p)
			group := ConsumerGroup(ch, p)

			ends, err := r.admin.ListEndOffsets(ctx, topic)
			if err != nil {
				return fmt.Errorf("list end offsets %s: %w", topic, err)
			}
			committed, err := r.admin.FetchOffsetsForTopics(ctx, group, topic)
			if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
				return fmt.Errorf("fetch offsets %s: %w", group, err)
			}

			lag := int64(0)
			ends.Each(func(o kadm.ListedOffset) {
				endOff := o.Offset
				var commit int64
				if resp, ok := committed.Lookup(o.Topic, o.Partition); ok && resp.Err == nil {
					commit = resp.At
					if commit < 0 { // -1 means no committed offset yet
						commit = 0
					}
				}
				if diff := endOff - commit; diff > 0 {
					lag += diff
				}
			})

			metrics.ConsumerLagRecords.WithLabelValues(string(ch), string(p)).Set(float64(lag))
		}
	}
	return nil
}

func (r *LagReader) sampleDLQ(ctx context.Context) error {
	for _, ch := range Channels {
		for _, p := range Priorities {
			topic := DLQTopic(ch, p)
			ends, err := r.admin.ListEndOffsets(ctx, topic)
			if err != nil {
				return fmt.Errorf("list end offsets %s: %w", topic, err)
			}
			total := int64(0)
			ends.Each(func(o kadm.ListedOffset) {
				if o.Offset > 0 {
					total += o.Offset
				}
			})
			metrics.DLQMessages.WithLabelValues(string(ch), string(p)).Set(float64(total))
		}
	}
	return nil
}
