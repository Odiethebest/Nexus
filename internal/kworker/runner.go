// Package kworker consumes events from Redpanda and drives per-channel
// delivery (email / in-app WebSocket / webhook). Each (channel, priority)
// pair gets its own consumer group and worker pool so a stuck low-priority
// lane cannot slow a high-priority one — the queue-level isolation the AMQP
// implementation used to get for free via three separate queues + Qos.
package kworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"nexus/internal/idempotency"
	"nexus/internal/kbroker"
	"nexus/internal/metrics"
	"nexus/internal/store"
	"nexus/internal/wsfeed"
)

// Outcome describes what the Processor decided to do with an event.
type Outcome int

const (
	OutcomeDelivered      Outcome = iota // persist as "delivered"
	OutcomeSkipped                       // persist as "skipped" (e.g. no webhook URL)
	OutcomeTransientError                // retry via re-produce; DLQ if budget exhausted
	OutcomePermanentError                // straight to DLQ
)

// MaxRetries caps how many times the same record is re-produced back onto
// the primary topic before being routed to the DLQ. AMQP webhook worker
// used the same budget (webhook.go:26). A record therefore sees at most
// MaxRetries+1 delivery attempts, spaced by defaultRetryBackoff.
const MaxRetries = 3

// Processor is the per-channel delivery logic. Email/InApp/Webhook each
// implement one. Kept small so the runner can drive all three identically.
type Processor interface {
	Channel() kbroker.Channel
	// Deliver performs the actual dispatch. It must NOT commit offsets or
	// touch the idempotency store — the runner does those uniformly.
	Deliver(ctx context.Context, event kbroker.Event, body []byte) Outcome
}

// Republisher writes back to primary/DLQ topics from within the worker. It
// is a small interface so tests can substitute a fake.
type Republisher interface {
	Retry(ctx context.Context, rec *kgo.Record, retryCount int) error
	DLQ(ctx context.Context, rec *kgo.Record, dlqTopic string) error
}

// NotificationStore is the slice of store.Store the runner persists
// through. Narrowed to an interface so the record-handling path can be
// unit-tested without a live PostgreSQL.
type NotificationStore interface {
	SaveNotification(ctx context.Context, n store.Notification) error
	// HasNotification reports whether this (message_id, channel) was already
	// written. It is the durable record of completion, used to second-guess
	// the idempotency claim — see Runner.handle.
	HasNotification(ctx context.Context, messageID, channel string) (bool, error)
}

// recordCommitter is the offset-commit half of *kgo.Client, split out for
// the same reason as NotificationStore.
type recordCommitter interface {
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
}

// LiveFeed receives one envelope per record the runner reaches a verdict on,
// for the dashboard's /ws stream. Publishing happens here rather than inside
// a Processor so every channel appears in the feed, not just in-app — the UI
// filters by channel and needs all three.
//
// Implementations must not block: this sits on the delivery path. Optional;
// a nil LiveFeed disables the feed.
type LiveFeed interface {
	Publish(ctx context.Context, env wsfeed.Envelope)
}

// Runner drives a single (channel, priority) lane end-to-end.
type Runner struct {
	Channel     kbroker.Channel
	Priority    kbroker.Priority
	PoolSize    int
	Processor   Processor
	Idempotency *idempotency.Client
	Store       NotificationStore
	Republisher Republisher
	Feed        LiveFeed
	Log         *slog.Logger

	client    *kgo.Client
	committer recordCommitter
	// inflight tracks records handed to the pool but not yet finished. The
	// rebalance callback waits on it, so it must outlive a single Run loop.
	inflight sync.WaitGroup
	// backoff maps "retries already attempted" to the wait before the next
	// one. Injectable so tests don't sit through the real 2s/4s/8s.
	backoff func(retryCount int) time.Duration
}

// NewRunner wires a franz-go consumer client to a Processor. Each lane gets
// a dedicated consumer group id (kbroker.ConsumerGroup) so committed
// offsets never gate progress across lanes.
func NewRunner(cfg kbroker.Config, opts RunnerOptions) (*Runner, error) {
	if opts.PoolSize <= 0 {
		return nil, errors.New("kworker: PoolSize must be > 0")
	}
	if opts.Processor == nil {
		return nil, errors.New("kworker: Processor is required")
	}

	topic := kbroker.TopicName(opts.Channel, opts.Priority)
	group := kbroker.ConsumerGroup(opts.Channel, opts.Priority)

	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	r := &Runner{
		Channel:     opts.Channel,
		Priority:    opts.Priority,
		PoolSize:    opts.PoolSize,
		Processor:   opts.Processor,
		Idempotency: opts.Idempotency,
		Store:       opts.Store,
		Republisher: opts.Republisher,
		Feed:        opts.Feed,
		Log:         log.With("channel", string(opts.Channel), "priority", string(opts.Priority)),
		backoff:     defaultRetryBackoff,
	}

	clientOpts := append(cfg.BaseOpts(),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Block rebalances while records are in-flight so we never lose
		// an offset ack because a partition was revoked mid-processing.
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(30*time.Second),
		// Hold the rebalance until in-flight records have finished.
		// BlockRebalanceOnPoll alone is not enough: it only defers
		// revocation to the next AllowRebalance, and we call that as soon as
		// a batch is dispatched, while the pool is still working.
		kgo.OnPartitionsRevoked(r.awaitInflightBeforeRevoke),
	)
	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("kworker: new consumer client (%s): %w", topic, err)
	}
	r.client = client
	r.committer = client
	return r, nil
}

// awaitInflightBeforeRevoke blocks a rebalance until every record this
// runner has in flight has finished.
//
// Without it a partition can move mid-processing: the original goroutine
// then commits an offset for a partition it no longer owns (rejected), and
// the new owner redelivers a record whose idempotency claim is still held.
// It is also what makes the guarantee stated in deploy/railway.worker.toml
// true rather than aspirational.
//
// The wait is bounded by the slowest in-flight record. The worst case is a
// record sleeping out the 8s retry backoff, comfortably inside franz-go's
// 60s rebalance timeout.
func (r *Runner) awaitInflightBeforeRevoke(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	start := time.Now()
	r.inflight.Wait()
	if waited := time.Since(start); waited > time.Second {
		r.Log.Info("held rebalance for in-flight records",
			"waited", waited.Round(time.Millisecond), "partitions", revoked)
	}
}

// RunnerOptions bundles the per-lane construction inputs.
type RunnerOptions struct {
	Channel     kbroker.Channel
	Priority    kbroker.Priority
	PoolSize    int
	Processor   Processor
	Idempotency *idempotency.Client
	Store       NotificationStore
	Republisher Republisher
	Feed        LiveFeed
	Log         *slog.Logger
}

// defaultRetryBackoff is the documented 2s / 4s / 8s schedule, indexed by
// how many retries the record has already been through.
func defaultRetryBackoff(retryCount int) time.Duration {
	return time.Duration(1<<uint(retryCount+1)) * time.Second
}

// Client exposes the underlying kgo client so tests can inspect offsets.
func (r *Runner) Client() *kgo.Client { return r.client }

// Run blocks until ctx is done. When it returns the client is closed and
// all in-flight records have either committed or been marked failed.
func (r *Runner) Run(ctx context.Context) error {
	sem := make(chan struct{}, r.PoolSize)

	defer func() {
		r.inflight.Wait() // drain in-flight before closing
		// No blanket commit here on purpose. franz-go refreshes its
		// "uncommitted" set on every PollFetches, so committing it would
		// also commit records the handler deliberately left uncommitted for
		// redelivery — a Redis blip during the last batch would silently
		// drop those messages. Records that finished already committed their
		// own offset; anything else has to come back.
		r.client.Close()
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		fetches := r.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				r.Log.Warn("poll fetches error", "topic", e.Topic, "err", e.Err)
			}
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				r.client.AllowRebalance()
				return ctx.Err()
			}
			r.inflight.Add(1)
			go func(rec *kgo.Record) {
				defer r.inflight.Done()
				defer func() { <-sem }()
				r.handle(ctx, rec)
			}(rec)
		}
		// Allow rebalance for the next poll cycle now that we've handed off
		// this batch. In-flight goroutines still hold the semaphore.
		r.client.AllowRebalance()
	}
}

func (r *Runner) handle(ctx context.Context, rec *kgo.Record) {
	start := time.Now()
	channelLabel := string(r.Channel)

	// Retry backoff is a deliberate sleep, not work. Counting it would make
	// the processing histogram measure the backoff schedule instead of the
	// critical section — an 8s wait alone overflows the 5s top bucket, and a
	// p99 landing in +Inf is not even JSON-encodable.
	var backoffSpent time.Duration
	defer func() {
		spent := (time.Since(start) - backoffSpent).Seconds()
		if spent < 0 {
			spent = 0
		}
		metrics.StageProcessingDuration.WithLabelValues(channelLabel).Observe(spent)
		metrics.ProcessDuration.WithLabelValues(channelLabel).Observe(spent)
	}()

	msgID := headerValue(rec, kbroker.HeaderMsgID)
	if msgID == "" {
		msgID = string(rec.Key) // fallback
	}

	// End-to-end lag histogram. Uses the original publish time so retries
	// don't reset the clock — that's the "lag < 1.5s" figure the RUNBOOK
	// points at.
	if producedAt := headerValue(rec, kbroker.HeaderProducedAt); producedAt != "" {
		if ns, err := strconv.ParseInt(producedAt, 10, 64); err == nil {
			metrics.EventE2ELag.WithLabelValues(channelLabel).Observe(time.Since(time.Unix(0, ns)).Seconds())
		}
	}

	// Idempotency check — Redis SETNX with 24h TTL, scoped per channel so
	// a fan-out event correctly persists once per channel. Second time
	// the same (channel, msg_id) shows up (worker restart, retry), we
	// skip and commit.
	ok, err := r.Idempotency.CheckScoped(ctx, channelLabel, msgID)
	if err != nil {
		r.Log.Error("idempotency check failed", "msg_id", msgID, "err", err)
		// Don't commit — let a later poll retry this record.
		return
	}
	if !ok {
		// The claim is held — but a claim is taken *before* the work, so it
		// only proves some worker started, not that it finished. If the
		// previous holder died between the SETNX and the PostgreSQL write
		// (SIGKILL, OOM, pod eviction), no row exists and skipping here
		// would drop the message silently until the 24h TTL expires.
		//
		// PostgreSQL is the only durable record of completion, so ask it.
		// This costs one primary-key lookup, and only on the duplicate path.
		persisted, err := r.Store.HasNotification(ctx, msgID, channelLabel)
		if err != nil {
			r.Log.Error("confirm duplicate against store", "msg_id", msgID, "err", err)
			// Don't commit — an unverified skip is how messages disappear.
			return
		}
		if persisted {
			metrics.MessagesProcessed.WithLabelValues(channelLabel, "duplicate").Inc()
			r.commit(ctx, rec)
			return
		}
		// Claim without a row: the earlier attempt died mid-flight. Redo it.
		// Two workers racing the same record can both land here and both
		// deliver, which is the correct trade for an at-least-once pipeline —
		// a duplicate notification beats a lost one, and the PG upsert keeps
		// history single-rowed either way.
		metrics.MessagesProcessed.WithLabelValues(channelLabel, "orphaned_claim").Inc()
		r.Log.Warn("idempotency claim held but nothing persisted; reprocessing",
			"msg_id", msgID)
	}

	var event kbroker.Event
	if err := json.Unmarshal(rec.Value, &event); err != nil {
		r.Log.Error("unmarshal failed", "msg_id", msgID, "err", err)
		// Malformed body — permanent failure, DLQ.
		r.sendToDLQ(ctx, rec)
		metrics.MessagesProcessed.WithLabelValues(channelLabel, "dlq").Inc()
		r.commit(ctx, rec)
		return
	}

	retryCount := parseInt(headerValue(rec, kbroker.HeaderRetryCount))

	deliverStart := time.Now()
	outcome := r.Processor.Deliver(ctx, event, rec.Value)
	metrics.StageDeliveryDuration.WithLabelValues(channelLabel).Observe(time.Since(deliverStart).Seconds())

	switch outcome {
	case OutcomeDelivered:
		metrics.MessagesProcessed.WithLabelValues(channelLabel, "delivered").Inc()
		r.recordOutcome(ctx, event, rec.Value, "delivered")
		r.commit(ctx, rec)

	case OutcomeSkipped:
		metrics.MessagesProcessed.WithLabelValues(channelLabel, "no_webhook").Inc()
		r.recordOutcome(ctx, event, rec.Value, "skipped")
		r.commit(ctx, rec)

	case OutcomeTransientError:
		if retryCount >= MaxRetries {
			r.Log.Warn("retry budget exhausted, routing to DLQ",
				"msg_id", msgID, "attempts", retryCount+1)
			r.sendToDLQ(ctx, rec)
			metrics.MessagesProcessed.WithLabelValues(channelLabel, "dlq").Inc()
			r.recordOutcome(ctx, event, rec.Value, "dlq")
			r.commit(ctx, rec)
			return
		}
		metrics.MessagesProcessed.WithLabelValues(channelLabel, "failed").Inc()
		// Record the failed attempt. SaveNotification upserts on
		// (message_id, channel), so a later success or DLQ overwrites this
		// — but until then the message is visible in history instead of
		// disappearing between attempts.
		r.recordOutcome(ctx, event, rec.Value, "failed")

		// Release the idempotency claim before re-producing. The claim
		// covers one attempt; the retry carries the same message_id into
		// the same lane, so holding the claim would make it fail the dedup
		// check and be dropped as a duplicate instead of redelivered. This
		// also covers the two paths below that return without committing —
		// the record is refetched and has to be let through again.
		r.releaseClaim(ctx, channelLabel, msgID)

		// Exponential backoff: 2s, 4s, 8s. Backoff happens in-process so
		// the semaphore slot is held — this naturally throttles retries.
		waitStart := time.Now()
		select {
		case <-time.After(r.backoff(retryCount)):
		case <-ctx.Done():
			backoffSpent += time.Since(waitStart)
			return
		}
		backoffSpent += time.Since(waitStart)
		if err := r.Republisher.Retry(ctx, rec, retryCount+1); err != nil {
			r.Log.Error("retry re-produce failed", "msg_id", msgID, "err", err)
			// Leave uncommitted — a later poll will refetch and retry.
			return
		}
		r.commit(ctx, rec)

	case OutcomePermanentError:
		r.sendToDLQ(ctx, rec)
		metrics.MessagesProcessed.WithLabelValues(channelLabel, "dlq").Inc()
		r.recordOutcome(ctx, event, rec.Value, "dlq")
		r.commit(ctx, rec)
	}
}

func (r *Runner) sendToDLQ(ctx context.Context, rec *kgo.Record) {
	dlq := kbroker.DLQTopic(r.Channel, r.Priority)
	if err := r.Republisher.DLQ(ctx, rec, dlq); err != nil {
		r.Log.Error("DLQ produce failed", "topic", dlq, "err", err)
	}
}

// releaseClaim drops the per-attempt idempotency entry. It runs on a
// detached context: the claim has to be released even when ctx is already
// cancelled by shutdown, otherwise the record is refetched after restart
// and skipped as a duplicate.
func (r *Runner) releaseClaim(ctx context.Context, scope, msgID string) {
	relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := r.Idempotency.Release(relCtx, scope, msgID); err != nil {
		// A stuck claim means the retry gets swallowed as a duplicate, so
		// this is error-level even though there is nothing to recover here.
		r.Log.Error("release idempotency claim", "msg_id", msgID, "err", err)
	}
}

func (r *Runner) commit(ctx context.Context, rec *kgo.Record) {
	// CommitRecords is synchronous — returns after broker confirms the
	// offset commit. This is the boundary that makes the whole pipeline
	// at-least-once: crash before this line → same record redelivered.
	if err := r.committer.CommitRecords(ctx, rec); err != nil {
		r.Log.Warn("commit record", "offset", rec.Offset, "err", err)
	}
}

// recordOutcome writes the verdict to both durable history (PostgreSQL) and
// the live dashboard feed. The two always move together — every state a row
// takes is a state the operator watching /live should see.
func (r *Runner) recordOutcome(ctx context.Context, event kbroker.Event, body []byte, status string) {
	err := r.Store.SaveNotification(ctx, store.Notification{
		MessageID: event.MessageID,
		Channel:   string(r.Channel),
		EventType: event.Type,
		Status:    status,
		Priority:  event.Priority,
		Payload:   body,
	})
	if err != nil {
		r.Log.Error("persist notification", "msg_id", event.MessageID, "err", err)
	}

	if r.Feed == nil {
		return
	}
	r.Feed.Publish(ctx, wsfeed.Envelope{
		MessageID: event.MessageID,
		Type:      event.Type,
		Priority:  event.Priority,
		Channel:   string(r.Channel),
		Status:    status,
		Payload:   event.Payload,
		Timestamp: event.Timestamp,
	})
}

// PoolSizesFor returns [pool, pool/2, pool/4] mirroring the AMQP QoS
// prefetch schedule from the original workers — high priority gets more
// slots than normal, which gets more than low.
func PoolSizesFor(pool int) map[kbroker.Priority]int {
	if pool <= 0 {
		pool = 1
	}
	return map[kbroker.Priority]int{
		kbroker.PriorityHigh:   pool,
		kbroker.PriorityNormal: max2(pool/2, 1),
		kbroker.PriorityLow:    max2(pool/4, 1),
	}
}

func headerValue(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func parseInt(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func max2(a, b int) int {
	if a > b {
		return a
	}
	return b
}
