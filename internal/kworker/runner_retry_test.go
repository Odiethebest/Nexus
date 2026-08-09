package kworker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"nexus/internal/idempotency"
	"nexus/internal/kbroker"
	"nexus/internal/store"
	"nexus/internal/wsfeed"
)

// The idempotency claim and the retry loop both key off message_id, so
// they have to be tested together: a retry re-produces the *same*
// message_id into the *same* lane, and an over-eager dedup check turns
// every retry into a silently dropped duplicate. These tests drive
// Runner.handle directly and feed the re-produced record back in, which
// is what the real topic round-trip does.

type scriptedProcessor struct {
	outcomes []Outcome // last entry repeats once exhausted
	calls    int
}

func (p *scriptedProcessor) Channel() kbroker.Channel { return kbroker.ChannelWebhook }

func (p *scriptedProcessor) Deliver(context.Context, kbroker.Event, []byte) Outcome {
	idx := p.calls
	p.calls++
	if idx >= len(p.outcomes) {
		idx = len(p.outcomes) - 1
	}
	return p.outcomes[idx]
}

type fakeRepublisher struct {
	retries []*kgo.Record
	dlq     []*kgo.Record
}

// Retry/DLQ build their records through the production cloneWithRetry so
// the header bookkeeping under test is the real thing.
func (f *fakeRepublisher) Retry(_ context.Context, rec *kgo.Record, retryCount int) error {
	f.retries = append(f.retries, cloneWithRetry(rec, rec.Topic, retryCount))
	return nil
}

func (f *fakeRepublisher) DLQ(_ context.Context, rec *kgo.Record, dlqTopic string) error {
	f.dlq = append(f.dlq, cloneWithRetry(rec, dlqTopic, MaxRetries))
	return nil
}

type fakeStore struct{ saved []store.Notification }

func (f *fakeStore) SaveNotification(_ context.Context, n store.Notification) error {
	f.saved = append(f.saved, n)
	return nil
}

func (f *fakeStore) statuses() []string {
	out := make([]string, 0, len(f.saved))
	for _, n := range f.saved {
		out = append(out, n.Status)
	}
	return out
}

type fakeCommitter struct{ commits int }

func (f *fakeCommitter) CommitRecords(context.Context, ...*kgo.Record) error {
	f.commits++
	return nil
}

type fakeFeed struct{ sent []wsfeed.Envelope }

func (f *fakeFeed) Publish(_ context.Context, env wsfeed.Envelope) {
	f.sent = append(f.sent, env)
}

func newTestRunner(t *testing.T, proc Processor) (*Runner, *fakeRepublisher, *fakeStore, *fakeCommitter, *fakeFeed) {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	repub := &fakeRepublisher{}
	st := &fakeStore{}
	com := &fakeCommitter{}
	feed := &fakeFeed{}

	return &Runner{
		Channel:     kbroker.ChannelWebhook,
		Priority:    kbroker.PriorityNormal,
		PoolSize:    1,
		Processor:   proc,
		Idempotency: idempotency.New(rdb),
		Store:       st,
		Republisher: repub,
		Feed:        feed,
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		committer:   com,
		backoff:     func(int) time.Duration { return 0 },
	}, repub, st, com, feed
}

func testRecord(t *testing.T, msgID string, retryCount int) *kgo.Record {
	t.Helper()
	body, err := json.Marshal(kbroker.Event{
		MessageID: msgID,
		Type:      "payment.completed",
		Priority:  string(kbroker.PriorityNormal),
		Payload:   map[string]any{"webhook_url": "http://example.invalid/hook"},
		Timestamp: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return &kgo.Record{
		Topic: kbroker.TopicName(kbroker.ChannelWebhook, kbroker.PriorityNormal),
		Key:   []byte(msgID),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: kbroker.HeaderMsgID, Value: []byte(msgID)},
			{Key: kbroker.HeaderEventType, Value: []byte("payment.completed")},
			{Key: kbroker.HeaderPriority, Value: []byte(kbroker.PriorityNormal)},
			{Key: kbroker.HeaderProducedAt, Value: []byte(strconv.FormatInt(time.Now().UnixNano(), 10))},
			{Key: kbroker.HeaderRetryCount, Value: []byte(strconv.Itoa(retryCount))},
		},
	}
}

// Regression: the retry used to reuse the claim taken on attempt 1, so the
// re-produced record failed the dedup check and was committed as a
// duplicate without ever being delivered.
func TestHandleRetryIsDeliveredNotSwallowedAsDuplicate(t *testing.T) {
	proc := &scriptedProcessor{outcomes: []Outcome{OutcomeTransientError, OutcomeDelivered}}
	r, repub, st, _, _ := newTestRunner(t, proc)
	ctx := context.Background()

	r.handle(ctx, testRecord(t, "msg-retry", 0))
	if len(repub.retries) != 1 {
		t.Fatalf("expected 1 re-produced record, got %d", len(repub.retries))
	}

	// Same message_id, same lane — exactly what comes back off the topic.
	r.handle(ctx, repub.retries[0])

	if proc.calls != 2 {
		t.Fatalf("retry was swallowed: Deliver called %d times, want 2", proc.calls)
	}
	if len(repub.dlq) != 0 {
		t.Fatalf("a recovered retry must not reach the DLQ, got %d records", len(repub.dlq))
	}
	if got := st.statuses(); len(got) != 2 || got[0] != "failed" || got[1] != "delivered" {
		t.Fatalf("persisted statuses = %v, want [failed delivered]", got)
	}
}

// The full budget: MaxRetries re-produces, then the DLQ. Each attempt has
// to actually reach the processor.
func TestHandleExhaustsRetryBudgetThenDLQ(t *testing.T) {
	proc := &scriptedProcessor{outcomes: []Outcome{OutcomeTransientError}}
	r, repub, st, _, _ := newTestRunner(t, proc)
	ctx := context.Background()

	rec := testRecord(t, "msg-dlq", 0)
	for attempt := 0; attempt <= MaxRetries; attempt++ {
		r.handle(ctx, rec)
		if attempt < MaxRetries {
			if len(repub.retries) != attempt+1 {
				t.Fatalf("attempt %d: expected %d re-produced records, got %d",
					attempt, attempt+1, len(repub.retries))
			}
			rec = repub.retries[attempt]
		}
	}

	if proc.calls != MaxRetries+1 {
		t.Fatalf("Deliver called %d times, want %d (initial + %d retries)",
			proc.calls, MaxRetries+1, MaxRetries)
	}
	for i, rr := range repub.retries {
		if got := headerValue(rr, kbroker.HeaderRetryCount); got != strconv.Itoa(i+1) {
			t.Errorf("re-produce %d: x-retry-count = %q, want %q", i, got, strconv.Itoa(i+1))
		}
	}
	if len(repub.dlq) != 1 {
		t.Fatalf("expected exactly 1 DLQ record, got %d", len(repub.dlq))
	}
	if want := kbroker.DLQTopic(kbroker.ChannelWebhook, kbroker.PriorityNormal); repub.dlq[0].Topic != want {
		t.Errorf("DLQ topic = %s, want %s", repub.dlq[0].Topic, want)
	}
	if got := st.statuses(); got[len(got)-1] != "dlq" {
		t.Errorf("final persisted status = %q, want dlq (all: %v)", got[len(got)-1], got)
	}
}

// Releasing the claim on retry must not weaken dedup for a plain
// redelivery — same record, same offset, no retry in between.
func TestHandleDedupesRedeliveryOfSameRecord(t *testing.T) {
	proc := &scriptedProcessor{outcomes: []Outcome{OutcomeDelivered}}
	r, _, st, com, _ := newTestRunner(t, proc)
	ctx := context.Background()

	rec := testRecord(t, "msg-dupe", 0)
	r.handle(ctx, rec)
	r.handle(ctx, rec)

	if proc.calls != 1 {
		t.Fatalf("redelivery should be deduped: Deliver called %d times, want 1", proc.calls)
	}
	if len(st.saved) != 1 {
		t.Fatalf("persisted %d rows, want 1", len(st.saved))
	}
	if com.commits != 2 {
		t.Errorf("commits = %d, want 2 (the duplicate is committed, not left dangling)", com.commits)
	}
}

// A permanent verdict skips the retry loop entirely and keeps its claim.
func TestHandlePermanentErrorGoesStraightToDLQ(t *testing.T) {
	proc := &scriptedProcessor{outcomes: []Outcome{OutcomePermanentError}}
	r, repub, st, _, _ := newTestRunner(t, proc)

	r.handle(context.Background(), testRecord(t, "msg-perm", 0))

	if len(repub.retries) != 0 {
		t.Errorf("permanent failure must not retry, got %d re-produces", len(repub.retries))
	}
	if len(repub.dlq) != 1 {
		t.Fatalf("expected 1 DLQ record, got %d", len(repub.dlq))
	}
	if got := st.statuses(); len(got) != 1 || got[0] != "dlq" {
		t.Errorf("persisted statuses = %v, want [dlq]", got)
	}
}

// The /live feed is driven from the runner, not from InAppProcessor, so the
// envelope must carry this lane's real channel — the UI filters on it and
// used to receive a hardcoded "inapp" for every event.
func TestHandlePublishesFeedEnvelopePerOutcome(t *testing.T) {
	proc := &scriptedProcessor{outcomes: []Outcome{OutcomeTransientError, OutcomeDelivered}}
	r, repub, _, _, feed := newTestRunner(t, proc)
	ctx := context.Background()

	r.handle(ctx, testRecord(t, "msg-feed", 0))
	r.handle(ctx, repub.retries[0])

	if len(feed.sent) != 2 {
		t.Fatalf("published %d envelopes, want 2 (failed then delivered)", len(feed.sent))
	}
	for i, env := range feed.sent {
		if env.Channel != string(kbroker.ChannelWebhook) {
			t.Errorf("envelope %d: channel = %q, want webhook (the runner's lane)", i, env.Channel)
		}
		if env.MessageID != "msg-feed" {
			t.Errorf("envelope %d: message_id = %q", i, env.MessageID)
		}
		if env.Priority != string(kbroker.PriorityNormal) {
			t.Errorf("envelope %d: priority = %q, want normal", i, env.Priority)
		}
	}
	if feed.sent[0].Status != "failed" || feed.sent[1].Status != "delivered" {
		t.Errorf("statuses = %q/%q, want failed/delivered",
			feed.sent[0].Status, feed.sent[1].Status)
	}
}

// A duplicate writes no row, so it must not reach the feed either —
// otherwise the UI shows phantom deliveries during a rebalance.
func TestHandleDoesNotPublishFeedForDuplicate(t *testing.T) {
	proc := &scriptedProcessor{outcomes: []Outcome{OutcomeDelivered}}
	r, _, _, _, feed := newTestRunner(t, proc)
	ctx := context.Background()

	rec := testRecord(t, "msg-dupe-feed", 0)
	r.handle(ctx, rec)
	r.handle(ctx, rec)

	if len(feed.sent) != 1 {
		t.Errorf("published %d envelopes, want 1 — the duplicate must be silent", len(feed.sent))
	}
}

// A nil Feed disables the live stream without breaking delivery.
func TestHandleToleratesNilFeed(t *testing.T) {
	proc := &scriptedProcessor{outcomes: []Outcome{OutcomeDelivered}}
	r, _, st, _, _ := newTestRunner(t, proc)
	r.Feed = nil

	r.handle(context.Background(), testRecord(t, "msg-nofeed", 0))

	if got := st.statuses(); len(got) != 1 || got[0] != "delivered" {
		t.Errorf("persisted statuses = %v, want [delivered]", got)
	}
}

func TestDefaultRetryBackoffSchedule(t *testing.T) {
	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}
	for retryCount, w := range want {
		if got := defaultRetryBackoff(retryCount); got != w {
			t.Errorf("defaultRetryBackoff(%d) = %s, want %s", retryCount, got, w)
		}
	}
}
