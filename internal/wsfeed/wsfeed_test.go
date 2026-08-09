package wsfeed_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"nexus/internal/wsfeed"
)

type captureSink struct {
	mu   sync.Mutex
	msgs [][]byte
}

func (c *captureSink) Broadcast(msg []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, append([]byte(nil), msg...))
}

func (c *captureSink) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.msgs)
}

func (c *captureSink) first() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.msgs) == 0 {
		return nil
	}
	return c.msgs[0]
}

func newRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

// The worker and producer are separate processes; this is the hop that
// carries a delivery event between them. Before it existed the worker
// broadcast into an in-process hub with no HTTP server attached, so /ws
// clients received nothing at all.
func TestPublisherToBridgeRoundTrip(t *testing.T) {
	rdb := newRedis(t)
	sink := &captureSink{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = wsfeed.NewBridge(rdb, sink, quietLogger()).Run(ctx) }()

	// Let the subscription establish before publishing — pub/sub drops
	// messages sent with no subscriber attached.
	time.Sleep(150 * time.Millisecond)

	pub := wsfeed.NewPublisher(rdb, quietLogger())
	defer pub.Close()

	sent := wsfeed.Envelope{
		MessageID: "msg-1",
		Type:      "payment.completed",
		Priority:  "high",
		Channel:   "webhook",
		Status:    "dlq",
		Payload:   map[string]any{"amount": float64(42)},
		Timestamp: time.Now().UTC().Truncate(time.Millisecond),
	}
	pub.Publish(ctx, sent)

	waitFor(t, func() bool { return sink.count() == 1 }, "the envelope to reach the sink")

	var got wsfeed.Envelope
	if err := json.Unmarshal(sink.first(), &got); err != nil {
		t.Fatalf("unmarshal forwarded payload: %v", err)
	}
	if got.MessageID != sent.MessageID || got.Type != sent.Type {
		t.Errorf("identity fields lost: %+v", got)
	}
	// channel and status are the whole reason this envelope exists rather
	// than the raw event — the UI filters on them.
	if got.Channel != "webhook" || got.Status != "dlq" {
		t.Errorf("channel/status = %q/%q, want webhook/dlq", got.Channel, got.Status)
	}
	if got.Priority != "high" {
		t.Errorf("priority = %q, want high", got.Priority)
	}
	if got.Payload["amount"] != float64(42) {
		t.Errorf("payload not carried through: %+v", got.Payload)
	}
}

// Every channel must appear on the feed, not just in-app: the /live page
// filters by channel and previously only ever saw one value.
func TestBridgeForwardsEveryChannel(t *testing.T) {
	rdb := newRedis(t)
	sink := &captureSink{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = wsfeed.NewBridge(rdb, sink, quietLogger()).Run(ctx) }()
	time.Sleep(150 * time.Millisecond)

	pub := wsfeed.NewPublisher(rdb, quietLogger())
	defer pub.Close()

	for _, ch := range []string{"email", "inapp", "webhook"} {
		pub.Publish(ctx, wsfeed.Envelope{MessageID: "m", Channel: ch, Status: "delivered"})
	}

	waitFor(t, func() bool { return sink.count() == 3 }, "three envelopes")

	seen := map[string]bool{}
	sink.mu.Lock()
	for _, raw := range sink.msgs {
		var env wsfeed.Envelope
		if err := json.Unmarshal(raw, &env); err == nil {
			seen[env.Channel] = true
		}
	}
	sink.mu.Unlock()

	for _, ch := range []string{"email", "inapp", "webhook"} {
		if !seen[ch] {
			t.Errorf("channel %q never reached the feed", ch)
		}
	}
}

// The feed is a dashboard, not a delivery channel: neither publishing nor
// shutdown may be held hostage by Redis.
func TestPublisherToleratesRedisBeingDown(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	mr.Close() // every publish now fails

	pub := wsfeed.NewPublisher(rdb, quietLogger())

	t.Run("Publish does not block the delivery path", func(t *testing.T) {
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < 5000; i++ {
				pub.Publish(context.Background(), wsfeed.Envelope{MessageID: "m", Channel: "email"})
			}
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Publish blocked with Redis unavailable — delivery would stall")
		}
		if pub.Dropped() == 0 {
			t.Error("expected frames to be dropped once the buffer filled")
		}
	})

	t.Run("Close gives up on the backlog instead of stalling shutdown", func(t *testing.T) {
		// The buffer is full of sends that each burn publishTimeout against a
		// dead Redis; draining it would take minutes and blow the SIGTERM
		// budget.
		start := time.Now()
		pub.Close()
		if elapsed := time.Since(start); elapsed > 6*time.Second {
			t.Fatalf("Close took %s — shutdown would be killed before it returns", elapsed)
		}
	})

	// Close is called again by the usual defer in production code.
	pub.Close()
}
