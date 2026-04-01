package idempotency_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"nexus/internal/idempotency"
)

func newClient(t *testing.T) *idempotency.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return idempotency.New(rdb)
}

func TestCheck_FirstTime_ReturnsTrue(t *testing.T) {
	c := newClient(t)
	ok, err := c.Check(context.Background(), "msg-001")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected true for first-time message ID")
	}
}

func TestCheck_Duplicate_ReturnsFalse(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	c.Check(ctx, "msg-002") // first — mark as seen
	ok, err := c.Check(ctx, "msg-002")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("expected false for duplicate message ID")
	}
}

func TestCheck_EmptyID_AlwaysAllows(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	for i := range 3 {
		ok, err := c.Check(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("call %d: expected true for empty message ID", i+1)
		}
	}
}

func TestCheck_IndependentIDs_EachAllowedOnce(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		ok, err := c.Check(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("id %q: expected true on first check", id)
		}
	}
}

func TestCheck_TTLExpiry_AllowsRedelivery(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	c := idempotency.New(rdb)
	ctx := context.Background()

	c.Check(ctx, "msg-ttl")

	// Fast-forward time past the 24h TTL in miniredis
	mr.FastForward(25 * time.Hour)

	ok, err := c.Check(ctx, "msg-ttl")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected true after TTL expiry")
	}
}
