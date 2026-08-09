package idempotency

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultTTL = 24 * time.Hour

// Client performs Redis-backed deduplication using SET NX.
type Client struct {
	rdb *redis.Client
	ttl time.Duration
}

// New creates a Client with a 24-hour deduplication window.
func New(rdb *redis.Client) *Client {
	return &Client{rdb: rdb, ttl: defaultTTL}
}

// Check returns true (first seen) and marks the message ID as processed.
// Returns false without error if the ID has already been seen.
// An empty messageID is always allowed through.
//
// Deprecated: prefer CheckScoped when the caller has a channel context —
// otherwise a single fan-out event marks itself "seen" in one channel and
// gets skipped in the other two. Kept for callers that legitimately have
// no scope (e.g. one-shot admin tools).
func (c *Client) Check(ctx context.Context, messageID string) (bool, error) {
	return c.CheckScoped(ctx, "", messageID)
}

// CheckScoped is Check with a scope namespace (typically the delivery
// channel). Same message_id landing in email vs inapp gets two
// independent SetNX entries, so a fan-out event correctly persists once
// per channel and worker restarts still dedupe within a scope.
//
// The entry it writes is a claim on the *attempt*, not a record of
// success. A caller that ends an attempt without reaching a terminal
// state (delivered / skipped / dead-lettered) must call Release, or the
// next delivery of that message is silently skipped as a duplicate.
func (c *Client) CheckScoped(ctx context.Context, scope, messageID string) (bool, error) {
	if messageID == "" {
		return true, nil
	}
	ok, err := c.rdb.SetNX(ctx, scopedKey(scope, messageID), 1, c.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("idempotency: redis setnx: %w", err)
	}
	return ok, nil
}

// Release drops the claim taken by CheckScoped so the same (scope,
// messageID) can be processed again. Used when an attempt is going to be
// retried: the retry carries the same message_id, so without a release it
// would fail the dedup check and be dropped instead of redelivered.
//
// Deleting a key that isn't there is not an error — Release is safe to
// call on paths where the claim may never have been taken.
func (c *Client) Release(ctx context.Context, scope, messageID string) error {
	if messageID == "" {
		return nil
	}
	if err := c.rdb.Del(ctx, scopedKey(scope, messageID)).Err(); err != nil {
		return fmt.Errorf("idempotency: redis del: %w", err)
	}
	return nil
}

func scopedKey(scope, messageID string) string {
	if scope == "" {
		return fmt.Sprintf("msg:%s", messageID)
	}
	return fmt.Sprintf("msg:%s:%s", scope, messageID)
}
