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
func (c *Client) CheckScoped(ctx context.Context, scope, messageID string) (bool, error) {
	if messageID == "" {
		return true, nil
	}
	var key string
	if scope == "" {
		key = fmt.Sprintf("msg:%s", messageID)
	} else {
		key = fmt.Sprintf("msg:%s:%s", scope, messageID)
	}
	ok, err := c.rdb.SetNX(ctx, key, 1, c.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("idempotency: redis setnx: %w", err)
	}
	return ok, nil
}
