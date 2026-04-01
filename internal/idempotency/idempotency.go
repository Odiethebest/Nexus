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
func (c *Client) Check(ctx context.Context, messageID string) (bool, error) {
	if messageID == "" {
		return true, nil
	}

	key := fmt.Sprintf("msg:%s", messageID)
	ok, err := c.rdb.SetNX(ctx, key, 1, c.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("idempotency: redis setnx: %w", err)
	}
	return ok, nil
}
