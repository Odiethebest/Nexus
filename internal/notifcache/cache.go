// Package notifcache implements the cache-aside path in front of the
// notifications store. The primary access pattern for the /notifications
// endpoints in a running system is "look up by message_id" (a user just
// received a notification and the UI wants to render details), so
// repeated reads of the same message_id are the load characteristic that
// makes caching pay off. That is what the RUNBOOK's "95% hit rate (by_id)"
// number measures.
package notifcache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"nexus/internal/metrics"
	"nexus/internal/store"
)

// Scope labels which cache the counters attribute a hit/miss to. Ordered
// so the strings match the Prometheus label values.
type Scope string

const (
	ScopeByID Scope = "by_id"
	ScopeList Scope = "list"
)

// TTLs are tuned to the semantics of each path. by_id data is per-record
// and only becomes stale when a worker changes status (rare after initial
// dispatch) — a full minute is fine. The list endpoint is inherently
// racy against inserts, so 2s just smooths burst reads without hiding
// meaningful updates.
const (
	TTLByID = 60 * time.Second
	TTLList = 2 * time.Second
)

// Cache wraps a Redis client + the underlying store to implement the
// cache-aside read pattern.
type Cache struct {
	rdb   *redis.Client
	store *store.Store
}

// New constructs a Cache from a Redis client and a Store.
func New(rdb *redis.Client, st *store.Store) *Cache {
	return &Cache{rdb: rdb, store: st}
}

// GetByMessageID is the hot-path read: check Redis first, fall through to
// PostgreSQL on miss and write-through the result with TTLByID. Cache
// counters are bumped per lookup so the RUNBOOK's hit-rate metric is
// exactly rate(hits) / (rate(hits) + rate(misses)) for scope="by_id".
func (c *Cache) GetByMessageID(ctx context.Context, id string) ([]store.Notification, error) {
	// The version segment is bumped whenever the cached struct changes
	// shape. Without it, entries written by the previous build deserialise
	// with the new field zeroed — after the priority column was added that
	// meant a blank badge in the UI for a full TTL after every deploy.
	key := "cache:notif:v2:" + id

	if raw, err := c.rdb.Get(ctx, key).Bytes(); err == nil {
		var cached []store.Notification
		if jsonErr := json.Unmarshal(raw, &cached); jsonErr == nil {
			metrics.CacheHits.WithLabelValues(string(ScopeByID)).Inc()
			return cached, nil
		}
		// Fall through to a fresh DB read if the cached value was corrupt.
	} else if !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("notifcache: redis get: %w", err)
	}

	metrics.CacheMisses.WithLabelValues(string(ScopeByID)).Inc()

	rows, err := c.store.GetByMessageID(ctx, id)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		rows = []store.Notification{}
	}
	if body, err := json.Marshal(rows); err == nil {
		// Fire-and-forget refill — a Redis outage must not break the read
		// path, so we swallow this error after logging via the scope
		// counter (missed refill → next call is another miss).
		_ = c.rdb.Set(ctx, key, body, TTLByID).Err()
	}
	return rows, nil
}

// ListNotifications caches the list endpoint under a very short TTL. This
// is a working-engineering nicety — the RUNBOOK's 95% figure is by_id, not
// list. Two seconds is long enough to absorb spikes from a polling
// dashboard without hiding fresh writes for meaningful periods.
func (c *Cache) ListNotifications(ctx context.Context, limit int) ([]store.Notification, error) {
	key := fmt.Sprintf("cache:notif:list:v2:%d", limit)

	if raw, err := c.rdb.Get(ctx, key).Bytes(); err == nil {
		var cached []store.Notification
		if jsonErr := json.Unmarshal(raw, &cached); jsonErr == nil {
			metrics.CacheHits.WithLabelValues(string(ScopeList)).Inc()
			return cached, nil
		}
	} else if !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("notifcache: redis get list: %w", err)
	}

	metrics.CacheMisses.WithLabelValues(string(ScopeList)).Inc()

	rows, err := c.store.ListNotifications(ctx, limit)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		rows = []store.Notification{}
	}
	if body, err := json.Marshal(rows); err == nil {
		_ = c.rdb.Set(ctx, key, body, TTLList).Err()
	}
	return rows, nil
}
