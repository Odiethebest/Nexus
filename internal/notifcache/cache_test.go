package notifcache

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"

	"nexus/internal/metrics"
)

// This mini-suite covers the interesting bits without spinning up
// PostgreSQL: the counter side of cache-aside. Actual PG-backed hits are
// exercised in the integration test in Step 7.

func TestCacheHitBumpsByIDCounter(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	// Pre-fill Redis so we hit without needing a store.
	if err := rdb.Set(ctx, "cache:notif:v2:m1", `[]`, TTLByID).Err(); err != nil {
		t.Fatalf("preload: %v", err)
	}
	c := &Cache{rdb: rdb, store: nil}

	before := counterValue(t, metrics.CacheHits.WithLabelValues(string(ScopeByID)))
	if _, err := c.GetByMessageID(ctx, "m1"); err != nil {
		t.Fatalf("GetByMessageID: %v", err)
	}
	after := counterValue(t, metrics.CacheHits.WithLabelValues(string(ScopeByID)))
	if after != before+1 {
		t.Errorf("hit counter: before=%v after=%v", before, after)
	}
}

func TestCacheHitOnListScope(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	if err := rdb.Set(ctx, "cache:notif:list:v2:50", `[]`, TTLList).Err(); err != nil {
		t.Fatalf("preload: %v", err)
	}
	c := &Cache{rdb: rdb, store: nil}

	before := counterValue(t, metrics.CacheHits.WithLabelValues(string(ScopeList)))
	if _, err := c.ListNotifications(ctx, 50); err != nil {
		t.Fatalf("ListNotifications: %v", err)
	}
	if got := counterValue(t, metrics.CacheHits.WithLabelValues(string(ScopeList))); got != before+1 {
		t.Errorf("list hit counter: before=%v after=%v", before, got)
	}
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("counter Write: %v", err)
	}
	if m.Counter == nil || m.Counter.Value == nil {
		return 0
	}
	return *m.Counter.Value
}
