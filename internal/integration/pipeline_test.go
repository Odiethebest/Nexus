//go:build integration

// Package integration tests the full publish → worker → DB pipeline
// end-to-end against a real Redpanda broker, real PostgreSQL, and
// miniredis. Run with:
//
//	go test -tags=integration ./internal/integration/...
//
// Docker must be available (testcontainers spins up Redpanda + PostgreSQL).
package integration_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"

	"nexus/internal/idempotency"
	"nexus/internal/kbroker"
	"nexus/internal/kworker"
	"nexus/internal/store"
)

type pipelineEnv struct {
	cfg   kbroker.Config
	pub   *kbroker.Publisher
	repub *kworker.KafkaRepublisher
	st    *store.Store
	idem  *idempotency.Client
}

func setupPipeline(t *testing.T) *pipelineEnv {
	t.Helper()
	ctx := context.Background()

	// Redpanda — single-node, KRaft, Kafka protocol on random host port.
	rp, err := tcredpanda.Run(ctx, "redpandadata/redpanda:v24.2.5")
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	t.Cleanup(func() { _ = rp.Terminate(ctx) })
	brokers, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("redpanda broker: %v", err)
	}
	t.Setenv("KAFKA_BROKERS", brokers)
	t.Setenv("KAFKA_TOPIC_PARTITIONS", "3") // small for fast local runs
	t.Setenv("KAFKA_REPLICATION_FACTOR", "1")

	cfg, err := kbroker.LoadConfig()
	if err != nil {
		t.Fatalf("load kafka config: %v", err)
	}
	// Create every lane + DLQ topic up-front so consumers don't race with
	// producer topic auto-create in the middle of the test.
	if err := kbroker.EnsureTopics(ctx, cfg); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	// PostgreSQL
	pgc, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("nexus_test"),
		tcpostgres.WithUsername("nexus"),
		tcpostgres.WithPassword("nexus"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pgc.Terminate(ctx) })
	dsn, _ := pgc.ConnectionString(ctx, "sslmode=disable")

	// Redis (miniredis — no Docker needed)
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	pub, err := kbroker.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pub.Close(closeCtx)
	})

	repub, err := kworker.NewKafkaRepublisher(cfg)
	if err != nil {
		t.Fatalf("republisher: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = repub.Close(closeCtx)
	})

	st, err := store.New(dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	return &pipelineEnv{
		cfg:   cfg,
		pub:   pub,
		repub: repub,
		st:    st,
		idem:  idempotency.New(rdb),
	}
}

// flakyProcessor reports a transient failure for its first `failures`
// deliveries and succeeds afterwards — a downstream that is briefly
// unavailable, which is what the retry budget exists for.
type flakyProcessor struct {
	channel  kbroker.Channel
	failures int

	mu    sync.Mutex
	calls int
}

func (p *flakyProcessor) Channel() kbroker.Channel { return p.channel }

func (p *flakyProcessor) Deliver(context.Context, kbroker.Event, []byte) kworker.Outcome {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls <= p.failures {
		return kworker.OutcomeTransientError
	}
	return kworker.OutcomeDelivered
}

func (p *flakyProcessor) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// waitForStatus polls until the (message_id, channel) row reaches want, or
// the deadline passes. Returns the last status seen so failures are
// diagnosable.
func waitForStatus(t *testing.T, st *store.Store, msgID, channel, want string, timeout time.Duration) (string, bool) {
	t.Helper()
	last := "<no row>"
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := st.GetByMessageID(context.Background(), msgID)
		if err != nil {
			t.Fatalf("get by message_id: %v", err)
		}
		for _, r := range rows {
			if r.Channel != channel {
				continue
			}
			last = r.Status
			if r.Status == want {
				return r.Status, true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return last, false
}

// waitForRows polls ListNotifications until at least n rows appear or the
// deadline is exceeded.
func waitForRows(t *testing.T, st *store.Store, n int, timeout time.Duration) []store.Notification {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := st.ListNotifications(context.Background(), 10)
		if err != nil {
			t.Fatalf("list notifications: %v", err)
		}
		if len(rows) >= n {
			return rows
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// runEmailLane spins up an email/high runner in the background for the
// duration of a single test.
func (env *pipelineEnv) runLane(t *testing.T, ctx context.Context, ch kbroker.Channel, p kbroker.Priority, proc kworker.Processor) {
	t.Helper()
	runner, err := kworker.NewRunner(env.cfg, kworker.RunnerOptions{
		Channel:     ch,
		Priority:    p,
		PoolSize:    2,
		Processor:   proc,
		Idempotency: env.idem,
		Store:       env.st,
		Republisher: env.repub,
	})
	if err != nil {
		t.Fatalf("build runner %s/%s: %v", ch, p, err)
	}
	go func() { _ = runner.Run(ctx) }()
}

func TestPipeline_PublishDeliveredToDB(t *testing.T) {
	if _, err := os.Stat("/var/run/docker.sock"); err != nil && os.Getenv("DOCKER_HOST") == "" {
		t.Skip("docker not available")
	}
	env := setupPipeline(t)

	wCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	env.runLane(t, wCtx, kbroker.ChannelEmail, kbroker.PriorityHigh, &kworker.EmailProcessor{})

	msgID, err := env.pub.Publish(context.Background(), "order", "high", map[string]any{"user_id": "u1"})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	rows := waitForRows(t, env.st, 1, 15*time.Second)
	if len(rows) == 0 {
		t.Fatal("timeout: no notification persisted after 15s")
	}
	found := false
	for _, r := range rows {
		if r.MessageID == msgID && r.Channel == "email" {
			found = true
			if r.Status != "delivered" {
				t.Errorf("status: got %s, want delivered", r.Status)
			}
		}
	}
	if !found {
		t.Errorf("email row for %s not found in %d rows", msgID, len(rows))
	}
}

// A transient failure must survive the round-trip through the lane topic
// and be delivered on the retry. This is the end-to-end counterpart to the
// unit tests in kworker: the retry carries the same message_id, so the
// idempotency claim has to be released before the record is re-produced or
// the retry is silently skipped as a duplicate.
func TestPipeline_TransientFailureIsRetriedThenDelivered(t *testing.T) {
	if _, err := os.Stat("/var/run/docker.sock"); err != nil && os.Getenv("DOCKER_HOST") == "" {
		t.Skip("docker not available")
	}
	env := setupPipeline(t)

	wCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	proc := &flakyProcessor{channel: kbroker.ChannelEmail, failures: 1}
	env.runLane(t, wCtx, kbroker.ChannelEmail, kbroker.PriorityHigh, proc)

	msgID, err := env.pub.Publish(context.Background(), "order", "high", map[string]any{"user_id": "u-retry"})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Attempt 1 fails → 2s backoff → re-produce → attempt 2 delivers.
	status, ok := waitForStatus(t, env.st, msgID, "email", "delivered", 30*time.Second)
	if !ok {
		t.Fatalf("message never reached delivered; last status = %q after %d delivery attempts",
			status, proc.callCount())
	}
	if got := proc.callCount(); got != 2 {
		t.Errorf("Deliver called %d times, want 2 (initial + one retry)", got)
	}
}

func TestPipeline_MultipleWorkers_AllChannelsDeliver(t *testing.T) {
	if _, err := os.Stat("/var/run/docker.sock"); err != nil && os.Getenv("DOCKER_HOST") == "" {
		t.Skip("docker not available")
	}
	env := setupPipeline(t)

	wCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	env.runLane(t, wCtx, kbroker.ChannelEmail, kbroker.PriorityHigh, &kworker.EmailProcessor{})
	env.runLane(t, wCtx, kbroker.ChannelInApp, kbroker.PriorityHigh, &kworker.InAppProcessor{})
	env.runLane(t, wCtx, kbroker.ChannelWebhook, kbroker.PriorityHigh, kworker.NewWebhookProcessor(nil))

	msgID, err := env.pub.Publish(context.Background(), "order", "high", map[string]any{"user_id": "u4"})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	rows := waitForRows(t, env.st, 3, 20*time.Second)
	if len(rows) < 3 {
		t.Fatalf("expected 3 rows (one per channel), got %d", len(rows))
	}
	channels := map[string]bool{}
	for _, r := range rows {
		if r.MessageID == msgID {
			channels[r.Channel] = true
		}
	}
	for _, ch := range []string{"email", "inapp", "webhook"} {
		if !channels[ch] {
			t.Errorf("channel %q did not deliver notification", ch)
		}
	}
}
