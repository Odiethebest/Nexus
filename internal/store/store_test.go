//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"nexus/internal/store"
)

// startPostgres boots a container and returns its DSN, without migrating —
// the migration test needs to seed a pre-migration table first.
func startPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	pgc, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("nexus_test"),
		postgres.WithUsername("nexus"),
		postgres.WithPassword("nexus"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { pgc.Terminate(ctx) })

	dsn, err := pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	return dsn
}

func setupStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.New(startPostgres(t))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return st
}

func TestStore_SaveAndList(t *testing.T) {
	st := setupStore(t)
	ctx := context.Background()

	n := store.Notification{
		MessageID: "msg-001",
		Channel:   "email",
		EventType: "order",
		Status:    "delivered",
		Payload:   []byte(`{"user_id":"u1"}`),
	}
	if err := st.SaveNotification(ctx, n); err != nil {
		t.Fatal(err)
	}

	rows, err := st.ListNotifications(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].MessageID != "msg-001" {
		t.Errorf("message_id: got %s, want msg-001", rows[0].MessageID)
	}
}

func TestStore_Upsert_UpdatesStatus(t *testing.T) {
	st := setupStore(t)
	ctx := context.Background()

	base := store.Notification{
		MessageID: "msg-002",
		Channel:   "email",
		EventType: "order",
		Payload:   []byte(`{}`),
	}

	base.Status = "pending"
	if err := st.SaveNotification(ctx, base); err != nil {
		t.Fatal(err)
	}
	base.Status = "delivered"
	if err := st.SaveNotification(ctx, base); err != nil {
		t.Fatal(err)
	}

	rows, err := st.ListNotifications(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after upsert, got %d", len(rows))
	}
	if rows[0].Status != "delivered" {
		t.Errorf("status: got %s, want delivered", rows[0].Status)
	}
}

func TestStore_ListNotifications_OrderedByCreatedAtDesc(t *testing.T) {
	st := setupStore(t)
	ctx := context.Background()

	for _, id := range []string{"first", "second", "third"} {
		if err := st.SaveNotification(ctx, store.Notification{
			MessageID: id, Channel: "email", EventType: "order",
			Status: "delivered", Payload: []byte(`{}`),
		}); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := st.ListNotifications(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[0].MessageID != "third" {
		t.Errorf("expected 'third' first, got %s", rows[0].MessageID)
	}
}

func TestStore_ListNotifications_RespectsLimit(t *testing.T) {
	st := setupStore(t)
	ctx := context.Background()

	for i := range 5 {
		st.SaveNotification(ctx, store.Notification{
			MessageID: string(rune('a' + i)), Channel: "email",
			EventType: "order", Status: "delivered", Payload: []byte(`{}`),
		})
	}

	rows, err := st.ListNotifications(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Errorf("expected 3 rows with limit=3, got %d", len(rows))
	}
}

func TestStore_SaveAndGet_RoundTripsPriority(t *testing.T) {
	st := setupStore(t)
	ctx := context.Background()

	if err := st.SaveNotification(ctx, store.Notification{
		MessageID: "msg-prio",
		Channel:   "webhook",
		EventType: "alert.critical",
		Status:    "delivered",
		Priority:  "high",
		Payload:   []byte(`{"priority":"high"}`),
	}); err != nil {
		t.Fatal(err)
	}

	rows, err := st.GetByMessageID(ctx, "msg-prio")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Priority != "high" {
		t.Fatalf("GetByMessageID priority = %+v, want high", rows)
	}

	list, err := st.ListNotifications(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Priority != "high" {
		t.Fatalf("ListNotifications priority = %+v, want high", list)
	}
}

// An empty priority would satisfy NOT NULL but is worse than the documented
// default, so the store normalises it.
func TestStore_SaveNotification_DefaultsEmptyPriority(t *testing.T) {
	st := setupStore(t)
	ctx := context.Background()

	if err := st.SaveNotification(ctx, store.Notification{
		MessageID: "msg-noprio",
		Channel:   "email",
		EventType: "order",
		Status:    "delivered",
	}); err != nil {
		t.Fatal(err)
	}

	rows, err := st.GetByMessageID(ctx, "msg-noprio")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Priority != "normal" {
		t.Fatalf("priority = %+v, want normal", rows)
	}
}

// The migration has to run against tables that predate the column, and it
// must recover each row's real priority from the stored event envelope
// rather than stamping everything 'normal'.
func TestStore_Migrate_BackfillsPriorityFromPayload(t *testing.T) {
	ctx := context.Background()
	dsn := startPostgres(t)

	// Build the pre-migration table and seed rows the old code would have
	// written: no priority column, full event envelope in payload.
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.ExecContext(ctx, `
		CREATE TABLE notifications (
			message_id  TEXT        NOT NULL,
			channel     TEXT        NOT NULL,
			event_type  TEXT        NOT NULL,
			status      TEXT        NOT NULL,
			payload     JSONB,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (message_id, channel)
		);
		INSERT INTO notifications (message_id, channel, event_type, status, payload) VALUES
			('old-high',    'email',   'a', 'delivered', '{"priority":"high"}'),
			('old-low',     'inapp',   'b', 'delivered', '{"priority":"low"}'),
			('old-bogus',   'webhook', 'c', 'skipped',   '{"priority":"wat"}'),
			('old-nopayld', 'email',   'd', 'delivered', NULL);
	`); err != nil {
		t.Fatalf("seed pre-migration table: %v", err)
	}

	st, err := store.New(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate over an existing table: %v", err)
	}
	// Idempotent: both services run this on every boot.
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	want := map[string]string{
		"old-high":    "high",   // recovered from payload
		"old-low":     "low",    // recovered from payload
		"old-bogus":   "normal", // unrecognised value must not poison the column
		"old-nopayld": "normal", // nothing to recover from
	}
	for id, wantPriority := range want {
		rows, err := st.GetByMessageID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 {
			t.Fatalf("%s: got %d rows, want 1", id, len(rows))
		}
		if rows[0].Priority != wantPriority {
			t.Errorf("%s: priority = %q, want %q", id, rows[0].Priority, wantPriority)
		}
	}

	// NOT NULL must hold after the backfill.
	var nulls int
	if err := raw.QueryRowContext(ctx,
		`SELECT count(*) FROM notifications WHERE priority IS NULL`).Scan(&nulls); err != nil {
		t.Fatal(err)
	}
	if nulls != 0 {
		t.Errorf("%d rows left with NULL priority", nulls)
	}
}

// Regression: the producer and the worker both run Migrate at boot. Without
// the advisory lock, concurrent CREATE TABLE IF NOT EXISTS races on
// pg_type_typname_nsp_index and one caller dies with SQLSTATE 23505 — which
// crash-looped the worker on every cold start against an empty database.
func TestStore_Migrate_IsSafeUnderConcurrency(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	const racers = 6
	errs := make(chan error, racers)
	release := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := store.New(dsn)
			if err != nil {
				errs <- err
				return
			}
			<-release // let them all pile into the DDL at once
			errs <- st.Migrate(ctx)
		}()
	}
	close(release)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent migrate failed: %v", err)
		}
	}
}
