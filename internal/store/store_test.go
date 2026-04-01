//go:build integration

package store_test

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"nexus/internal/store"
)

func setupStore(t *testing.T) *store.Store {
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

	st, err := store.New(dsn)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
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
