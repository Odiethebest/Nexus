package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Notification is a persisted delivery record.
type Notification struct {
	MessageID string          `json:"message_id"`
	Channel   string          `json:"channel"`
	EventType string          `json:"event_type"`
	Status    string          `json:"status"`
	Priority  string          `json:"priority"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

// Store persists notification history to PostgreSQL.
type Store struct {
	db *sql.DB
}

// New opens a connection to PostgreSQL and verifies connectivity.
func New(dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{db: db}, nil
}

// Migrate brings the notifications table up to the current shape. It is
// idempotent and runs on every boot of both the producer and the worker.
//
// The whole thing is one multi-statement Exec with no arguments on purpose:
// pgx then uses the simple protocol, which Postgres wraps in a single
// implicit transaction, so the migration is all-or-nothing even with two
// services racing to run it at startup. Adding a bind parameter would
// switch to the extended protocol and silently lose that property.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		-- Serialise migrations across instances. The producer and the worker
		-- both migrate at boot, and CREATE TABLE IF NOT EXISTS is not safe
		-- under concurrency: two sessions can both pass the existence check
		-- and one then dies on pg_type_typname_nsp_index. On a cold start
		-- that crash-looped the worker until the producer won the race.
		-- The lock is transaction-scoped, so it releases on commit.
		SELECT pg_advisory_xact_lock(4823150927364);

		CREATE TABLE IF NOT EXISTS notifications (
			message_id  TEXT        NOT NULL,
			channel     TEXT        NOT NULL,
			event_type  TEXT        NOT NULL,
			status      TEXT        NOT NULL,
			priority    TEXT        NOT NULL DEFAULT 'normal',
			payload     JSONB,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (message_id, channel)
		);
		CREATE INDEX IF NOT EXISTS notifications_created_at_idx
			ON notifications (created_at DESC);

		-- Existing deployments: CREATE TABLE IF NOT EXISTS will not add a
		-- column to a table that already exists, so add it explicitly.
		ALTER TABLE notifications ADD COLUMN IF NOT EXISTS priority TEXT;

		-- Recover the real priority instead of stamping every historical row
		-- 'normal'. payload holds the full event envelope, which carries the
		-- priority the message was published with. Restricting to the three
		-- known lanes also keeps a malformed payload from poisoning the
		-- column.
		UPDATE notifications
		   SET priority = payload->>'priority'
		 WHERE priority IS NULL
		   AND payload->>'priority' IN ('high', 'normal', 'low');

		-- Rows whose payload cannot answer (NULL or malformed) fall back, so
		-- the NOT NULL below cannot fail on existing data.
		UPDATE notifications SET priority = 'normal' WHERE priority IS NULL;

		ALTER TABLE notifications ALTER COLUMN priority SET DEFAULT 'normal';
		ALTER TABLE notifications ALTER COLUMN priority SET NOT NULL;
	`)
	if err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	return nil
}

// SaveNotification upserts a delivery record.
func (s *Store) SaveNotification(ctx context.Context, n Notification) error {
	// The column is NOT NULL and an empty string would be worse than the
	// documented default, so normalise here rather than relying on callers.
	priority := n.Priority
	if priority == "" {
		priority = "normal"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO notifications (message_id, channel, event_type, status, priority, payload)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (message_id, channel) DO UPDATE
			SET status = EXCLUDED.status,
			    priority = EXCLUDED.priority
	`, n.MessageID, n.Channel, n.EventType, n.Status, priority, n.Payload)
	if err != nil {
		return fmt.Errorf("store: save notification: %w", err)
	}
	return nil
}

// HasNotification reports whether a delivery row already exists for this
// (message_id, channel) pair — a primary-key lookup.
//
// This is the durable answer to "was this message already handled?". The
// Redis idempotency entry cannot answer it on its own: that claim is taken
// *before* the work, so its presence only proves some worker started, not
// that it finished.
func (s *Store) HasNotification(ctx context.Context, messageID, channel string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM notifications WHERE message_id = $1 AND channel = $2
		)
	`, messageID, channel).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("store: has notification: %w", err)
	}
	return exists, nil
}

// ListNotifications returns the most recent notifications up to limit.
func (s *Store) ListNotifications(ctx context.Context, limit int) ([]Notification, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT message_id, channel, event_type, status, priority, payload, created_at
		FROM notifications
		ORDER BY created_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list notifications: %w", err)
	}
	defer rows.Close()

	var result []Notification
	for rows.Next() {
		var n Notification
		// payload is nullable, and database/sql cannot scan NULL straight
		// into json.RawMessage. Going via []byte yields a nil payload
		// instead of failing the whole query on one such row.
		var payload []byte
		if err := rows.Scan(
			&n.MessageID, &n.Channel, &n.EventType,
			&n.Status, &n.Priority, &payload, &n.CreatedAt,
		); err != nil {
			return nil, err
		}
		n.Payload = payload
		result = append(result, n)
	}
	return result, rows.Err()
}

// GetByMessageID returns every persisted delivery row for a single
// message_id, one per channel it was fanned out to. The result is small
// (at most 3 rows in the current design) so callers can cache it whole
// under cache:notif:v2:{id}.
func (s *Store) GetByMessageID(ctx context.Context, messageID string) ([]Notification, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT message_id, channel, event_type, status, priority, payload, created_at
		FROM notifications
		WHERE message_id = $1
		ORDER BY channel
	`, messageID)
	if err != nil {
		return nil, fmt.Errorf("store: get by message_id: %w", err)
	}
	defer rows.Close()

	var out []Notification
	for rows.Next() {
		var n Notification
		var payload []byte // see ListNotifications: payload is nullable
		if err := rows.Scan(&n.MessageID, &n.Channel, &n.EventType, &n.Status, &n.Priority, &payload, &n.CreatedAt); err != nil {
			return nil, err
		}
		n.Payload = payload
		out = append(out, n)
	}
	return out, rows.Err()
}

// ClearNotificationsBefore deletes notifications created at or before the
// provided cutoff time and returns the number of deleted rows.
func (s *Store) ClearNotificationsBefore(ctx context.Context, before time.Time) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM notifications
		WHERE created_at <= $1
	`, before.UTC())
	if err != nil {
		return 0, fmt.Errorf("store: clear notifications: %w", err)
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: clear notifications rows affected: %w", err)
	}
	return deleted, nil
}
