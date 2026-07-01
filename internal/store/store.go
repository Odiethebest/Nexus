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

// Migrate creates the notifications table and index if they don't exist.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS notifications (
			message_id  TEXT        NOT NULL,
			channel     TEXT        NOT NULL,
			event_type  TEXT        NOT NULL,
			status      TEXT        NOT NULL,
			payload     JSONB,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (message_id, channel)
		);
		CREATE INDEX IF NOT EXISTS notifications_created_at_idx
			ON notifications (created_at DESC);
	`)
	if err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	return nil
}

// SaveNotification upserts a delivery record.
func (s *Store) SaveNotification(ctx context.Context, n Notification) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO notifications (message_id, channel, event_type, status, payload)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (message_id, channel) DO UPDATE
			SET status = EXCLUDED.status
	`, n.MessageID, n.Channel, n.EventType, n.Status, n.Payload)
	if err != nil {
		return fmt.Errorf("store: save notification: %w", err)
	}
	return nil
}

// ListNotifications returns the most recent notifications up to limit.
func (s *Store) ListNotifications(ctx context.Context, limit int) ([]Notification, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT message_id, channel, event_type, status, payload, created_at
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
		if err := rows.Scan(
			&n.MessageID, &n.Channel, &n.EventType,
			&n.Status, &n.Payload, &n.CreatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, n)
	}
	return result, rows.Err()
}

// GetByMessageID returns every persisted delivery row for a single
// message_id, one per channel it was fanned out to. The result is small
// (at most 3 rows in the current design) so callers can cache it whole
// under cache:notif:{id}.
func (s *Store) GetByMessageID(ctx context.Context, messageID string) ([]Notification, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT message_id, channel, event_type, status, payload, created_at
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
		if err := rows.Scan(&n.MessageID, &n.Channel, &n.EventType, &n.Status, &n.Payload, &n.CreatedAt); err != nil {
			return nil, err
		}
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
