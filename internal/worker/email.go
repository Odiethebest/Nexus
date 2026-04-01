package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
	"nexus/internal/broker"
	"nexus/internal/idempotency"
	"nexus/internal/store"
)

const (
	EmailQueue    = "nexus.email"
	EmailDLQ      = "nexus.email.dlq"
	EmailPoolSize = 10
)

// EmailWorker consumes from the email queue and delivers email notifications.
type EmailWorker struct {
	ch          *amqp.Channel
	idempotency *idempotency.Client
	store       *store.Store
	poolSize    int
}

// NewEmailWorker declares the queue/DLQ and returns a ready worker.
func NewEmailWorker(ch *amqp.Channel, idem *idempotency.Client, st *store.Store, poolSize int) (*EmailWorker, error) {
	if poolSize <= 0 {
		poolSize = EmailPoolSize
	}
	w := &EmailWorker{ch: ch, idempotency: idem, store: st, poolSize: poolSize}
	if err := w.setup(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *EmailWorker) setup() error {
	if _, err := w.ch.QueueDeclare(EmailDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("email worker: declare dlq: %w", err)
	}

	args := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": EmailDLQ,
	}
	if _, err := w.ch.QueueDeclare(EmailQueue, true, false, false, false, args); err != nil {
		return fmt.Errorf("email worker: declare queue: %w", err)
	}

	if err := w.ch.QueueBind(EmailQueue, "event.*.*", broker.ExchangeName, false, nil); err != nil {
		return fmt.Errorf("email worker: bind queue: %w", err)
	}

	return nil
}

// Run starts consuming messages with a bounded goroutine pool.
func (w *EmailWorker) Run(ctx context.Context) error {
	msgs, err := w.ch.Consume(EmailQueue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("email worker: consume: %w", err)
	}

	sem := make(chan struct{}, w.poolSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("email worker: channel closed")
			}
			sem <- struct{}{}
			go func(d amqp.Delivery) {
				defer func() { <-sem }()
				w.process(ctx, d)
			}(msg)
		}
	}
}

func (w *EmailWorker) process(ctx context.Context, d amqp.Delivery) {
	ok, err := w.idempotency.Check(ctx, d.MessageId)
	if err != nil {
		slog.Error("email: idempotency check failed", "msg_id", d.MessageId, "err", err)
		d.Nack(false, true)
		return
	}
	if !ok {
		slog.Info("email: duplicate message, skipping", "msg_id", d.MessageId)
		d.Ack(false)
		return
	}

	var event broker.Event
	if err := json.Unmarshal(d.Body, &event); err != nil {
		slog.Error("email: unmarshal failed", "err", err)
		d.Nack(false, false)
		return
	}

	// TODO: integrate SMTP / transactional email provider
	slog.Info("email: delivering notification", "msg_id", event.MessageID, "type", event.Type)

	if err := w.store.SaveNotification(ctx, store.Notification{
		MessageID: event.MessageID,
		Channel:   "email",
		EventType: event.Type,
		Status:    "delivered",
		Payload:   d.Body,
	}); err != nil {
		slog.Error("email: persist failed", "err", err)
	}

	d.Ack(false)
}
