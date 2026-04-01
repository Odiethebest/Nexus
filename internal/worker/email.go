package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"nexus/internal/broker"
	"nexus/internal/idempotency"
	"nexus/internal/mailer"
	"nexus/internal/metrics"
	"nexus/internal/store"
)

const (
	EmailQueue    = "nexus.email"
	EmailDLQ      = "nexus.email.dlq"
	EmailPoolSize = 10
)

// EmailWorker consumes from the email queue and delivers email notifications.
// If m is nil the worker skips actual sending (useful when SMTP is not configured).
type EmailWorker struct {
	ch          *amqp.Channel
	mailer      *mailer.Mailer
	idempotency *idempotency.Client
	store       *store.Store
	poolSize    int
}

// NewEmailWorker declares the queue/DLQ and returns a ready worker.
func NewEmailWorker(ch *amqp.Channel, m *mailer.Mailer, idem *idempotency.Client, st *store.Store, poolSize int) (*EmailWorker, error) {
	if poolSize <= 0 {
		poolSize = EmailPoolSize
	}
	w := &EmailWorker{ch: ch, mailer: m, idempotency: idem, store: st, poolSize: poolSize}
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
	start := time.Now()
	defer func() { metrics.ProcessDuration.WithLabelValues("email").Observe(time.Since(start).Seconds()) }()

	ok, err := w.idempotency.Check(ctx, d.MessageId)
	if err != nil {
		slog.Error("email: idempotency check failed", "msg_id", d.MessageId, "err", err)
		d.Nack(false, true)
		return
	}
	if !ok {
		slog.Info("email: duplicate message, skipping", "msg_id", d.MessageId)
		metrics.MessagesProcessed.WithLabelValues("email", "duplicate").Inc()
		d.Ack(false)
		return
	}

	var event broker.Event
	if err := json.Unmarshal(d.Body, &event); err != nil {
		slog.Error("email: unmarshal failed", "err", err)
		d.Nack(false, false)
		return
	}

	status := "delivered"
	if err := w.send(event); err != nil {
		slog.Error("email: send failed", "msg_id", event.MessageID, "err", err)
		metrics.MessagesProcessed.WithLabelValues("email", "failed").Inc()
		d.Nack(false, true)
		return
	}
	metrics.MessagesProcessed.WithLabelValues("email", "delivered").Inc()

	if err := w.store.SaveNotification(ctx, store.Notification{
		MessageID: event.MessageID,
		Channel:   "email",
		EventType: event.Type,
		Status:    status,
		Payload:   d.Body,
	}); err != nil {
		slog.Error("email: persist failed", "err", err)
	}

	d.Ack(false)
}

func (w *EmailWorker) send(event broker.Event) error {
	if w.mailer == nil {
		slog.Info("email: SMTP not configured, skipping send", "msg_id", event.MessageID)
		return nil
	}

	to, _ := event.Payload["email"].(string)
	if to == "" {
		slog.Warn("email: no recipient in payload, skipping", "msg_id", event.MessageID)
		return nil
	}

	subject := fmt.Sprintf("[Nexus] %s notification", event.Type)
	body := fmt.Sprintf("Event: %s\nPriority: %s\nMessage ID: %s",
		event.Type, event.Priority, event.MessageID)

	return w.mailer.Send(to, subject, body)
}
