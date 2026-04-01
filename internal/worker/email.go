package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
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

// EmailWorker consumes from three priority lanes and delivers email notifications.
// If m is nil, actual sending is skipped (useful when SMTP is not configured).
type EmailWorker struct {
	conn        *broker.Connection
	mailer      *mailer.Mailer
	idempotency *idempotency.Client
	store       *store.Store
	poolSize    int
}

// NewEmailWorker declares priority queues/DLQs and returns a ready worker.
func NewEmailWorker(conn *broker.Connection, m *mailer.Mailer, idem *idempotency.Client, st *store.Store, poolSize int) (*EmailWorker, error) {
	if poolSize <= 0 {
		poolSize = EmailPoolSize
	}
	w := &EmailWorker{conn: conn, mailer: m, idempotency: idem, store: st, poolSize: poolSize}

	ch, err := conn.OpenChannel()
	if err != nil {
		return nil, fmt.Errorf("email worker: open setup channel: %w", err)
	}
	defer ch.Close()

	if err := w.setup(ch); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *EmailWorker) setup(ch *amqp.Channel) error {
	for _, lane := range broker.PriorityLanes {
		q   := EmailQueue + "." + lane.Name
		dlq := EmailDLQ  + "." + lane.Name

		if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
			return fmt.Errorf("email worker: declare dlq %s: %w", dlq, err)
		}
		args := amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": dlq,
		}
		if _, err := ch.QueueDeclare(q, true, false, false, false, args); err != nil {
			return fmt.Errorf("email worker: declare queue %s: %w", q, err)
		}
		if err := ch.QueueBind(q, lane.Binding, broker.ExchangeName, false, nil); err != nil {
			return fmt.Errorf("email worker: bind %s: %w", q, err)
		}
	}
	return nil
}

// Run starts one goroutine pool per priority lane. High-priority lanes receive
// a proportionally larger prefetch count so they drain faster under load.
func (w *EmailWorker) Run(ctx context.Context) error {
	prefetches := [3]int{w.poolSize, max(w.poolSize/2, 1), max(w.poolSize/4, 1)}
	var wg sync.WaitGroup

	for i, lane := range broker.PriorityLanes {
		ch, err := w.conn.OpenChannel()
		if err != nil {
			return fmt.Errorf("email worker: open channel %s: %w", lane.Name, err)
		}

		prefetch := prefetches[i]
		if err := ch.Qos(prefetch, 0, false); err != nil {
			return fmt.Errorf("email worker: qos %s: %w", lane.Name, err)
		}

		msgs, err := ch.Consume(EmailQueue+"."+lane.Name, "", false, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("email worker: consume %s: %w", lane.Name, err)
		}

		sem := make(chan struct{}, prefetch)
		wg.Add(1)
		go func(msgs <-chan amqp.Delivery, sem chan struct{}) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case d, ok := <-msgs:
					if !ok {
						return
					}
					sem <- struct{}{}
					go func(d amqp.Delivery) {
						defer func() { <-sem }()
						w.process(ctx, d)
					}(d)
				}
			}
		}(msgs, sem)
	}

	wg.Wait()
	return ctx.Err()
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
		Status:    "delivered",
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
