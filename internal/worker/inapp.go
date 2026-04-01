package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"nexus/internal/broker"
	"nexus/internal/hub"
	"nexus/internal/idempotency"
	"nexus/internal/metrics"
	"nexus/internal/store"
)

const (
	InAppQueue    = "nexus.inapp"
	InAppDLQ      = "nexus.inapp.dlq"
	InAppPoolSize = 5
)

// InAppWorker broadcasts events to the WebSocket hub for real-time delivery.
type InAppWorker struct {
	ch          *amqp.Channel
	hub         *hub.Hub
	idempotency *idempotency.Client
	store       *store.Store
	poolSize    int
}

// NewInAppWorker declares the queue/DLQ and returns a ready worker.
func NewInAppWorker(ch *amqp.Channel, h *hub.Hub, idem *idempotency.Client, st *store.Store, poolSize int) (*InAppWorker, error) {
	if poolSize <= 0 {
		poolSize = InAppPoolSize
	}
	w := &InAppWorker{ch: ch, hub: h, idempotency: idem, store: st, poolSize: poolSize}
	if err := w.setup(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *InAppWorker) setup() error {
	if _, err := w.ch.QueueDeclare(InAppDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("inapp worker: declare dlq: %w", err)
	}

	args := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": InAppDLQ,
	}
	if _, err := w.ch.QueueDeclare(InAppQueue, true, false, false, false, args); err != nil {
		return fmt.Errorf("inapp worker: declare queue: %w", err)
	}

	if err := w.ch.QueueBind(InAppQueue, "event.*.*", broker.ExchangeName, false, nil); err != nil {
		return fmt.Errorf("inapp worker: bind queue: %w", err)
	}

	return nil
}

// Run starts consuming messages with a bounded goroutine pool.
func (w *InAppWorker) Run(ctx context.Context) error {
	msgs, err := w.ch.Consume(InAppQueue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("inapp worker: consume: %w", err)
	}

	sem := make(chan struct{}, w.poolSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("inapp worker: channel closed")
			}
			sem <- struct{}{}
			go func(d amqp.Delivery) {
				defer func() { <-sem }()
				w.process(ctx, d)
			}(msg)
		}
	}
}

func (w *InAppWorker) process(ctx context.Context, d amqp.Delivery) {
	start := time.Now()
	defer func() { metrics.ProcessDuration.WithLabelValues("inapp").Observe(time.Since(start).Seconds()) }()

	ok, err := w.idempotency.Check(ctx, d.MessageId)
	if err != nil {
		slog.Error("inapp: idempotency check failed", "msg_id", d.MessageId, "err", err)
		d.Nack(false, true)
		return
	}
	if !ok {
		slog.Info("inapp: duplicate message, skipping", "msg_id", d.MessageId)
		metrics.MessagesProcessed.WithLabelValues("inapp", "duplicate").Inc()
		d.Ack(false)
		return
	}

	var event broker.Event
	if err := json.Unmarshal(d.Body, &event); err != nil {
		slog.Error("inapp: unmarshal failed", "err", err)
		d.Nack(false, false)
		return
	}

	w.hub.Broadcast(d.Body)
	slog.Info("inapp: broadcast to hub", "msg_id", event.MessageID, "type", event.Type)
	metrics.MessagesProcessed.WithLabelValues("inapp", "delivered").Inc()

	if err := w.store.SaveNotification(ctx, store.Notification{
		MessageID: event.MessageID,
		Channel:   "inapp",
		EventType: event.Type,
		Status:    "delivered",
		Payload:   d.Body,
	}); err != nil {
		slog.Error("inapp: persist failed", "err", err)
	}

	d.Ack(false)
}
