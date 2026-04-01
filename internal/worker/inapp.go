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
	conn        *broker.Connection
	hub         *hub.Hub
	idempotency *idempotency.Client
	store       *store.Store
	poolSize    int
}

// NewInAppWorker declares priority queues/DLQs and returns a ready worker.
func NewInAppWorker(conn *broker.Connection, h *hub.Hub, idem *idempotency.Client, st *store.Store, poolSize int) (*InAppWorker, error) {
	if poolSize <= 0 {
		poolSize = InAppPoolSize
	}
	w := &InAppWorker{conn: conn, hub: h, idempotency: idem, store: st, poolSize: poolSize}

	ch, err := conn.OpenChannel()
	if err != nil {
		return nil, fmt.Errorf("inapp worker: open setup channel: %w", err)
	}
	defer ch.Close()

	if err := w.setup(ch); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *InAppWorker) setup(ch *amqp.Channel) error {
	for _, lane := range broker.PriorityLanes {
		q   := InAppQueue + "." + lane.Name
		dlq := InAppDLQ  + "." + lane.Name

		if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
			return fmt.Errorf("inapp worker: declare dlq %s: %w", dlq, err)
		}
		args := amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": dlq,
		}
		if _, err := ch.QueueDeclare(q, true, false, false, false, args); err != nil {
			return fmt.Errorf("inapp worker: declare queue %s: %w", q, err)
		}
		if err := ch.QueueBind(q, lane.Binding, broker.ExchangeName, false, nil); err != nil {
			return fmt.Errorf("inapp worker: bind %s: %w", q, err)
		}
	}
	return nil
}

// Run starts one goroutine pool per priority lane.
func (w *InAppWorker) Run(ctx context.Context) error {
	prefetches := [3]int{w.poolSize, max(w.poolSize/2, 1), max(w.poolSize/4, 1)}
	var wg sync.WaitGroup

	for i, lane := range broker.PriorityLanes {
		ch, err := w.conn.OpenChannel()
		if err != nil {
			return fmt.Errorf("inapp worker: open channel %s: %w", lane.Name, err)
		}

		prefetch := prefetches[i]
		if err := ch.Qos(prefetch, 0, false); err != nil {
			return fmt.Errorf("inapp worker: qos %s: %w", lane.Name, err)
		}

		msgs, err := ch.Consume(InAppQueue+"."+lane.Name, "", false, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("inapp worker: consume %s: %w", lane.Name, err)
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
