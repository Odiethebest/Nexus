package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"nexus/internal/broker"
	"nexus/internal/idempotency"
	"nexus/internal/metrics"
	"nexus/internal/store"
)

const (
	WebhookQueue    = "nexus.webhook"
	WebhookDLQ      = "nexus.webhook.dlq"
	WebhookPoolSize = 8
	maxRetries      = 3
)

// WebhookWorker delivers events to outbound HTTP endpoints with exponential backoff.
type WebhookWorker struct {
	conn        *broker.Connection
	httpClient  *http.Client
	idempotency *idempotency.Client
	store       *store.Store
	poolSize    int
}

// NewWebhookWorker declares priority queues/DLQs and returns a ready worker.
func NewWebhookWorker(conn *broker.Connection, idem *idempotency.Client, st *store.Store, poolSize int) (*WebhookWorker, error) {
	if poolSize <= 0 {
		poolSize = WebhookPoolSize
	}
	w := &WebhookWorker{
		conn:        conn,
		httpClient:  &http.Client{Timeout: 10 * time.Second},
		idempotency: idem,
		store:       st,
		poolSize:    poolSize,
	}

	ch, err := conn.OpenChannel()
	if err != nil {
		return nil, fmt.Errorf("webhook worker: open setup channel: %w", err)
	}
	defer ch.Close()

	if err := w.setup(ch); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WebhookWorker) setup(ch *amqp.Channel) error {
	for _, lane := range broker.PriorityLanes {
		q   := WebhookQueue + "." + lane.Name
		dlq := WebhookDLQ  + "." + lane.Name

		if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
			return fmt.Errorf("webhook worker: declare dlq %s: %w", dlq, err)
		}
		args := amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": dlq,
		}
		if _, err := ch.QueueDeclare(q, true, false, false, false, args); err != nil {
			return fmt.Errorf("webhook worker: declare queue %s: %w", q, err)
		}
		if err := ch.QueueBind(q, lane.Binding, broker.ExchangeName, false, nil); err != nil {
			return fmt.Errorf("webhook worker: bind %s: %w", q, err)
		}
	}
	return nil
}

// Run starts one goroutine pool per priority lane.
func (w *WebhookWorker) Run(ctx context.Context) error {
	prefetches := [3]int{w.poolSize, max(w.poolSize/2, 1), max(w.poolSize/4, 1)}
	var wg sync.WaitGroup

	for i, lane := range broker.PriorityLanes {
		ch, err := w.conn.OpenChannel()
		if err != nil {
			return fmt.Errorf("webhook worker: open channel %s: %w", lane.Name, err)
		}

		prefetch := prefetches[i]
		if err := ch.Qos(prefetch, 0, false); err != nil {
			return fmt.Errorf("webhook worker: qos %s: %w", lane.Name, err)
		}

		msgs, err := ch.Consume(WebhookQueue+"."+lane.Name, "", false, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("webhook worker: consume %s: %w", lane.Name, err)
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

func (w *WebhookWorker) process(ctx context.Context, d amqp.Delivery) {
	start := time.Now()
	defer func() { metrics.ProcessDuration.WithLabelValues("webhook").Observe(time.Since(start).Seconds()) }()

	ok, err := w.idempotency.Check(ctx, d.MessageId)
	if err != nil {
		slog.Error("webhook: idempotency check failed", "msg_id", d.MessageId, "err", err)
		d.Nack(false, true)
		return
	}
	if !ok {
		slog.Info("webhook: duplicate message, skipping", "msg_id", d.MessageId)
		metrics.MessagesProcessed.WithLabelValues("webhook", "duplicate").Inc()
		d.Ack(false)
		return
	}

	var event broker.Event
	if err := json.Unmarshal(d.Body, &event); err != nil {
		slog.Error("webhook: unmarshal failed", "err", err)
		d.Nack(false, false)
		return
	}

	deathCount := xDeathCount(d)
	if deathCount >= maxRetries {
		slog.Warn("webhook: max retries exceeded, routing to DLQ", "msg_id", event.MessageID)
		metrics.MessagesProcessed.WithLabelValues("webhook", "dlq").Inc()
		d.Nack(false, false)
		return
	}

	if err := w.deliver(ctx, event, d.Body); err != nil {
		backoff := time.Duration(math.Pow(2, float64(deathCount+1))) * time.Second
		slog.Error("webhook: delivery failed, requeuing",
			"msg_id", event.MessageID, "attempt", deathCount+1, "backoff", backoff, "err", err)
		metrics.MessagesProcessed.WithLabelValues("webhook", "failed").Inc()
		time.Sleep(backoff)
		d.Nack(false, true)
		return
	}
	metrics.MessagesProcessed.WithLabelValues("webhook", "delivered").Inc()

	if err := w.store.SaveNotification(ctx, store.Notification{
		MessageID: event.MessageID,
		Channel:   "webhook",
		EventType: event.Type,
		Status:    "delivered",
		Payload:   d.Body,
	}); err != nil {
		slog.Error("webhook: persist failed", "err", err)
	}

	d.Ack(false)
}

func (w *WebhookWorker) deliver(ctx context.Context, event broker.Event, body []byte) error {
	webhookURL, _ := event.Payload["webhook_url"].(string)
	if webhookURL == "" {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("webhook: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Nexus-Message-ID", event.MessageID)

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook: upstream returned %d", resp.StatusCode)
	}
	return nil
}

func xDeathCount(d amqp.Delivery) int {
	deaths, ok := d.Headers["x-death"].([]interface{})
	if !ok || len(deaths) == 0 {
		return 0
	}
	table, ok := deaths[0].(amqp.Table)
	if !ok {
		return 0
	}
	count, _ := table["count"].(int64)
	return int(count)
}
