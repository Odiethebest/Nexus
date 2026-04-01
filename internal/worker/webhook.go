package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
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
	ch          *amqp.Channel
	httpClient  *http.Client
	idempotency *idempotency.Client
	store       *store.Store
	poolSize    int
}

// NewWebhookWorker declares the queue/DLQ and returns a ready worker.
func NewWebhookWorker(ch *amqp.Channel, idem *idempotency.Client, st *store.Store, poolSize int) (*WebhookWorker, error) {
	if poolSize <= 0 {
		poolSize = WebhookPoolSize
	}
	w := &WebhookWorker{
		ch:          ch,
		httpClient:  &http.Client{Timeout: 10 * time.Second},
		idempotency: idem,
		store:       st,
		poolSize:    poolSize,
	}
	if err := w.setup(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WebhookWorker) setup() error {
	if _, err := w.ch.QueueDeclare(WebhookDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("webhook worker: declare dlq: %w", err)
	}

	args := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": WebhookDLQ,
	}
	if _, err := w.ch.QueueDeclare(WebhookQueue, true, false, false, false, args); err != nil {
		return fmt.Errorf("webhook worker: declare queue: %w", err)
	}

	if err := w.ch.QueueBind(WebhookQueue, "event.*.*", broker.ExchangeName, false, nil); err != nil {
		return fmt.Errorf("webhook worker: bind queue: %w", err)
	}

	return nil
}

// Run starts consuming messages with a bounded goroutine pool.
func (w *WebhookWorker) Run(ctx context.Context) error {
	msgs, err := w.ch.Consume(WebhookQueue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("webhook worker: consume: %w", err)
	}

	sem := make(chan struct{}, w.poolSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("webhook worker: channel closed")
			}
			sem <- struct{}{}
			go func(d amqp.Delivery) {
				defer func() { <-sem }()
				w.process(ctx, d)
			}(msg)
		}
	}
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

	// Route to DLQ after maxRetries attempts.
	deathCount := xDeathCount(d)
	if deathCount >= maxRetries {
		slog.Warn("webhook: max retries exceeded, routing to DLQ", "msg_id", event.MessageID)
		metrics.MessagesProcessed.WithLabelValues("webhook", "dlq").Inc()
		d.Nack(false, false)
		return
	}

	if err := w.deliver(ctx, event, d.Body); err != nil {
		// Exponential backoff: 2s → 4s → 8s
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
		return nil // no target configured
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

// xDeathCount extracts the x-death retry count from AMQP headers.
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
