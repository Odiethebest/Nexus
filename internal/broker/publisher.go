package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"nexus/internal/metrics"
)

// Event is the canonical message payload published to the exchange.
type Event struct {
	MessageID string         `json:"message_id"`
	Type      string         `json:"type"`
	Priority  string         `json:"priority"`
	Payload   map[string]any `json:"payload"`
	Timestamp time.Time      `json:"timestamp"`
}

// Publisher publishes events to the topic exchange using a dedicated
// confirm-mode channel. A mutex serialises publishes so that each ack
// can be matched to its delivery without a sequence-number tracker.
type Publisher struct {
	conn     *Connection
	mu       sync.Mutex
	ch       *amqp.Channel
	confirms <-chan amqp.Confirmation
}

// NewPublisher opens a confirm-mode channel and returns a ready Publisher.
func NewPublisher(conn *Connection) (*Publisher, error) {
	p := &Publisher{conn: conn}
	if err := p.openChannel(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Publisher) openChannel() error {
	ch, err := p.conn.OpenConfirmChannel()
	if err != nil {
		return err
	}
	p.ch = ch
	p.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	return nil
}

// Publish routes an event to the exchange with routing key event.{type}.{priority}.
// It waits for a broker ack before returning. On channel failure it reopens
// the channel and retries once.
func (p *Publisher) Publish(ctx context.Context, eventType, priority string, payload map[string]any) (string, error) {
	start := time.Now()
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("publisher: generate uuid: %w", err)
	}

	event := Event{
		MessageID: id.String(),
		Type:      eventType,
		Priority:  priority,
		Payload:   payload,
		Timestamp: time.Now().UTC(),
	}

	body, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("publisher: marshal: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.publishAndConfirm(ctx, event, body); err != nil {
		// Channel may be dead — reopen and retry once
		if reopenErr := p.openChannel(); reopenErr != nil {
			return "", fmt.Errorf("publisher: reopen channel: %w", reopenErr)
		}
		if err := p.publishAndConfirm(ctx, event, body); err != nil {
			return "", err
		}
	}

	metrics.PublishDuration.Observe(time.Since(start).Seconds())
	metrics.MessagesPublished.WithLabelValues(eventType, priority).Inc()

	return event.MessageID, nil
}

func (p *Publisher) publishAndConfirm(ctx context.Context, event Event, body []byte) error {
	safeType := strings.ReplaceAll(event.Type, ".", "_")
	routingKey := fmt.Sprintf("event.%s.%s", safeType, event.Priority)

	if err := p.ch.PublishWithContext(
		ctx,
		ExchangeName,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    event.MessageID,
			Timestamp:    event.Timestamp,
			Body:         body,
		},
	); err != nil {
		return fmt.Errorf("publisher: publish: %w", err)
	}

	select {
	case confirm := <-p.confirms:
		if !confirm.Ack {
			return fmt.Errorf("publisher: broker nacked message %s", event.MessageID)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
