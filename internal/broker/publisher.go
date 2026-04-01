package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Event is the canonical message payload published to the exchange.
type Event struct {
	MessageID string         `json:"message_id"`
	Type      string         `json:"type"`
	Priority  string         `json:"priority"`
	Payload   map[string]any `json:"payload"`
	Timestamp time.Time      `json:"timestamp"`
}

// Publisher publishes events to the topic exchange.
type Publisher struct {
	conn *Connection
}

// NewPublisher creates a Publisher backed by the given connection.
func NewPublisher(conn *Connection) *Publisher {
	return &Publisher{conn: conn}
}

// Publish routes an event to the exchange with routing key event.{type}.{priority}.
// Returns the generated UUIDv7 message ID.
func (p *Publisher) Publish(ctx context.Context, eventType, priority string, payload map[string]any) (string, error) {
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

	routingKey := fmt.Sprintf("event.%s.%s", eventType, priority)

	if err := p.conn.channel.PublishWithContext(
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
		return "", fmt.Errorf("publisher: publish: %w", err)
	}

	return event.MessageID, nil
}
