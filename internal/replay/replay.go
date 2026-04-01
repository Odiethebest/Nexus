// Package replay provides DLQ message replay — reads dead-lettered messages
// from a queue and republishes them to the exchange for reprocessing.
package replay

import (
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
	"nexus/internal/broker"
)

// Replayer reads messages from a DLQ and republishes them to the exchange.
type Replayer struct {
	conn *broker.Connection
}

// New creates a Replayer backed by conn.
func New(conn *broker.Connection) *Replayer {
	return &Replayer{conn: conn}
}

// Replay pulls up to max messages from queue and re-publishes each one to the
// exchange using the original routing key stored in the x-death header.
// Returns the number of messages successfully replayed.
func (r *Replayer) Replay(ctx context.Context, queue string, max int) (int, error) {
	ch, err := r.conn.OpenChannel()
	if err != nil {
		return 0, fmt.Errorf("replay: open channel: %w", err)
	}
	defer ch.Close()

	count := 0
	for count < max {
		msg, ok, err := ch.Get(queue, false)
		if err != nil {
			return count, fmt.Errorf("replay: get from %q: %w", queue, err)
		}
		if !ok {
			break // queue empty
		}

		routingKey := originalRoutingKey(msg)
		slog.Info("replay: republishing message",
			"msg_id", msg.MessageId, "queue", queue, "routing_key", routingKey)

		if err := ch.PublishWithContext(ctx,
			broker.ExchangeName,
			routingKey,
			false, false,
			amqp.Publishing{
				ContentType:  msg.ContentType,
				DeliveryMode: amqp.Persistent,
				MessageId:    msg.MessageId,
				Timestamp:    msg.Timestamp,
				Body:         msg.Body,
			},
		); err != nil {
			msg.Nack(false, true) // put it back
			return count, fmt.Errorf("replay: republish message %q: %w", msg.MessageId, err)
		}

		msg.Ack(false)
		count++
	}

	slog.Info("replay: done", "queue", queue, "replayed", count)
	return count, nil
}

// originalRoutingKey extracts the first routing key from the x-death header
// so the message is re-delivered to the same queue it came from.
func originalRoutingKey(d amqp.Delivery) string {
	deaths, ok := d.Headers["x-death"].([]interface{})
	if !ok || len(deaths) == 0 {
		return "event.unknown.normal"
	}
	table, ok := deaths[0].(amqp.Table)
	if !ok {
		return "event.unknown.normal"
	}
	keys, _ := table["routing-keys"].([]interface{})
	if len(keys) == 0 {
		return "event.unknown.normal"
	}
	key, _ := keys[0].(string)
	if key == "" {
		return "event.unknown.normal"
	}
	return key
}
