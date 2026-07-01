// Package replay's AMQP path: retained during the migration so the
// legacy USE_KAFKA=false code path keeps working. Deleted in Step 7 of
// MIGRATION.md.
package replay

import (
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	"nexus/internal/broker"
)

// AMQPReplayer reads dead-lettered messages from a RabbitMQ DLQ queue and
// re-publishes them to the topic exchange under the original routing key.
type AMQPReplayer struct {
	conn *broker.Connection
}

// NewAMQP builds an AMQPReplayer bound to the given AMQP connection.
func NewAMQP(conn *broker.Connection) *Replayer {
	// Wrap in the Replayer facade so cmd/producer sees a single type. The
	// backend is chosen inside the facade methods.
	return &Replayer{amqp: &AMQPReplayer{conn: conn}}
}

// replayAMQP is the shim the facade calls when running against RabbitMQ.
func (a *AMQPReplayer) replay(ctx context.Context, queue string, max int) (int, error) {
	ch, err := a.conn.OpenChannel()
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
			break
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
			msg.Nack(false, true)
			return count, fmt.Errorf("replay: republish message %q: %w", msg.MessageId, err)
		}
		msg.Ack(false)
		count++
	}
	slog.Info("replay: done (amqp)", "queue", queue, "replayed", count)
	return count, nil
}

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
