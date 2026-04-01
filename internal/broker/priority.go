package broker

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Lane describes a single priority tier.
type Lane struct {
	Name    string // "high" | "normal" | "low"
	Binding string // AMQP routing-key pattern
}

// PriorityLanes lists the three delivery tiers in descending priority order.
// Workers open a dedicated channel per lane and set QoS proportionally so
// high-priority messages are pre-fetched and processed first.
var PriorityLanes = []Lane{
	{Name: "high",   Binding: "event.*.high"},
	{Name: "normal", Binding: "event.*.normal"},
	{Name: "low",    Binding: "event.*.low"},
}

// OpenChannel opens a new, independent AMQP channel from the underlying
// connection. Callers own the returned channel and must close it.
func (c *Connection) OpenChannel() (*amqp.Channel, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("broker: open channel: %w", err)
	}
	return ch, nil
}
