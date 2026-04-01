package broker

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeName = "nexus.events"
	ExchangeType = "topic"
)

// Connection wraps an AMQP connection and channel.
type Connection struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// New dials RabbitMQ, opens a channel, and declares the topic exchange.
func New(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("broker: dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("broker: open channel: %w", err)
	}

	if err := ch.ExchangeDeclare(
		ExchangeName,
		ExchangeType,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return nil, fmt.Errorf("broker: declare exchange: %w", err)
	}

	return &Connection{conn: conn, channel: ch}, nil
}

// Channel returns the underlying AMQP channel.
func (c *Connection) Channel() *amqp.Channel {
	return c.channel
}

// Close shuts down the channel and connection.
func (c *Connection) Close() {
	c.channel.Close()
	c.conn.Close()
}
