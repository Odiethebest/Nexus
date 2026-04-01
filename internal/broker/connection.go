package broker

import (
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeName    = "nexus.events"
	ExchangeType    = "topic"
	maxRetryBackoff = 30 * time.Second
)

// Connection wraps an AMQP connection and channel with automatic reconnect.
type Connection struct {
	url     string
	mu      sync.RWMutex
	conn    *amqp.Connection
	channel *amqp.Channel
}

// New dials RabbitMQ, declares the topic exchange, and starts a background
// reconnect watcher.
func New(url string) (*Connection, error) {
	c := &Connection{url: url}
	if err := c.dial(); err != nil {
		return nil, err
	}
	go c.watchClose()
	return c, nil
}

func (c *Connection) dial() error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return fmt.Errorf("broker: dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("broker: open channel: %w", err)
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
		conn.Close()
		return fmt.Errorf("broker: declare exchange: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.channel = ch
	c.mu.Unlock()
	return nil
}

// watchClose listens for connection-level close events and reconnects with
// exponential backoff (1s → 2s → 4s … capped at 30s).
func (c *Connection) watchClose() {
	for {
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		closed := make(chan *amqp.Error, 1)
		conn.NotifyClose(closed)

		amqpErr := <-closed
		if amqpErr == nil {
			// intentional Close() call — stop the watcher
			return
		}

		slog.Warn("broker: connection lost, reconnecting", "err", amqpErr)

		for attempt := 1; ; attempt++ {
			backoff := time.Duration(math.Min(
				float64(maxRetryBackoff),
				float64(time.Second)*math.Pow(2, float64(attempt-1)),
			))
			time.Sleep(backoff)

			if err := c.dial(); err != nil {
				slog.Warn("broker: reconnect failed", "attempt", attempt, "err", err)
				continue
			}

			slog.Info("broker: reconnected successfully", "attempt", attempt)
			break
		}
	}
}

// Channel returns the current shared AMQP channel (used by workers).
func (c *Connection) Channel() *amqp.Channel {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.channel
}

// OpenConfirmChannel opens a dedicated channel with publisher confirms enabled.
// The caller owns this channel and must close it when done.
func (c *Connection) OpenConfirmChannel() (*amqp.Channel, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("broker: open confirm channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, fmt.Errorf("broker: enable confirm mode: %w", err)
	}
	return ch, nil
}

// Close shuts down the shared channel and connection. The reconnect watcher
// stops automatically when it sees a nil error on the close notification.
func (c *Connection) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.channel.Close()
	c.conn.Close()
}
