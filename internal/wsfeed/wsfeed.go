// Package wsfeed carries live delivery events from the worker process to the
// producer process, which is the one that serves /ws.
//
// The two run as separate services, so the worker cannot broadcast to
// WebSocket clients directly — it has no HTTP server, and the producer's hub
// is a different object in a different process. Redis pub/sub bridges them:
// the worker PUBLISHes one envelope per record it reaches a verdict on, and
// every producer replica SUBSCRIBEs and fans out to its own connected
// clients. Pub/sub (rather than a Kafka consumer group) is deliberate — each
// producer replica must receive every event, not one replica per event.
//
// The feed is best-effort. It is a dashboard, not a delivery channel: a
// Redis outage or a slow subscriber must never slow down or fail the actual
// notification pipeline.
package wsfeed

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ChannelName is the Redis pub/sub channel the envelopes travel on.
const ChannelName = "nexus:ws:events"

// Envelope is the JSON delivered to /ws clients. It is a wire contract with
// the frontend's WsEvent type (web/types/index.ts) — do not rename fields.
//
// Unlike the raw kbroker.Event it is derived from, this carries channel and
// status, because one published event fans out to three channels and the UI
// needs to tell them apart.
type Envelope struct {
	MessageID string         `json:"message_id"`
	Type      string         `json:"type"`
	Priority  string         `json:"priority"`
	Channel   string         `json:"channel"`
	Status    string         `json:"status"`
	Payload   map[string]any `json:"payload"`
	Timestamp time.Time      `json:"timestamp"`
}

const (
	publishBuffer  = 1024
	publishTimeout = 1 * time.Second
	// closeTimeout bounds how long shutdown waits for the buffer to drain.
	// With Redis unreachable every send burns publishTimeout, so a full
	// buffer would otherwise hold the process open for many minutes — far
	// past any SIGTERM budget. Undelivered dashboard frames are expendable.
	closeTimeout = 2 * time.Second
)

// Publisher pushes envelopes onto the Redis channel from the worker.
//
// Publish never blocks the caller: envelopes go onto a bounded buffer drained
// by a single goroutine, and are dropped when it is full. Dropping frames
// from a live dashboard is the correct trade against stalling delivery.
type Publisher struct {
	rdb *redis.Client
	log *slog.Logger

	ch       chan []byte
	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	dropped  atomic.Int64
}

// NewPublisher starts the background drain goroutine. Call Close to stop it.
func NewPublisher(rdb *redis.Client, log *slog.Logger) *Publisher {
	if log == nil {
		log = slog.Default()
	}
	p := &Publisher{
		rdb:  rdb,
		log:  log,
		ch:   make(chan []byte, publishBuffer),
		stop: make(chan struct{}),
	}
	p.wg.Add(1)
	go p.drain()
	return p
}

// Publish enqueues an envelope. Safe for concurrent use.
func (p *Publisher) Publish(_ context.Context, env Envelope) {
	body, err := json.Marshal(env)
	if err != nil {
		p.log.Warn("wsfeed: marshal envelope", "msg_id", env.MessageID, "err", err)
		return
	}
	select {
	case p.ch <- body:
	default:
		// Buffer full — the feed is behind. Count it and move on.
		if n := p.dropped.Add(1); n%1000 == 1 {
			p.log.Warn("wsfeed: dropping live-feed frames, buffer full", "dropped_total", n)
		}
	}
}

// Dropped reports how many envelopes were discarded due to a full buffer.
func (p *Publisher) Dropped() int64 { return p.dropped.Load() }

// Close stops the background goroutine, waiting up to closeTimeout for any
// buffered envelopes to go out. It is safe to call more than once.
func (p *Publisher) Close() {
	p.stopOnce.Do(func() { close(p.ch) })

	drained := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
	case <-time.After(closeTimeout):
		// Redis is unreachable or far behind. Abandon the backlog rather
		// than hold up process shutdown.
		close(p.stop)
		<-drained
		p.log.Warn("wsfeed: abandoned undelivered live-feed frames on close")
	}
}

func (p *Publisher) drain() {
	defer p.wg.Done()
	for {
		select {
		case <-p.stop:
			return
		case body, ok := <-p.ch:
			if !ok {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
			err := p.rdb.Publish(ctx, ChannelName, body).Err()
			cancel()
			if err != nil {
				// Debug, not error: a dashboard feed failing is not an incident.
				p.log.Debug("wsfeed: publish", "err", err)
			}
		}
	}
}

// Broadcaster is the sink a Bridge forwards into. *hub.Hub satisfies it.
type Broadcaster interface {
	Broadcast(msg []byte)
}

// Bridge subscribes to the Redis channel and forwards each payload to a
// Broadcaster. Runs in the producer.
type Bridge struct {
	rdb *redis.Client
	out Broadcaster
	log *slog.Logger
}

// NewBridge wires a Redis client to a broadcast sink.
func NewBridge(rdb *redis.Client, out Broadcaster, log *slog.Logger) *Bridge {
	if log == nil {
		log = slog.Default()
	}
	return &Bridge{rdb: rdb, out: out, log: log}
}

// Run blocks until ctx is cancelled. go-redis reconnects the subscription
// internally, so a Redis blip does not need handling here.
func (b *Bridge) Run(ctx context.Context) error {
	sub := b.rdb.Subscribe(ctx, ChannelName)
	defer sub.Close()

	b.log.Info("wsfeed: bridge subscribed", "channel", ChannelName)
	msgs := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return nil
			}
			// Forward the payload verbatim — the producer does not need to
			// understand the envelope to fan it out.
			b.out.Broadcast([]byte(msg.Payload))
		}
	}
}
