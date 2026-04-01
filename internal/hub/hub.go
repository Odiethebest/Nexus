package hub

import (
	"log/slog"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type client struct {
	conn *websocket.Conn
	send chan []byte
}

// Hub manages active WebSocket connections and fans out messages to all clients.
type Hub struct {
	mu       sync.RWMutex
	clients  map[*client]struct{}
	upgrader websocket.Upgrader
}

// New creates an empty Hub.
func New(checkOrigins ...func(*http.Request) bool) *Hub {
	checkOrigin := func(*http.Request) bool { return true }
	if len(checkOrigins) > 0 && checkOrigins[0] != nil {
		checkOrigin = checkOrigins[0]
	}

	return &Hub{
		clients: make(map[*client]struct{}),
		upgrader: websocket.Upgrader{
			CheckOrigin: checkOrigin,
		},
	}
}

// ServeWS upgrades an HTTP connection to WebSocket and registers the client.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("hub: upgrade failed", "err", err)
		return
	}

	c := &client{conn: conn, send: make(chan []byte, 256)}
	h.register(c)
	go c.writePump()
	c.readPump(func() { h.unregister(c) })
}

// Broadcast sends msg to every connected client.
// Slow clients are dropped rather than blocking the broadcaster.
func (h *Hub) Broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		select {
		case c.send <- msg:
		default:
			// slow client — drop
		}
	}
}

func (h *Hub) register(c *client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	close(c.send)
}

func (c *client) writePump() {
	defer c.conn.Close()
	for msg := range c.send {
		if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}

func (c *client) readPump(onClose func()) {
	defer func() {
		onClose()
		c.conn.Close()
	}()
	// Drain incoming frames (ping/pong/close); we don't process client messages.
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}
