package hub_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"nexus/internal/hub"
)

// Browsers do not apply CORS to WebSocket handshakes, so /ws is only as
// protected as the upgrader's CheckOrigin. hub.New has always accepted a
// checker; nothing passed one until the producer's origin allow-list was
// wired up. These tests pin that the parameter is actually honored.

func dialWS(t *testing.T, srv *httptest.Server, origin string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
	}
	return websocket.DefaultDialer.Dial(wsURL, header)
}

func TestServeWS_RejectsDisallowedOrigin(t *testing.T) {
	h := hub.New(func(r *http.Request) bool {
		return r.Header.Get("Origin") == "https://app.example.com"
	})
	srv := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	defer srv.Close()

	conn, resp, err := dialWS(t, srv, "https://evil.example.com")
	if err == nil {
		conn.Close()
		t.Fatal("handshake succeeded for a disallowed origin")
	}
	if !errors.Is(err, websocket.ErrBadHandshake) {
		t.Fatalf("err = %v, want ErrBadHandshake", err)
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %v, want 403", resp)
	}
	if h.Count() != 0 {
		t.Errorf("rejected client was registered: Count() = %d", h.Count())
	}
}

func TestServeWS_AcceptsAllowedOrigin(t *testing.T) {
	h := hub.New(func(r *http.Request) bool {
		return r.Header.Get("Origin") == "https://app.example.com"
	})
	srv := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	defer srv.Close()

	conn, _, err := dialWS(t, srv, "https://app.example.com")
	if err != nil {
		t.Fatalf("handshake failed for an allowed origin: %v", err)
	}
	defer conn.Close()

	// Registration happens in the server goroutine, so wait for it rather
	// than racing the Dial return.
	waitForCount(t, h, 1)

	// Round-trip a broadcast to prove the connection is genuinely live and
	// not just a handshake that succeeded and died.
	want := []byte(`{"message_id":"m1"}`)
	h.Broadcast(want)

	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, got, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read broadcast: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("broadcast payload = %q, want %q", got, want)
	}
}

func waitForCount(t *testing.T, h *hub.Hub, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.Count() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Count() = %d after 3s, want %d", h.Count(), want)
}

func TestNew_WithoutCheckerAllowsAnyOrigin(t *testing.T) {
	// Zero-config demo default.
	h := hub.New()
	srv := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	defer srv.Close()

	conn, _, err := dialWS(t, srv, "https://anywhere.example.com")
	if err != nil {
		t.Fatalf("default hub should accept any origin, got %v", err)
	}
	conn.Close()
}
