// Package grpcserver exposes a gRPC EventService alongside the HTTP API.
// It uses a JSON codec registered under the "proto" name so the service
// can be called without a generated protobuf client — any gRPC client that
// sets Content-Type: application/grpc+json will work.
//
// To use a standard protobuf client instead, run `buf generate` from the
// repo root (requires the buf toolchain) to regenerate stubs from
// proto/event.proto, then replace this package with the generated code.
package grpcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"nexus/internal/broker"
)

func init() {
	// Register a JSON codec under the "proto" name so gRPC uses JSON
	// framing instead of protobuf binary. This avoids a protoc/buf
	// dependency while keeping the standard gRPC wire format intact.
	encoding.RegisterCodec(jsonCodec{})
}

// ── JSON codec ────────────────────────────────────────────────────────────────

type jsonCodec struct{}

func (jsonCodec) Marshal(v interface{}) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(b []byte, v interface{}) error    { return json.Unmarshal(b, v) }
func (jsonCodec) Name() string                                { return "proto" }

// ── Request / response types (mirror proto/event.proto) ──────────────────────

// PublishRequest mirrors the proto PublishRequest message.
type PublishRequest struct {
	Type     string          `json:"type"`
	Priority string          `json:"priority"`
	Payload  json.RawMessage `json:"payload"` // JSON-encoded map[string]any
}

// PublishResponse mirrors the proto PublishResponse message.
type PublishResponse struct {
	MessageID string `json:"message_id"`
}

// ── Service implementation ────────────────────────────────────────────────────

// EventServer implements the EventService gRPC API.
type EventServer struct {
	pub *broker.Publisher
}

// NewEventServer returns an EventServer backed by pub.
func NewEventServer(pub *broker.Publisher) *EventServer {
	return &EventServer{pub: pub}
}

// Publish publishes an event to the exchange and returns the message ID.
func (s *EventServer) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error) {
	if req.Type == "" || req.Priority == "" {
		return nil, fmt.Errorf("type and priority are required")
	}

	var payload map[string]any
	if len(req.Payload) > 0 {
		if err := json.Unmarshal(req.Payload, &payload); err != nil {
			return nil, fmt.Errorf("invalid payload JSON: %w", err)
		}
	}

	msgID, err := s.pub.Publish(ctx, req.Type, req.Priority, payload)
	if err != nil {
		return nil, err
	}
	return &PublishResponse{MessageID: msgID}, nil
}

// ── gRPC service descriptor ───────────────────────────────────────────────────

// ServiceDesc describes the EventService for manual gRPC registration.
// This mirrors what protoc-gen-go-grpc would generate from proto/event.proto.
var ServiceDesc = grpc.ServiceDesc{
	ServiceName: "event.v1.EventService",
	HandlerType: (*EventServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Publish",
			Handler:    publishHandler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/event.proto",
}

func publishHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	req := new(PublishRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*EventServer).Publish(ctx, req)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/event.v1.EventService/Publish",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*EventServer).Publish(ctx, req.(*PublishRequest))
	}
	return interceptor(ctx, req, info, handler)
}

// ── Server factory ────────────────────────────────────────────────────────────

// Listen creates a TCP listener on addr, registers EventService, and returns
// the grpc.Server and net.Listener. Caller must call srv.Serve(lis).
func Listen(addr string, pub *broker.Publisher) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("grpc: listen %s: %w", addr, err)
	}
	srv := grpc.NewServer()
	srv.RegisterService(&ServiceDesc, NewEventServer(pub))
	return srv, lis, nil
}
