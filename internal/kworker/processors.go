package kworker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"nexus/internal/kbroker"
	"nexus/internal/mailer"
)

// InAppProcessor accepts events for the in-app channel. It has no failure
// mode of its own, so it always reports Delivered.
//
// It used to broadcast to a *hub.Hub directly, which never reached a browser:
// the worker's hub is a different object in a different process from the one
// the producer serves /ws from. Fan-out to WebSocket clients now goes through
// the runner's LiveFeed (internal/wsfeed), which covers every channel rather
// than only this one.
type InAppProcessor struct {
	Log *slog.Logger
}

func (p *InAppProcessor) Channel() kbroker.Channel { return kbroker.ChannelInApp }

func (p *InAppProcessor) Deliver(_ context.Context, event kbroker.Event, _ []byte) Outcome {
	if p.Log != nil {
		p.Log.Debug("inapp: accepted", "msg_id", event.MessageID, "type", event.Type)
	}
	return OutcomeDelivered
}

// EmailProcessor sends via SMTP. Nil mailer means "SMTP not configured" —
// we count that as Delivered so the pipeline stays green in local dev
// without SMTP credentials; the notification row's status still says
// "delivered", matching the old AMQP behaviour (email.go:171).
type EmailProcessor struct {
	Mailer *mailer.Mailer
	Log    *slog.Logger
}

func (p *EmailProcessor) Channel() kbroker.Channel { return kbroker.ChannelEmail }

func (p *EmailProcessor) Deliver(_ context.Context, event kbroker.Event, _ []byte) Outcome {
	if p.Mailer == nil {
		if p.Log != nil {
			p.Log.Info("email: SMTP not configured, skipping send", "msg_id", event.MessageID)
		}
		return OutcomeDelivered
	}
	to, _ := event.Payload["email"].(string)
	if to == "" {
		if p.Log != nil {
			p.Log.Warn("email: no recipient in payload, skipping", "msg_id", event.MessageID)
		}
		return OutcomeDelivered
	}
	subject := fmt.Sprintf("[Nexus] %s notification", event.Type)
	body := fmt.Sprintf("Event: %s\nPriority: %s\nMessage ID: %s",
		event.Type, event.Priority, event.MessageID)
	if err := p.Mailer.Send(to, subject, body); err != nil {
		if p.Log != nil {
			p.Log.Error("email: send failed", "msg_id", event.MessageID, "err", err)
		}
		return OutcomeTransientError
	}
	return OutcomeDelivered
}

// WebhookProcessor POSTs the raw event JSON to payload.webhook_url. Any
// non-2xx response or transport error is treated as a transient failure —
// the runner will re-produce back to the primary topic until the retry
// budget is exhausted. Missing webhook_url is Skipped (matches the AMQP
// no_webhook counter path).
type WebhookProcessor struct {
	HTTP *http.Client
	Log  *slog.Logger
}

// NewWebhookProcessor builds a processor with a sensible HTTP timeout.
func NewWebhookProcessor(log *slog.Logger) *WebhookProcessor {
	return &WebhookProcessor{
		HTTP: &http.Client{Timeout: 10 * time.Second},
		Log:  log,
	}
}

func (p *WebhookProcessor) Channel() kbroker.Channel { return kbroker.ChannelWebhook }

func (p *WebhookProcessor) Deliver(ctx context.Context, event kbroker.Event, body []byte) Outcome {
	webhookURL, _ := event.Payload["webhook_url"].(string)
	if webhookURL == "" {
		return OutcomeSkipped
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		if p.Log != nil {
			p.Log.Error("webhook: build request", "msg_id", event.MessageID, "err", err)
		}
		return OutcomePermanentError // bad URL is not going to fix itself
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Nexus-Message-ID", event.MessageID)

	resp, err := p.HTTP.Do(req)
	if err != nil {
		if p.Log != nil {
			p.Log.Warn("webhook: transport error", "msg_id", event.MessageID, "err", err)
		}
		return OutcomeTransientError
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return OutcomeTransientError
	}
	if resp.StatusCode >= 400 {
		// 4xx that isn't 429 — client-side bug in the upstream, retrying
		// won't help.
		return OutcomePermanentError
	}
	return OutcomeDelivered
}
