package kworker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"nexus/internal/kbroker"
)

func TestCloneWithRetryUpdatesRetryHeader(t *testing.T) {
	src := &kgo.Record{
		Topic: "nexus.webhook.high",
		Key:   []byte("msg-1"),
		Value: []byte(`{"message_id":"msg-1"}`),
		Headers: []kgo.RecordHeader{
			{Key: kbroker.HeaderMsgID, Value: []byte("msg-1")},
			{Key: kbroker.HeaderProducedAt, Value: []byte("123456789")},
			{Key: kbroker.HeaderRetryCount, Value: []byte("0")},
		},
	}
	out := cloneWithRetry(src, "nexus.webhook.high", 2)

	if out.Topic != "nexus.webhook.high" {
		t.Errorf("topic = %s", out.Topic)
	}
	got := map[string]string{}
	for _, h := range out.Headers {
		got[h.Key] = string(h.Value)
	}
	if got[kbroker.HeaderRetryCount] != "2" {
		t.Errorf("retry-count = %s, want 2", got[kbroker.HeaderRetryCount])
	}
	// x-produced-at must survive so the e2e lag histogram stays honest.
	if got[kbroker.HeaderProducedAt] != "123456789" {
		t.Errorf("produced-at header dropped: got %q", got[kbroker.HeaderProducedAt])
	}
}

func TestCloneWithRetryAppendsWhenMissing(t *testing.T) {
	src := &kgo.Record{
		Topic:   "nexus.email.normal",
		Headers: []kgo.RecordHeader{{Key: kbroker.HeaderMsgID, Value: []byte("m")}},
	}
	out := cloneWithRetry(src, "nexus.dlq.email.normal", 3)
	if out.Topic != "nexus.dlq.email.normal" {
		t.Errorf("target topic = %s", out.Topic)
	}
	found := false
	for _, h := range out.Headers {
		if h.Key == kbroker.HeaderRetryCount && string(h.Value) == "3" {
			found = true
		}
	}
	if !found {
		t.Error("retry header not appended when missing")
	}
}

func TestPoolSizesForMirrorsAMQPQos(t *testing.T) {
	got := PoolSizesFor(10)
	if got[kbroker.PriorityHigh] != 10 || got[kbroker.PriorityNormal] != 5 || got[kbroker.PriorityLow] != 2 {
		t.Errorf("unexpected pool split: %v", got)
	}
	// Small pool must not drop lanes to 0.
	low := PoolSizesFor(1)
	if low[kbroker.PriorityLow] < 1 || low[kbroker.PriorityNormal] < 1 {
		t.Errorf("pool=1 produced zero lane: %v", low)
	}
}
