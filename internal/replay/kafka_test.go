package replay

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"nexus/internal/kbroker"
)

func TestRebuildForPrimaryResetsRetryAndPreservesRest(t *testing.T) {
	src := &kgo.Record{
		Topic: "nexus.dlq.webhook.high",
		Key:   []byte("m1"),
		Value: []byte(`{"x":1}`),
		Headers: []kgo.RecordHeader{
			{Key: kbroker.HeaderMsgID, Value: []byte("m1")},
			{Key: kbroker.HeaderEventType, Value: []byte("payment.completed")},
			{Key: kbroker.HeaderPriority, Value: []byte("high")},
			{Key: kbroker.HeaderProducedAt, Value: []byte("1700000000000000000")},
			{Key: kbroker.HeaderRetryCount, Value: []byte("3")},
		},
	}
	out := rebuildForPrimary(src, "nexus.webhook.high")
	if out.Topic != "nexus.webhook.high" {
		t.Errorf("primary topic = %s", out.Topic)
	}
	got := map[string]string{}
	for _, h := range out.Headers {
		got[h.Key] = string(h.Value)
	}
	if got[kbroker.HeaderRetryCount] != "0" {
		t.Errorf("retry-count not reset: %q", got[kbroker.HeaderRetryCount])
	}
	// x-produced-at is preserved so e2e lag reflects true event age,
	// including the time spent parked in the DLQ.
	if got[kbroker.HeaderProducedAt] != "1700000000000000000" {
		t.Errorf("produced-at dropped: %q", got[kbroker.HeaderProducedAt])
	}
	if got[kbroker.HeaderEventType] != "payment.completed" {
		t.Errorf("event-type dropped")
	}
}

func TestRebuildForPrimaryAppendsRetryWhenMissing(t *testing.T) {
	out := rebuildForPrimary(&kgo.Record{}, "nexus.inapp.low")
	found := false
	for _, h := range out.Headers {
		if h.Key == kbroker.HeaderRetryCount && string(h.Value) == "0" {
			found = true
		}
	}
	if !found {
		t.Error("missing retry-count header after rebuild")
	}
}
