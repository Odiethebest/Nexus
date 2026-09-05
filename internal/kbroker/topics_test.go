package kbroker

import "testing"

func TestTopicName(t *testing.T) {
	cases := []struct {
		ch   Channel
		p    Priority
		want string
	}{
		{ChannelEmail, PriorityHigh, "nexus.email.high"},
		{ChannelInApp, PriorityNormal, "nexus.inapp.normal"},
		{ChannelWebhook, PriorityLow, "nexus.webhook.low"},
	}
	for _, c := range cases {
		if got := TopicName(c.ch, c.p); got != c.want {
			t.Errorf("TopicName(%s,%s)=%s want %s", c.ch, c.p, got, c.want)
		}
	}
}

func TestDLQTopic(t *testing.T) {
	if got := DLQTopic(ChannelEmail, PriorityHigh); got != "nexus.dlq.email.high" {
		t.Errorf("DLQTopic email/high = %s", got)
	}
	if got := DLQTopic(ChannelWebhook, PriorityLow); got != "nexus.dlq.webhook.low" {
		t.Errorf("DLQTopic webhook/low = %s", got)
	}
}

func TestConsumerGroupIsPerLane(t *testing.T) {
	seen := map[string]struct{}{}
	for _, ch := range Channels {
		for _, p := range Priorities {
			g := ConsumerGroup(ch, p)
			if _, dup := seen[g]; dup {
				t.Fatalf("duplicate consumer group %q", g)
			}
			seen[g] = struct{}{}
		}
	}
	if len(seen) != len(Channels)*len(Priorities) {
		t.Fatalf("expected %d unique groups, got %d", len(Channels)*len(Priorities), len(seen))
	}
}

func TestAllTopicsAndDLQTopics(t *testing.T) {
	if got := len(AllTopics()); got != 9 {
		t.Errorf("AllTopics len=%d want 9", got)
	}
	if got := len(AllDLQTopics()); got != 9 {
		t.Errorf("AllDLQTopics len=%d want 9", got)
	}
}

func TestNormalizeDLQTopic(t *testing.T) {
	cases := map[string]string{
		"nexus.dlq.email.high":       "nexus.dlq.email.high", // already normalized
		"nexus.email.dlq.high":       "nexus.dlq.email.high", // legacy AMQP form
		"nexus.webhook.dlq.low":      "nexus.dlq.webhook.low",
		"  nexus.inapp.dlq.normal  ": "nexus.dlq.inapp.normal",
		"random-string":              "random-string", // untouched
	}
	for in, want := range cases {
		if got := NormalizeDLQTopic(in); got != want {
			t.Errorf("NormalizeDLQTopic(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPrimaryFromDLQ(t *testing.T) {
	primary, ok := PrimaryFromDLQ("nexus.dlq.webhook.normal")
	if !ok || primary != "nexus.webhook.normal" {
		t.Errorf("PrimaryFromDLQ webhook/normal = %q, ok=%v", primary, ok)
	}
	if _, ok := PrimaryFromDLQ("nexus.webhook.high"); ok {
		t.Error("PrimaryFromDLQ should reject non-DLQ topic")
	}
}
