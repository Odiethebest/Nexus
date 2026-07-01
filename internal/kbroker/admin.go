package kbroker

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// EnsureTopics creates every primary lane topic and DLQ topic if it does
// not already exist. Idempotent — TOPIC_ALREADY_EXISTS is swallowed. Called
// once at Producer boot; workers assume topics already exist.
func EnsureTopics(ctx context.Context, cfg Config) error {
	client, err := kgo.NewClient(cfg.BaseOpts()...)
	if err != nil {
		return fmt.Errorf("kbroker: admin client: %w", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)

	all := append(AllTopics(), AllDLQTopics()...)
	resp, err := admin.CreateTopics(ctx, cfg.TopicPartitions, cfg.ReplicationFactor, nil, all...)
	if err != nil {
		return fmt.Errorf("kbroker: create topics: %w", err)
	}
	for _, r := range resp {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kbroker: create topic %s: %w", r.Topic, r.Err)
		}
	}
	return nil
}
