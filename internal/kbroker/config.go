package kbroker

import (
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config captures the operator-tunable pieces of the Redpanda connection.
// Everything is read from environment variables so the same binary boots
// against docker-compose (plaintext) and Redpanda Cloud (SASL_SSL) with only
// config differences.
type Config struct {
	Brokers           []string
	ClientID          string
	TopicPartitions   int32
	ReplicationFactor int16
	SASLUser          string
	SASLPass          string
	SASLMechanism     string // "SCRAM-SHA-256" | "SCRAM-SHA-512" | "" (plaintext)
	TLS               bool
}

// LoadConfig reads env vars once. Missing KAFKA_BROKERS is a hard error —
// there is no default because pointing at the wrong cluster silently is
// worse than failing to start.
func LoadConfig() (Config, error) {
	brokersRaw := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersRaw == "" {
		return Config{}, fmt.Errorf("kbroker: KAFKA_BROKERS is required (e.g. localhost:19092)")
	}
	brokers := splitCSV(brokersRaw)

	partitions := int32(12) // default matches README derivation for 50K/s target
	if raw := os.Getenv("KAFKA_TOPIC_PARTITIONS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("kbroker: invalid KAFKA_TOPIC_PARTITIONS=%q", raw)
		}
		partitions = int32(n)
	}

	rf := int16(1) // default suits single-node Redpanda for local/dev
	if raw := os.Getenv("KAFKA_REPLICATION_FACTOR"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("kbroker: invalid KAFKA_REPLICATION_FACTOR=%q", raw)
		}
		rf = int16(n)
	}

	clientID := os.Getenv("KAFKA_CLIENT_ID")
	if clientID == "" {
		clientID = "nexus"
	}

	mech := strings.ToUpper(strings.TrimSpace(os.Getenv("KAFKA_SASL_MECHANISM")))
	if mech == "" && os.Getenv("KAFKA_SASL_USER") != "" {
		mech = "SCRAM-SHA-256"
	}

	return Config{
		Brokers:           brokers,
		ClientID:          clientID,
		TopicPartitions:   partitions,
		ReplicationFactor: rf,
		SASLUser:          os.Getenv("KAFKA_SASL_USER"),
		SASLPass:          os.Getenv("KAFKA_SASL_PASS"),
		SASLMechanism:     mech,
		TLS:               strings.EqualFold(os.Getenv("KAFKA_TLS"), "true"),
	}, nil
}

// BaseOpts returns the shared kgo options used by every client (producer,
// worker, replayer, lag reader). Callers append their own role-specific
// options (producer acks, consumer group, etc.).
func (c Config) BaseOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
	}
	if c.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	if c.SASLUser != "" {
		auth := scram.Auth{User: c.SASLUser, Pass: c.SASLPass}
		switch c.SASLMechanism {
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(auth.AsSha512Mechanism()))
		default: // SCRAM-SHA-256
			opts = append(opts, kgo.SASL(auth.AsSha256Mechanism()))
		}
	}
	return opts
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}
