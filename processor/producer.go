package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"

	"chargesquare/processor/transform"
)

// Writers holds the two output producers: the clean topic (flat, validated, deduped)
// and the dead-letter topic (rejected raw payloads). Both are synchronous with
// RequireAll so a produce is durable before we commit the source offset -- the
// at-least-once contract. NOTE: local Redpanda runs replication factor 1, so RequireAll
// is a leader-only ack (not strong durability); production wants RF>=3.
type Writers struct {
	clean *kafka.Writer
	dlq   *kafka.Writer
}

func NewWriters(cfg *Config) *Writers {
	mk := func(topic string, batchSize int) *kafka.Writer {
		return &kafka.Writer{
			Addr:         kafka.TCP(cfg.Kafka.Brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // key=station_id -> per-station order into ClickHouse
			BatchSize:    batchSize,
			BatchTimeout: time.Duration(cfg.Kafka.LingerMs) * time.Millisecond,
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			Compression:  kafka.Snappy,
		}
	}
	// The analytics path hands the clean writer a whole batch per flush, so its BatchSize
	// must be >= analytics.batch_size or kafka-go would split one flush across several
	// produce requests; the DLQ stays at the base size (rejects are the rare path).
	cleanBatch := cfg.Kafka.BatchSize
	if cfg.Analytics.BatchSize > cleanBatch {
		cleanBatch = cfg.Analytics.BatchSize
	}
	return &Writers{clean: mk(cfg.Kafka.TopicClean, cleanBatch), dlq: mk(cfg.Kafka.TopicDLQ, cfg.Kafka.BatchSize)}
}

// WriteCleanBatch publishes a batch of flattened events to the clean topic (keyed by
// station_id) in one synchronous, RequireAll WriteMessages call. It is all-or-nothing: on
// error the caller must NOT Mark or commit, so the whole batch replays and ClickHouse's
// ReplacingMergeTree collapses the re-produced duplicates.
func (w *Writers) WriteCleanBatch(ctx context.Context, events []transform.CleanEvent) error {
	if len(events) == 0 {
		return nil
	}
	msgs := make([]kafka.Message, len(events))
	for i := range events {
		b, err := json.Marshal(events[i])
		if err != nil {
			return err
		}
		msgs[i] = kafka.Message{Key: []byte(events[i].StationID), Value: b}
	}
	return w.clean.WriteMessages(ctx, msgs...)
}

type dlqRecord struct {
	RawPayload string `json:"raw_payload"`
	Error      string `json:"error"`
	IngestedAt string `json:"ingested_at"`
}

// dlqItem is one rejected message queued for a batched dead-letter write. Rule is the
// machine label for the metrics (processor_dlq_total / _validation_errors_total); it is
// not serialised into the dead-letter record.
type dlqItem struct {
	Key   []byte
	Raw   []byte
	Rule  string
	Error string
}

// WriteDLQBatch publishes a batch of rejected payloads in one synchronous, RequireAll
// call, each keyed by its raw station_id (available even when the payload won't parse) and
// stamped with the shared processing time. Same all-or-nothing contract as the clean path.
func (w *Writers) WriteDLQBatch(ctx context.Context, items []dlqItem, ingestedAt time.Time) error {
	if len(items) == 0 {
		return nil
	}
	ts := ingestedAt.UTC().Format(transform.TimeLayout)
	msgs := make([]kafka.Message, len(items))
	for i := range items {
		b, err := json.Marshal(dlqRecord{
			RawPayload: string(items[i].Raw),
			Error:      items[i].Error,
			IngestedAt: ts,
		})
		if err != nil {
			return err
		}
		msgs[i] = kafka.Message{Key: items[i].Key, Value: b}
	}
	return w.dlq.WriteMessages(ctx, msgs...)
}

func (w *Writers) Close() error {
	err := w.clean.Close()
	if err2 := w.dlq.Close(); err2 != nil && err == nil {
		err = err2
	}
	return err
}
