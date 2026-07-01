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
	mk := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:         kafka.TCP(cfg.Kafka.Brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // key=station_id -> per-station order into ClickHouse
			BatchSize:    cfg.Kafka.BatchSize,
			BatchTimeout: time.Duration(cfg.Kafka.LingerMs) * time.Millisecond,
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			Compression:  kafka.Snappy,
		}
	}
	return &Writers{clean: mk(cfg.Kafka.TopicClean), dlq: mk(cfg.Kafka.TopicDLQ)}
}

// WriteClean publishes a flattened event to the clean topic, keyed by station_id.
func (w *Writers) WriteClean(ctx context.Context, ce transform.CleanEvent) error {
	b, err := json.Marshal(ce)
	if err != nil {
		return err
	}
	return w.clean.WriteMessages(ctx, kafka.Message{Key: []byte(ce.StationID), Value: b})
}

type dlqRecord struct {
	RawPayload string `json:"raw_payload"`
	Error      string `json:"error"`
	IngestedAt string `json:"ingested_at"`
}

// WriteDLQ publishes a rejected payload with the reason and processing time. The key is
// the raw message key (station_id), which is available even when the payload won't parse.
func (w *Writers) WriteDLQ(ctx context.Context, key, raw []byte, errStr string, ingestedAt time.Time) error {
	rec := dlqRecord{
		RawPayload: string(raw),
		Error:      errStr,
		IngestedAt: ingestedAt.UTC().Format(transform.TimeLayout),
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return w.dlq.WriteMessages(ctx, kafka.Message{Key: key, Value: b})
}

func (w *Writers) Close() error {
	err := w.clean.Close()
	if err2 := w.dlq.Close(); err2 != nil && err == nil {
		err = err2
	}
	return err
}
