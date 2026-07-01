package main

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"

	"chargesquare/processor/transform"
)

// analyticsHandler is the throughput-first, AUTHORITATIVE path: validate -> dead-letter
// invalid, dedup -> produce valid to the clean topic. Dedup ordering is
// EXISTS -> produce -> Mark (see Deduper): marking only after a durable produce means a
// crash re-produces a duplicate (collapsed by ClickHouse) rather than dropping a unique
// event. It returns an error ONLY when a produce genuinely fails, so that message is
// redelivered instead of lost.
type analyticsHandler struct {
	reg     *Registry
	dedup   *Deduper
	writers *Writers
	m       *Metrics
}

func (h *analyticsHandler) handle(ctx context.Context, msg kafka.Message) error {
	ingestedAt := time.Now()

	e, verr := Decode(msg.Value)
	if verr == nil {
		verr = Validate(e, h.reg)
	}
	if verr != nil {
		if err := h.writers.WriteDLQ(ctx, msg.Key, msg.Value, verr.Msg, ingestedAt); err != nil {
			return err // couldn't dead-letter -> redeliver rather than drop
		}
		reason := "validation"
		if verr.Rule == "invalid_json" {
			reason = "invalid_json"
		}
		h.m.DLQ.WithLabelValues(reason).Inc()
		h.m.ValidationErrors.WithLabelValues(verr.Rule).Inc()
		return nil
	}

	h.m.Consumed.WithLabelValues("analytics", e.EventType).Inc()

	// Lag metrics. Transport lag (wall-clock, from the Kafka produce time) is the real
	// SLO; ingestion lag (event-time to ingest) is only meaningful at accel=1.
	h.m.TransportLag.Observe(time.Since(msg.Time).Seconds())
	if ts, err := time.Parse(time.RFC3339, e.Timestamp); err == nil {
		h.m.IngestionLag.Observe(ingestedAt.Sub(ts).Seconds())
	}

	// Best-effort dedup, fails OPEN (ClickHouse is the correctness backstop).
	ds := time.Now()
	seen, derr := h.dedup.Seen(ctx, e.EventID)
	h.m.RedisWrite.WithLabelValues("dedup").Observe(time.Since(ds).Seconds())
	if derr != nil {
		h.m.RedisErrors.WithLabelValues("dedup").Inc()
	} else if seen {
		h.m.DuplicatesDropped.Inc()
		return nil // in-window duplicate -> drop, commit
	}

	ps := time.Now()
	if err := h.writers.WriteClean(ctx, transform.Flatten(e, ingestedAt)); err != nil {
		return err // produce failed -> redeliver
	}
	h.m.CleanProduce.Observe(time.Since(ps).Seconds())
	h.m.CleanProduced.Inc()

	// Mark AFTER the durable produce so a crash re-produces (dup, collapsed) not drops.
	if err := h.dedup.Mark(ctx, e.EventID); err != nil {
		h.m.RedisErrors.WithLabelValues("dedup").Inc() // benign: ClickHouse still dedups
	}
	return nil
}
