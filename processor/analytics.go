package main

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"

	"chargesquare/processor/transform"
)

// analyticsHandler is the throughput-first, AUTHORITATIVE path. It processes a whole batch
// per flush to amortise the round-trips that capped the per-event path (H1): one pipelined
// Redis EXISTS, one clean-topic produce, one DLQ produce, one pipelined Redis SET, and one
// offset commit for the entire batch. The ordering is preserved end to end --
// Seen -> produce -> Mark -> commit -- so a crash between any two steps re-produces a
// duplicate (collapsed by ClickHouse's ReplacingMergeTree) rather than dropping a unique
// event. flush returns an error ONLY when a produce genuinely fails, so the batch replays
// (uncommitted) instead of being lost.
type analyticsHandler struct {
	reg     *Registry
	dedup   *Deduper
	writers *Writers
	m       *Metrics
}

// validEvent is a decoded, validated, pre-flattened batch member awaiting dedup + produce.
type validEvent struct {
	clean   transform.CleanEvent
	msgTime time.Time // Kafka produce time, for the wall-clock transport-lag SLO
	ts      time.Time // event time, parsed once by Validate, for the ingestion-lag metric
}

func (h *analyticsHandler) flush(ctx context.Context, batch []kafka.Message) error {
	ingestedAt := time.Now()

	// 1. Decode + validate each message: valid -> flattened row, invalid -> dead-letter.
	valid := make([]validEvent, 0, len(batch))
	var dlqItems []dlqItem
	for i := range batch {
		e, verr := Decode(batch[i].Value)
		var ts time.Time
		if verr == nil {
			ts, verr = Validate(e, h.reg)
		}
		if verr != nil {
			dlqItems = append(dlqItems, dlqItem{
				Key: batch[i].Key, Raw: batch[i].Value, Rule: verr.Rule, Error: verr.Msg,
			})
			continue
		}
		ce := transform.Flatten(e, ingestedAt)
		// Stamp the produce->store-write lag anchor from the raw Kafka produce time.
		ce.ProducedAt = batch[i].Time.UTC().Format(transform.TimeLayout)
		valid = append(valid, validEvent{clean: ce, msgTime: batch[i].Time, ts: ts})
	}

	// In-batch dedup: keep the first occurrence of each event_id so intra-batch duplicates
	// never reach the clean topic (ClickHouse would collapse them, but dropping is cheaper).
	// Per-event metrics are emitted once, after the durable produce, so a redelivered batch
	// is not double-counted.
	inBatch := make(map[string]struct{}, len(valid))
	uniq := make([]validEvent, 0, len(valid))
	intraDupes := 0
	for i := range valid {
		id := valid[i].clean.EventID
		if _, dup := inBatch[id]; dup {
			intraDupes++
			continue
		}
		inBatch[id] = struct{}{}
		uniq = append(uniq, valid[i])
	}

	// 2. Batch dedup: one pipelined EXISTS for the unique event_ids. Fails OPEN (produce
	// everything and let ClickHouse dedup) rather than head-of-line-blocking on Redis.
	ids := make([]string, len(uniq))
	for i := range uniq {
		ids[i] = uniq[i].clean.EventID
	}
	ds := time.Now()
	seen, derr := h.dedup.SeenBatch(ctx, ids)
	h.m.RedisWrite.WithLabelValues("dedup").Observe(time.Since(ds).Seconds())
	if derr != nil {
		h.m.RedisErrors.WithLabelValues("dedup").Inc()
		seen = nil // fail open: treat all as fresh
	}

	// 3. fresh = valid & !seen; dupes (in-batch + cross-batch) are dropped and counted below.
	fresh := make([]transform.CleanEvent, 0, len(uniq))
	freshIDs := make([]string, 0, len(uniq))
	crossDupes := 0
	for i := range uniq {
		if seen != nil && seen[i] {
			crossDupes++
			continue
		}
		fresh = append(fresh, uniq[i].clean)
		freshIDs = append(freshIDs, uniq[i].clean.EventID)
	}

	// 4. Produce both outputs durably. On ANY produce error, return WITHOUT Mark/commit and
	// WITHOUT emitting any per-batch metric, so a redelivered batch is counted exactly once
	// on its eventual success -- never a Mark or commit ahead of a durable produce.
	ps := time.Now()
	if err := h.writers.WriteCleanBatch(ctx, fresh); err != nil {
		return err
	}
	if len(fresh) > 0 {
		h.m.CleanProduce.Observe(time.Since(ps).Seconds())
	}
	if err := h.writers.WriteDLQBatch(ctx, dlqItems, ingestedAt); err != nil {
		return err
	}

	// 5. Both produces are durable: emit the batch's metrics exactly once. Per valid event,
	// count it and observe its lag (transport lag, the wall-clock SLO, measured to the point
	// the event is durably in the clean topic).
	for i := range valid {
		ce := valid[i].clean
		h.m.Consumed.WithLabelValues("analytics", ce.EventType).Inc()
		h.m.TransportLag.Observe(time.Since(valid[i].msgTime).Seconds())
		h.m.IngestionLag.Observe(ingestedAt.Sub(valid[i].ts).Seconds())
	}
	if d := intraDupes + crossDupes; d > 0 {
		h.m.DuplicatesDropped.Add(float64(d))
	}
	h.m.CleanProduced.Add(float64(len(fresh)))
	for i := range dlqItems {
		reason := "validation"
		if dlqItems[i].Rule == "invalid_json" {
			reason = "invalid_json"
		}
		h.m.DLQ.WithLabelValues(reason).Inc()
		h.m.ValidationErrors.WithLabelValues(dlqItems[i].Rule).Inc()
	}

	// 6. Mark AFTER both durable produces (best-effort: log via metric, never block the
	// commit -- ClickHouse is the authoritative dedup backstop).
	if err := h.dedup.MarkBatch(ctx, freshIDs); err != nil {
		h.m.RedisErrors.WithLabelValues("dedup").Inc()
	}
	h.m.AnalyticsBatch.Observe(float64(len(batch)))
	return nil // 7. caller commits the whole batch's offsets
}
