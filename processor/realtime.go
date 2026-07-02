package main

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// realtimeRetryBackoff is the short pause before the single retry of a failed CAS pipeline.
// Kept small because the realtime path is latency-sensitive and self-heals: the caller
// commits the batch regardless of the retry's outcome (see runRealtimeBatchGroup).
const realtimeRetryBackoff = 50 * time.Millisecond

// realtimeHandler is the latency-first path: it maintains Redis current-state and does
// nothing else. It is a BEST-EFFORT state projection -- it validates but does NOT
// dead-letter (the analytics path owns validation/DLQ), and its caller commits even if the
// Redis write fails, because the state is self-healing (the next event refreshes it) and we
// must never head-of-line-block a partition on a Redis blip. It processes a whole batch per
// flush (H2): one pipelined CAS across the batch and one offset commit, amortising the
// per-event Redis round-trip that capped the per-message path at ~1k ev/s.
type realtimeHandler struct {
	reg   *Registry
	state *StateStore
	m     *Metrics
}

// applyBatch is the batch analog of the old per-message handle. It decodes and validates
// each message IN OFFSET ORDER, skips invalids (best-effort: count + drop, no DLQ), collects
// the valid (event, ts) pairs PRESERVING offset order, and applies the whole batch to Redis
// in one pipelined round trip. It never returns an error: the caller commits the batch's
// offsets regardless (best-effort), so a Redis blip is retried once and then absorbed rather
// than blocking the partition.
func (h *realtimeHandler) applyBatch(ctx context.Context, batch []kafka.Message) {
	// Decode + validate in offset order; invalid -> count + SKIP. Offset order == arrival
	// order under station-keyed partitioning (one connector -> one partition -> one worker),
	// so keeping this order is what lets ApplyBatch's per-connector CAS keep the newest state.
	events := make([]stateEvent, 0, len(batch))
	for i := range batch {
		e, derr := Decode(batch[i].Value)
		if derr != nil {
			h.m.RealtimeInvalidSkipped.Inc()
			continue // best-effort; the analytics path dead-letters it
		}
		h.m.Consumed.WithLabelValues("realtime", e.EventType).Inc()
		if verr := Validate(e, h.reg); verr != nil {
			h.m.RealtimeInvalidSkipped.Inc()
			continue
		}
		ts, terr := time.Parse(time.RFC3339, e.Timestamp)
		if terr != nil {
			continue // already validated as parseable; stay safe
		}
		events = append(events, stateEvent{e: e, ts: ts, produced: batch[i].Time})
	}
	if len(events) == 0 {
		return
	}

	// One pipelined CAS for the whole batch. On a Redis error, retry the pipeline ONCE after
	// a short backoff, then give up -- the caller commits regardless. Re-applying the same
	// events is idempotent under the CAS (the newest wins, older ones are rejected), so the
	// retry cannot corrupt state.
	start := time.Now()
	applied, err := h.state.ApplyBatch(ctx, events)
	h.m.RedisWrite.WithLabelValues("state").Observe(time.Since(start).Seconds())
	if err != nil {
		h.m.RedisErrors.WithLabelValues("state").Inc()
		time.Sleep(realtimeRetryBackoff)
		start = time.Now()
		applied, err = h.state.ApplyBatch(ctx, events)
		h.m.RedisWrite.WithLabelValues("state").Observe(time.Since(start).Seconds())
		if err != nil {
			h.m.RedisErrors.WithLabelValues("state").Inc()
			return // self-healing: next event refreshes state; caller commits anyway
		}
	}

	// Observe the produce->apply store-write lag for events that actually landed (a stale
	// event was superseded by a newer write, so it doesn't reflect current-state freshness).
	// Measured after the CAS returns, so any retry backoff is included honestly.
	applyNow := time.Now()
	stale := 0
	for i, ok := range applied {
		if ok {
			h.m.StateApplyLag.Observe(applyNow.Sub(events[i].produced).Seconds())
		} else {
			stale++
		}
	}
	if stale > 0 {
		h.m.StaleSkipped.Add(float64(stale))
	}
}
