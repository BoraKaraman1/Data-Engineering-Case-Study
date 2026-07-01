package main

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// realtimeHandler is the latency-first path: it maintains Redis current-state and does
// nothing else. It is a BEST-EFFORT state projection -- it validates but does NOT
// dead-letter (the analytics path owns validation/DLQ), and it commits even if the Redis
// write fails, because the state is self-healing (the next event refreshes it) and we
// must never head-of-line-block a partition on a Redis blip.
type realtimeHandler struct {
	reg   *Registry
	state *StateStore
	m     *Metrics
}

func (h *realtimeHandler) handle(ctx context.Context, msg kafka.Message) error {
	e, derr := Decode(msg.Value)
	if derr != nil {
		h.m.ValidationErrors.WithLabelValues(derr.Rule).Inc()
		return nil // best-effort; the analytics path dead-letters it
	}
	h.m.Consumed.WithLabelValues("realtime", e.EventType).Inc()
	if verr := Validate(e, h.reg); verr != nil {
		h.m.ValidationErrors.WithLabelValues(verr.Rule).Inc()
		return nil
	}

	ts, err := time.Parse(time.RFC3339, e.Timestamp)
	if err != nil {
		return nil // already validated as parseable; stay safe
	}

	start := time.Now()
	applied, err := h.state.Apply(ctx, e, ts)
	h.m.RedisWrite.WithLabelValues("state").Observe(time.Since(start).Seconds())
	if err != nil {
		h.m.RedisErrors.WithLabelValues("state").Inc()
		return nil // best-effort: commit anyway, next event self-heals
	}
	if !applied {
		h.m.StaleSkipped.Inc()
	}
	return nil
}
