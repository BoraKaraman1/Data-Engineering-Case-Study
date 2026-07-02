package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// batchGroupSpec describes the analytics consumer group: how many reader goroutines to run
// and a flush that handles a whole slice of messages at once, returning nil to commit them
// all or an error to leave the batch uncommitted (redelivered) -- the at-least-once lever.
type batchGroupSpec struct {
	name    string
	groupID string
	workers int
	flush   func(ctx context.Context, batch []kafka.Message) error
}

// realtimeBatchSpec describes the realtime consumer group. apply mirrors batchGroupSpec's
// flush but returns nothing: the realtime path is best-effort and commits every batch
// regardless of the apply result (current state self-heals from the next event), so unlike
// analytics it has no redeliver-on-error lever.
type realtimeBatchSpec struct {
	name    string
	groupID string
	workers int
	apply   func(ctx context.Context, batch []kafka.Message)
}

func newReader(cfg *Config, groupID string, tune readerTuning) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Kafka.Brokers,
		GroupID:        groupID,
		Topic:          cfg.Kafka.TopicRaw,
		MinBytes:       tune.MinBytes,
		MaxBytes:       10e6,
		MaxWait:        time.Duration(tune.MaxWaitMs) * time.Millisecond,
		CommitInterval: 0, // explicit synchronous commits
		StartOffset:    kafka.FirstOffset,
	})
}

// flushReason records why a batch flushed: it reached the size cap (N) or the wait window
// elapsed (T). It labels the realtime flush-reason counter.
type flushReason int

const (
	flushBySize flushReason = iota
	flushByTimer
)

func (r flushReason) String() string {
	if r == flushByTimer {
		return "timer"
	}
	return "size"
}

// fillBatch is the bounded opportunistic min(N, T) accumulation shared by both batch consumer
// groups. It blocks for the first message on ctx (the flush window is measured from that
// message's arrival), then fills buf until the batch holds size messages (flushBySize) or
// window elapses since the first message (flushByTimer), whichever comes first. buf is reused
// across calls to avoid reallocating. A fetch error on the FIRST message (e.g. ctx cancelled
// during shutdown) is returned with an empty batch so the caller can exit or back off; a
// cancel DURING the fill just ends accumulation and returns the partial batch (nil error), so
// the caller flushes the in-hand batch on shutdown. The ONLY realtime-vs-analytics difference
// -- the flush+commit policy -- stays with the caller.
func fillBatch(ctx context.Context, r *kafka.Reader, buf []kafka.Message, size int, window time.Duration) ([]kafka.Message, flushReason, error) {
	first, err := r.FetchMessage(ctx)
	if err != nil {
		return buf[:0], flushBySize, err
	}
	batch := append(buf[:0], first)
	fctx, fcancel := context.WithDeadline(ctx, time.Now().Add(window))
	defer fcancel()
	for len(batch) < size {
		msg, ferr := r.FetchMessage(fctx)
		if ferr != nil {
			return batch, flushByTimer, nil
		}
		batch = append(batch, msg)
	}
	return batch, flushBySize, nil
}

// runAnalyticsBatchGroup runs the analytics group: each reader accumulates a size-or-time
// batch via fillBatch, flushes it through spec.flush, and commits the whole batch's offsets
// on success. This amortises the per-event Redis/produce/commit round-trips that capped the
// per-message path (H1). Commit policy: NEVER commit before a durable produce -- on a flush
// error the batch is left uncommitted and redelivered.
func runAnalyticsBatchGroup(ctx context.Context, wg *sync.WaitGroup, cfg *Config, m *Metrics, spec batchGroupSpec) {
	size := cfg.Analytics.BatchSize
	window := time.Duration(cfg.Analytics.FlushMs) * time.Millisecond
	for i := 0; i < spec.workers; i++ {
		wg.Add(1)
		pollLagHere := i == 0 // one lag poller per group is enough (best-effort)
		go func() {
			defer wg.Done()
			r := newReader(cfg, spec.groupID, cfg.Readers.Analytics)
			defer func() { r.Close() }()
			if pollLagHere {
				go pollLag(ctx, r, m, spec.name)
			}
			batch := make([]kafka.Message, 0, size)
			for {
				var err error
				batch, _, err = fillBatch(ctx, r, batch, size, window)
				if err != nil {
					if ctx.Err() != nil {
						return // shutting down
					}
					log.Printf("[%s] fetch: %v", spec.name, err)
					time.Sleep(200 * time.Millisecond)
					continue
				}

				// Flush + commit on a background context so an in-hand batch finishes even
				// during shutdown (no produced-but-uncommitted gap).
				pctx, pcancel := context.WithTimeout(context.Background(), 30*time.Second)
				if herr := spec.flush(pctx, batch); herr != nil {
					log.Printf("[%s] flush: %v", spec.name, herr)
					pcancel()
					// Uncommitted: rejoin from the last commit so the batch is redelivered
					// (kafka-go only redelivers uncommitted offsets on a new generation),
					// then retry. Only a durable-produce failure reaches here.
					r.Close()
					r = newReader(cfg, spec.groupID, cfg.Readers.Analytics)
					time.Sleep(200 * time.Millisecond)
					continue
				}
				if cerr := r.CommitMessages(pctx, batch...); cerr != nil {
					log.Printf("[%s] commit: %v", spec.name, cerr)
				}
				pcancel()
			}
		}()
	}
}

// runRealtimeBatchGroup runs the realtime group. It shares fillBatch's bounded min(N, T)
// accumulation with the analytics group; the ONLY difference is the flush+commit policy.
// Realtime applies the batch's current-state CAS in one pipelined round trip and then commits
// the batch's offsets REGARDLESS of the apply result (best-effort), which lifts the per-event
// Redis-RTT ceiling while keeping tail freshness well under the <1 s SLA at load: T=25 ms
// leaves ample budget for the CAS pipeline + Redis RTT. N (batch_max_messages) caps tail
// latency; T (batch_max_wait_ms) bounds how long an already-open batch waits client-side, so a
// fetched event is applied within T instead of stalling until N fills. (T governs client-side
// accumulation only; the realtime reader is tuned separately -- MinBytes=1, MaxWait~50ms via
// cfg.Readers.Realtime -- so a lone trickle event is fetched and applied promptly instead of
// stalling on kafka-go's 10s broker-fetch default, which the old shared reader hit at trickle.)
func runRealtimeBatchGroup(ctx context.Context, wg *sync.WaitGroup, cfg *Config, m *Metrics, spec realtimeBatchSpec) {
	size := cfg.Realtime.BatchMaxMessages
	window := time.Duration(cfg.Realtime.BatchMaxWaitMs) * time.Millisecond
	for i := 0; i < spec.workers; i++ {
		wg.Add(1)
		pollLagHere := i == 0 // one lag poller per group is enough (best-effort)
		go func() {
			defer wg.Done()
			r := newReader(cfg, spec.groupID, cfg.Readers.Realtime)
			defer r.Close()
			if pollLagHere {
				go pollLag(ctx, r, m, spec.name)
			}
			batch := make([]kafka.Message, 0, size)
			for {
				var reason flushReason
				var err error
				batch, reason, err = fillBatch(ctx, r, batch, size, window)
				if err != nil {
					if ctx.Err() != nil {
						return // shutting down
					}
					log.Printf("[%s] fetch: %v", spec.name, err)
					time.Sleep(200 * time.Millisecond)
					continue
				}

				// Apply + commit on a background context so an in-hand batch finishes even
				// during shutdown -- this flushes the last T ms of state updates before the
				// worker exits, so a deploy does not drop them.
				pctx, pcancel := context.WithTimeout(context.Background(), 15*time.Second)
				spec.apply(pctx, batch)
				// DELIBERATE, and different from the analytics group: commit even if apply
				// failed. Realtime current state is self-healing (the next event refreshes
				// it), so losing a batch of sub-second-old updates is acceptable, whereas the
				// "never commit before a durable write" invariant would head-of-line-block the
				// partition on a Redis blip and make every connector on it stale.
				if cerr := r.CommitMessages(pctx, batch...); cerr != nil {
					log.Printf("[%s] commit: %v", spec.name, cerr)
				}
				pcancel()
				m.RealtimeBatch.Observe(float64(len(batch)))
				m.RealtimeFlush.WithLabelValues(reason.String()).Inc()
			}
		}()
	}
}

// pollLag mirrors the reader's lag into a gauge. Best-effort (kafka-go computes it from
// the last fetch); authoritative consumer lag comes from `rpk group describe` in Phase 4.
func pollLag(ctx context.Context, r *kafka.Reader, m *Metrics, group string) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// kafka-go's group-reader Lag() is best-effort and returns -1 before the
			// first fetch; skip those so the gauge never shows a bogus negative. The
			// authoritative lag for Task 4 comes from `rpk group describe`.
			if l := r.Lag(); l >= 0 {
				m.ConsumerLag.WithLabelValues(group).Set(float64(l))
			}
		}
	}
}
