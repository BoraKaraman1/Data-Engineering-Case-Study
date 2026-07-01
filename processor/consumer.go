package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// groupSpec describes one consumer group: how many reader goroutines to run and the
// per-message handler. handle returns nil to commit the offset, or an error to leave it
// uncommitted (redelivered) -- the at-least-once lever.
type groupSpec struct {
	name    string
	groupID string
	workers int
	handle  func(ctx context.Context, m kafka.Message) error
}

// batchGroupSpec is the batched analog of groupSpec: flush handles a whole slice of
// messages at once and returns nil to commit them all, or an error to leave the batch
// uncommitted (redelivered).
type batchGroupSpec struct {
	name    string
	groupID string
	workers int
	flush   func(ctx context.Context, batch []kafka.Message) error
}

func newReader(cfg *Config, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Kafka.Brokers,
		GroupID:        groupID,
		Topic:          cfg.Kafka.TopicRaw,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0, // explicit synchronous commits
		StartOffset:    kafka.FirstOffset,
	})
}

// runGroup starts spec.workers readers in the same group. The coordinator balances the
// raw topic's partitions across them, so parallelism caps at the partition count.
func runGroup(ctx context.Context, wg *sync.WaitGroup, cfg *Config, m *Metrics, spec groupSpec) {
	for i := 0; i < spec.workers; i++ {
		wg.Add(1)
		pollLagHere := i == 0 // one lag poller per group is enough (best-effort)
		go func() {
			defer wg.Done()
			r := newReader(cfg, spec.groupID)
			defer r.Close()
			if pollLagHere {
				go pollLag(ctx, r, m, spec.name)
			}
			for {
				msg, err := r.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return // shutting down
					}
					log.Printf("[%s] fetch: %v", spec.name, err)
					time.Sleep(200 * time.Millisecond)
					continue
				}
				// Process + commit on a background context so a message already in hand
				// finishes even during shutdown (no produced-but-uncommitted gap).
				pctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				if herr := spec.handle(pctx, msg); herr != nil {
					log.Printf("[%s] handle: %v", spec.name, herr)
					cancel()
					time.Sleep(200 * time.Millisecond)
					continue // not committed -> redelivered
				}
				if cerr := r.CommitMessages(pctx, msg); cerr != nil {
					log.Printf("[%s] commit: %v", spec.name, cerr)
				}
				cancel()
			}
		}()
	}
}

// runAnalyticsBatchGroup is the batched variant of runGroup for the analytics group. Each
// reader accumulates FetchMessage'd messages until the batch reaches cfg.Analytics.BatchSize
// OR cfg.Analytics.FlushMs elapses since the batch's first message, flushes the batch
// through spec.flush, and commits the whole batch's offsets on success. This amortises the
// per-event Redis/produce/commit round-trips that capped the per-message path (H1). The
// realtime group keeps runGroup unchanged.
func runAnalyticsBatchGroup(ctx context.Context, wg *sync.WaitGroup, cfg *Config, m *Metrics, spec batchGroupSpec) {
	size := cfg.Analytics.BatchSize
	window := time.Duration(cfg.Analytics.FlushMs) * time.Millisecond
	for i := 0; i < spec.workers; i++ {
		wg.Add(1)
		pollLagHere := i == 0 // one lag poller per group is enough (best-effort)
		go func() {
			defer wg.Done()
			r := newReader(cfg, spec.groupID)
			defer func() { r.Close() }()
			if pollLagHere {
				go pollLag(ctx, r, m, spec.name)
			}
			batch := make([]kafka.Message, 0, size)
			for {
				// Block for the first message on the parent context; the flush window is
				// measured from its arrival.
				first, err := r.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return // shutting down
					}
					log.Printf("[%s] fetch: %v", spec.name, err)
					time.Sleep(200 * time.Millisecond)
					continue
				}
				batch = append(batch[:0], first)

				// Fill until the batch is full or the flush window elapses (a fetch on the
				// deadline'd context returns an error, which just ends accumulation).
				fctx, fcancel := context.WithDeadline(ctx, time.Now().Add(window))
				for len(batch) < size {
					msg, ferr := r.FetchMessage(fctx)
					if ferr != nil {
						break
					}
					batch = append(batch, msg)
				}
				fcancel()

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
					r = newReader(cfg, spec.groupID)
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
