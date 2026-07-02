package main

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// delayedMsg is one out-of-order event held back until fireAt (enqueue time + delay).
type delayedMsg struct {
	fireAt    time.Time
	msg       kafka.Message
	eventType string
}

// delayHeap is a min-heap of delayedMsg ordered by fireAt so the earliest-due event
// is always at index 0.
type delayHeap []delayedMsg

func (h delayHeap) Len() int           { return len(h) }
func (h delayHeap) Less(i, j int) bool { return h[i].fireAt.Before(h[j].fireAt) }
func (h delayHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *delayHeap) Push(x any) { *h = append(*h, x.(delayedMsg)) }

func (h *delayHeap) Pop() any {
	old := *h
	n := len(old)
	d := old[n-1]
	old[n-1] = delayedMsg{} // drop the reference so the message bytes can be GC'd
	*h = old[:n-1]
	return d
}

// delayQueue holds all pending out-of-order events on one shared min-heap, drained by
// a single worker goroutine. This replaces the former one-goroutine-plus-timer per
// delayed event, which could pile up thousands of both at high ingest rates.
type delayQueue struct {
	mu   sync.Mutex
	h    delayHeap
	wake chan struct{}
}

func newDelayQueue() *delayQueue {
	return &delayQueue{wake: make(chan struct{}, 1)}
}

// push enqueues d and nudges the worker. The wake channel is buffered to one and the
// send is non-blocking, so a coalesced signal is enough: the worker always recomputes
// the earliest deadline from the heap after waking.
func (q *delayQueue) push(d delayedMsg) {
	q.mu.Lock()
	heap.Push(&q.h, d)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// popDue removes and returns every event whose fireAt is at or before now, earliest
// first, leaving later events on the heap.
func (q *delayQueue) popDue(now time.Time) []delayedMsg {
	q.mu.Lock()
	defer q.mu.Unlock()
	var due []delayedMsg
	for len(q.h) > 0 && !q.h[0].fireAt.After(now) {
		due = append(due, heap.Pop(&q.h).(delayedMsg))
	}
	return due
}

// nextDeadline peeks the earliest fireAt; ok is false when the queue is empty.
func (q *delayQueue) nextDeadline() (time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.h) == 0 {
		return time.Time{}, false
	}
	return q.h[0].fireAt, true
}

// worker drains the queue on a single goroutine: it sleeps until the earliest deadline
// (or until push() wakes it with a nearer item, or ctx is cancelled) and then writes
// everything now due via write. On ctx cancellation it returns, dropping any pending
// events -- the same as the per-event timers did before.
//
// One timer is reused across iterations; Go 1.23+ guarantees Reset/Stop drop any value
// prepared before the call, so the channel needs no manual draining.
func (q *delayQueue) worker(ctx context.Context, write func(context.Context, kafka.Message, string)) {
	t := time.NewTimer(time.Hour)
	defer t.Stop()
	for {
		if deadline, ok := q.nextDeadline(); ok {
			t.Reset(time.Until(deadline))
		} else {
			t.Stop() // queue empty: park until a push or cancellation
		}
		select {
		case <-ctx.Done():
			return
		case <-q.wake:
			// A push arrived; loop to recompute the earliest deadline.
		case <-t.C:
			for _, d := range q.popDue(time.Now()) {
				write(ctx, d.msg, d.eventType)
			}
		}
	}
}
