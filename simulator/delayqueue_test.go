package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func labels(msgs []delayedMsg) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.eventType
	}
	return out
}

// The heap must hand back exactly the due events, earliest first, and hold onto the
// rest across repeated drains without losing any -- all verified against a fixed clock
// so there is no timing flakiness.
func TestDelayQueuePopDueOrdersAndRetains(t *testing.T) {
	q := newDelayQueue()
	if _, ok := q.nextDeadline(); ok {
		t.Fatalf("nextDeadline on empty queue = ok true, want false")
	}

	now := time.Unix(1_000_000, 0).UTC()
	// Pushed in scrambled fireAt order; offsets are seconds relative to now.
	pushes := []struct {
		off   int
		label string
	}{
		{-5, "due-c"},
		{3, "future-b"},
		{-30, "due-a"},
		{10, "future-c"},
		{0, "due-d"}, // fireAt == now counts as due
		{-10, "due-b"},
		{1, "future-a"},
	}
	for _, p := range pushes {
		q.push(delayedMsg{fireAt: now.Add(time.Duration(p.off) * time.Second), eventType: p.label})
	}

	// First drain: only the four due events, earliest first.
	due := labels(q.popDue(now))
	want := []string{"due-a", "due-b", "due-c", "due-d"}
	if !reflect.DeepEqual(due, want) {
		t.Fatalf("popDue(now) = %v, want %v", due, want)
	}

	// Earliest remaining is future-a at now+1s.
	dl, ok := q.nextDeadline()
	if !ok || !dl.Equal(now.Add(1*time.Second)) {
		t.Fatalf("nextDeadline = (%v, %v), want (%v, true)", dl, ok, now.Add(1*time.Second))
	}

	// Re-draining at the same instant yields nothing; the futures are still held.
	if again := q.popDue(now); len(again) != 0 {
		t.Fatalf("second popDue(now) = %v, want empty", labels(again))
	}

	// Advancing the clock releases the rest in order and empties the queue.
	if got := labels(q.popDue(now.Add(1 * time.Second))); !reflect.DeepEqual(got, []string{"future-a"}) {
		t.Fatalf("popDue(now+1s) = %v, want [future-a]", got)
	}
	if got := labels(q.popDue(now.Add(20 * time.Second))); !reflect.DeepEqual(got, []string{"future-b", "future-c"}) {
		t.Fatalf("popDue(now+20s) = %v, want [future-b future-c]", got)
	}
	if again := q.popDue(now.Add(time.Hour)); len(again) != 0 {
		t.Fatalf("popDue on drained queue = %v, want empty", labels(again))
	}
	if _, ok := q.nextDeadline(); ok {
		t.Fatalf("nextDeadline on drained queue = ok true, want false")
	}
}

// The worker drains everything pushed to it, exactly once, on a single goroutine.
func TestDelayQueueWorkerWritesAll(t *testing.T) {
	q := newDelayQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan string, 8)
	write := func(_ context.Context, _ kafka.Message, eventType string) { got <- eventType }
	go q.worker(ctx, write)

	now := time.Now()
	want := map[string]bool{"a": true, "b": true, "c": true}
	q.push(delayedMsg{fireAt: now.Add(1 * time.Millisecond), eventType: "a"})
	q.push(delayedMsg{fireAt: now.Add(5 * time.Millisecond), eventType: "b"})
	q.push(delayedMsg{fireAt: now.Add(2 * time.Millisecond), eventType: "c"})

	seen := make(map[string]bool)
	timeout := time.After(2 * time.Second)
	for len(seen) < len(want) {
		select {
		case et := <-got:
			if seen[et] {
				t.Fatalf("event %q written twice", et)
			}
			seen[et] = true
		case <-timeout:
			t.Fatalf("worker wrote %v, want %v", seen, want)
		}
	}
}

// On ctx cancellation the worker exits and pending (not-yet-due) events are dropped,
// matching the pre-existing per-event timers.
func TestDelayQueueWorkerDropsOnCancel(t *testing.T) {
	q := newDelayQueue()
	ctx, cancel := context.WithCancel(context.Background())

	got := make(chan string, 1)
	write := func(_ context.Context, _ kafka.Message, eventType string) { got <- eventType }
	go q.worker(ctx, write)

	// Far in the future so it can never fire on its own during the test.
	q.push(delayedMsg{fireAt: time.Now().Add(time.Hour), eventType: "pending"})
	cancel()

	select {
	case et := <-got:
		t.Fatalf("worker wrote %q after cancel; pending events must drop", et)
	case <-time.After(50 * time.Millisecond):
		// no write -> the pending event was dropped as expected
	}
}
