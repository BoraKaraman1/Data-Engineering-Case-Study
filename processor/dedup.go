package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Deduper is the best-effort, hot-path dedup layer. It is an OPTIMIZATION that sheds the
// bulk of in-window duplicates so they never reach the clean topic or ClickHouse; it is
// NOT the correctness authority -- ClickHouse's ReplacingMergeTree + exact reads are.
// The analytics handler therefore orders it EXISTS -> produce -> Mark: marking only
// after a durable produce means a crash re-produces a duplicate (which ClickHouse
// collapses) rather than dropping a unique event. Duplicates of one event share
// station_id -> one partition -> one worker, so Seen and Mark never race for a key.
type Deduper struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewDeduper(rdb *redis.Client, ttl time.Duration) *Deduper {
	return &Deduper{rdb: rdb, ttl: ttl}
}

func dedupKey(eventID string) string { return "dedup:" + eventID }

// SeenBatch reports, per event_id and in input order, whether it was already marked within
// the TTL window. It issues one pipelined EXISTS round-trip for the whole slice, so the
// batch pays a single Redis RTT instead of one per event. Fails as a unit: on a pipeline
// error the caller treats every id as not-seen (fail open -- ClickHouse is the authority).
func (d *Deduper) SeenBatch(ctx context.Context, eventIDs []string) ([]bool, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	pipe := d.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, len(eventIDs))
	for i, id := range eventIDs {
		cmds[i] = pipe.Exists(ctx, dedupKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	seen := make([]bool, len(eventIDs))
	for i, c := range cmds {
		seen[i] = c.Val() > 0
	}
	return seen, nil
}

// MarkBatch claims a set of event_ids for the TTL window in one pipelined SET round-trip.
// Called AFTER a durable clean produce so a crash re-produces a collapsible duplicate
// rather than dropping a unique event. Best-effort: an error is benign (ClickHouse dedups).
func (d *Deduper) MarkBatch(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	pipe := d.rdb.Pipeline()
	for _, id := range eventIDs {
		pipe.Set(ctx, dedupKey(id), "", d.ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}
