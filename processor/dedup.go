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

// Seen reports whether this event_id was already marked within the TTL window.
func (d *Deduper) Seen(ctx context.Context, eventID string) (bool, error) {
	n, err := d.rdb.Exists(ctx, dedupKey(eventID)).Result()
	return n > 0, err
}

// Mark claims an event_id for the TTL window. Called AFTER a successful clean produce.
func (d *Deduper) Mark(ctx context.Context, eventID string) error {
	return d.rdb.Set(ctx, dedupKey(eventID), "", d.ttl).Err()
}
