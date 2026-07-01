package main

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"chargesquare/processor/transform"
)

// casScript is an atomic compare-and-set: it refuses to overwrite current state with an
// OLDER event (out-of-order arrival), refreshes the TTL, and writes only the fields the
// event carries. Station-keyed partitioning already serialises a connector's events, so
// this is belt-and-braces plus a single-round-trip write.
var casScript = redis.NewScript(`
local stored = redis.call('HGET', KEYS[1], 'last_seen')
if stored and tonumber(ARGV[1]) < tonumber(stored) then
  return 0
end
redis.call('HSET', KEYS[1], 'last_seen', ARGV[1], unpack(ARGV, 3))
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`)

// StateStore maintains the real-time current state: one hash per connector, answering
// "status of this connector in the last N minutes" as a sub-millisecond point lookup.
// The TTL refresh on every write means key existence == "seen within the window", so
// readers need no time math and memory self-bounds as connectors go quiet.
type StateStore struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewStateStore(rdb *redis.Client, ttl time.Duration) *StateStore {
	return &StateStore{rdb: rdb, ttl: ttl}
}

func stateKey(stationID string, connectorID int) string {
	return "station:" + stationID + ":" + strconv.Itoa(connectorID)
}

// deriveStatus returns the connector status ONLY from STATUS_CHANGE events, which are
// the authoritative status timeline (the simulator emits one on every transition). The
// other event types update power/soc/session but must not touch status: during a
// fault-abort the FAULT_ALERT, SESSION_STOP and STATUS_CHANGE=Faulted share a timestamp,
// and since the CAS only rejects strictly-older events, letting SESSION_STOP imply
// "Available" could race the connector back out of Faulted. Status-from-STATUS_CHANGE
// also matches how A2 reconstructs uptime.
func deriveStatus(e transform.Event) string {
	if e.EventType == "STATUS_CHANGE" {
		return e.Status
	}
	return ""
}

// stateEvent is one decoded, validated event awaiting a batched current-state write, paired
// with its parsed event time (the CAS last_seen comparand). ApplyBatch preserves input
// (offset) order so the newest event per connector wins the CAS.
type stateEvent struct {
	e  transform.Event
	ts time.Time
}

// stateArgs builds the casScript arguments for one event: ARGV[1]=event ms, ARGV[2]=ttl ms,
// then ONLY the fields this event actually knows about, so a STATUS_CHANGE never clobbers the
// last known power/soc with zeros (status only from STATUS_CHANGE, meter only when present,
// SESSION_STOP clears session and power). Shared by Apply and ApplyBatch so the single-event
// and batched paths build byte-for-byte identical CAS writes.
func stateArgs(e transform.Event, ts time.Time, ttl time.Duration) []any {
	args := []any{ts.UnixMilli(), ttl.Milliseconds()}
	if st := deriveStatus(e); st != "" {
		args = append(args, "status", st)
	}
	if e.Meter != nil {
		args = append(args, "power", e.Meter.PowerKW, "soc", e.Meter.SocPercent)
	}
	switch e.EventType {
	case "SESSION_START", "METER_UPDATE":
		if e.SessionID != "" {
			args = append(args, "session", e.SessionID)
		}
	case "SESSION_STOP":
		args = append(args, "session", "", "power", 0)
	}
	return args
}

// Apply writes the connector's current state from this event. Returns false when the
// event is stale (older than what's stored) and was therefore skipped.
func (s *StateStore) Apply(ctx context.Context, e transform.Event, ts time.Time) (bool, error) {
	key := stateKey(e.StationID, e.ConnectorID)
	res, err := casScript.Run(ctx, s.rdb, []string{key}, stateArgs(e, ts, s.ttl)...).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// ApplyBatch writes the current state for a whole batch of events in ONE pipelined round
// trip. It runs the SAME casScript per event -- so the older-than-last_seen rejection, the
// field-selective HSET, and the TTL refresh are byte-for-byte the per-event Apply semantics
// -- but pipelines them so the batch pays a single Redis RTT instead of one per event, which
// is what lifts the realtime path off its per-event-round-trip ceiling (H2). Events MUST be
// in offset (arrival) order: same-station events share one partition and one worker, so a
// connector's events stay ordered here and the CAS keeps its newest state. Returns, per event
// and in input order, whether the write applied (true) or was skipped as stale (false).
//
// Eval (not EvalSha/Run) is used inside the pipeline so a cold script cache can never surface
// a NOSCRIPT mid-pipeline: the whole batch is still ONE round trip, and the bottleneck was the
// RTT, not the ~200-byte script text carried in it.
func (s *StateStore) ApplyBatch(ctx context.Context, events []stateEvent) ([]bool, error) {
	if len(events) == 0 {
		return nil, nil
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.Cmd, len(events))
	for i := range events {
		key := stateKey(events[i].e.StationID, events[i].e.ConnectorID)
		cmds[i] = casScript.Eval(ctx, pipe, []string{key}, stateArgs(events[i].e, events[i].ts, s.ttl)...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	applied := make([]bool, len(events))
	for i, c := range cmds {
		n, err := c.Int()
		if err != nil {
			return nil, err
		}
		applied[i] = n == 1
	}
	return applied, nil
}
