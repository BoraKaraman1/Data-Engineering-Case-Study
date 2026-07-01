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

// Apply writes the connector's current state from this event. Returns false when the
// event is stale (older than what's stored) and was therefore skipped.
func (s *StateStore) Apply(ctx context.Context, e transform.Event, ts time.Time) (bool, error) {
	key := stateKey(e.StationID, e.ConnectorID)

	// Only the fields this event actually knows about, so a STATUS_CHANGE never clobbers
	// the last known power/soc with zeros. ARGV[1]=event ms, ARGV[2]=ttl ms, ARGV[3..]
	// are the field/value pairs.
	args := []any{ts.UnixMilli(), s.ttl.Milliseconds()}
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

	res, err := casScript.Run(ctx, s.rdb, []string{key}, args...).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}
