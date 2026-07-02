package main

import (
	"reflect"
	"testing"
	"time"

	"chargesquare/processor/transform"
)

// TestStateArgs pins the field-selective CAS argument building shared by Apply and
// ApplyBatch: ARGV[1]=event ms, ARGV[2]=ttl ms, then status ONLY from STATUS_CHANGE, meter
// fields ONLY when a meter is present, a session on SESSION_START/METER_UPDATE, and
// SESSION_STOP clearing session and power. A divergence here corrupts the current-state
// hash (or lets a STATUS_CHANGE clobber power/soc with zeros), so it is worth locking down.
func TestStateArgs(t *testing.T) {
	ts := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	ttl := 300 * time.Second
	tsMs := ts.UnixMilli()
	ttlMs := ttl.Milliseconds()
	meter := &transform.Meter{PowerKW: 50, SocPercent: 60}

	cases := []struct {
		name string
		e    transform.Event
		want []any
	}{
		{
			name: "STATUS_CHANGE sets status only, no meter/session",
			e:    transform.Event{EventType: "STATUS_CHANGE", Status: "Charging"},
			want: []any{tsMs, ttlMs, "status", "Charging"},
		},
		{
			name: "METER_UPDATE sets meter+session, never status",
			e:    transform.Event{EventType: "METER_UPDATE", SessionID: "s1", Meter: meter},
			want: []any{tsMs, ttlMs, "power", 50.0, "soc", 60, "session", "s1"},
		},
		{
			name: "SESSION_START sets meter+session",
			e:    transform.Event{EventType: "SESSION_START", SessionID: "s1", Meter: meter},
			want: []any{tsMs, ttlMs, "power", 50.0, "soc", 60, "session", "s1"},
		},
		{
			// power is written twice (meter value then 0); the HSET applies pairs left to
			// right so 0 wins, clearing power while soc keeps the final meter reading.
			name: "SESSION_STOP clears session and power after the meter fields",
			e:    transform.Event{EventType: "SESSION_STOP", SessionID: "s1", Meter: meter},
			want: []any{tsMs, ttlMs, "power", 50.0, "soc", 60, "session", "", "power", 0},
		},
		{
			name: "HEARTBEAT carries neither status, meter, nor session",
			e:    transform.Event{EventType: "HEARTBEAT"},
			want: []any{tsMs, ttlMs},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stateArgs(tc.e, ts, ttl)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("stateArgs = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// TestStateKeyForEvent pins the F10 key routing: a HEARTBEAT is station-level liveness and
// must land in its own station_liveness:{id} namespace, not the connector-shaped
// station:{id}:0 that the connector-state readers scan; every other event keeps its
// per-connector station:{id}:{n} key.
func TestStateKeyForEvent(t *testing.T) {
	hb := transform.Event{EventType: "HEARTBEAT", StationID: "TR-IST-0001", ConnectorID: 0}
	if got, want := stateKeyForEvent(hb), "station_liveness:TR-IST-0001"; got != want {
		t.Fatalf("HEARTBEAT key = %q, want %q", got, want)
	}
	conn := transform.Event{EventType: "METER_UPDATE", StationID: "TR-IST-0001", ConnectorID: 2}
	if got, want := stateKeyForEvent(conn), "station:TR-IST-0001:2"; got != want {
		t.Fatalf("connector key = %q, want %q", got, want)
	}
}
