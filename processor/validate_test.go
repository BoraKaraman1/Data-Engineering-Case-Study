package main

import (
	"testing"

	"chargesquare/processor/transform"
)

func testRegistry() *Registry {
	return &Registry{
		stations: map[string]stationMeta{
			"TR-IST-0001": {
				numConnectors: 2,
				operatorID:    "ChargeSquare",
				city:          "Istanbul",
				country:       "TR",
				lat:           41,
				lon:           29,
			},
		},
		tariffs: map[string]struct{}{"standard-v1": {}},
	}
}

func validMeterUpdate() transform.Event {
	return transform.Event{
		EventID: "e1", EventType: "METER_UPDATE", StationID: "TR-IST-0001",
		ConnectorID: 1, SessionID: "s1", Timestamp: "2026-07-01T09:59:00.000Z",
		OperatorID: "ChargeSquare",
		Location:   transform.Location{Lat: 41, Lon: 29, City: "Istanbul", Country: "TR"},
		Meter:      &transform.Meter{PowerKW: 50, EnergyKWh: 1, VoltageV: 400, CurrentA: 125, SocPercent: 60},
		TariffID:   "standard-v1",
	}
}

func TestValidateAcceptsGoodEvent(t *testing.T) {
	if verr := Validate(validMeterUpdate(), testRegistry()); verr != nil {
		t.Fatalf("expected valid, got %v", verr)
	}
}

func TestValidateRules(t *testing.T) {
	reg := testRegistry()
	cases := []struct {
		name string
		rule string
		mut  func(e *transform.Event)
	}{
		{"missing event_id", "missing_event_id", func(e *transform.Event) { e.EventID = "" }},
		{"unknown type", "unknown_event_type", func(e *transform.Event) { e.EventType = "NOPE" }},
		{"unknown station", "unknown_station", func(e *transform.Event) { e.StationID = "TR-XXX-9999" }},
		{"bad timestamp", "bad_timestamp", func(e *transform.Event) { e.Timestamp = "not-a-time" }},
		{"connector out of range", "connector_out_of_range", func(e *transform.Event) { e.ConnectorID = 9 }},
		{"unknown tariff", "unknown_tariff", func(e *transform.Event) { e.TariffID = "ghost" }},
		{"soc out of range", "soc_out_of_range", func(e *transform.Event) { e.Meter.SocPercent = 150 }},
		{"missing meter", "missing_meter", func(e *transform.Event) { e.Meter = nil }},
		{"missing tariff", "missing_tariff", func(e *transform.Event) { e.TariffID = "" }},
		{"missing location", "missing_location", func(e *transform.Event) { e.Location.City = "" }},
		{"bad status", "bad_status", func(e *transform.Event) { e.EventType = "STATUS_CHANGE"; e.Status = "Weird" }},
		{"start without vehicle", "missing_vehicle", func(e *transform.Event) { e.EventType = "SESSION_START"; e.Vehicle = nil }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := validMeterUpdate()
			c.mut(&e)
			verr := Validate(e, reg)
			if verr == nil {
				t.Fatalf("expected rule %q, got nil", c.rule)
			}
			if verr.Rule != c.rule {
				t.Fatalf("expected rule %q, got %q (%s)", c.rule, verr.Rule, verr.Msg)
			}
		})
	}
}

func TestHeartbeatConnectorMustBeZero(t *testing.T) {
	e := transform.Event{
		EventID: "h1", EventType: "HEARTBEAT", StationID: "TR-IST-0001", ConnectorID: 1,
		Timestamp: "2026-07-01T09:59:00.000Z", OperatorID: "ChargeSquare",
		Location: transform.Location{Lat: 41, Lon: 29, City: "Istanbul", Country: "TR"},
	}
	verr := Validate(e, testRegistry())
	if verr == nil || verr.Rule != "bad_connector" {
		t.Fatalf("expected bad_connector, got %v", verr)
	}
}

// TestValidateReferential pins the F4 registry cross-check: an event whose operator, city,
// country, and coordinates match the seeded station passes, and each individual mismatch
// returns its own rule. The match case is also the simulator's contract -- it emits exactly
// the seeded operator/city/country and station coords, so the strict string equality and
// the coordinate epsilon must never false-positive on a genuine event.
func TestValidateReferential(t *testing.T) {
	reg := testRegistry()

	// Exact match on every referential field (what the simulator emits) -> valid.
	if verr := Validate(validMeterUpdate(), reg); verr != nil {
		t.Fatalf("expected referential match to pass, got %v", verr)
	}

	// Coordinates within the epsilon (float round-trip through Postgres+JSON) still match.
	near := validMeterUpdate()
	near.Location.Lat = 41 + 5e-5
	near.Location.Lon = 29 - 5e-5
	if verr := Validate(near, reg); verr != nil {
		t.Fatalf("expected within-epsilon geo to pass, got %v", verr)
	}

	cases := []struct {
		name string
		rule string
		mut  func(e *transform.Event)
	}{
		{"operator mismatch", "operator_mismatch", func(e *transform.Event) { e.OperatorID = "OtherCo" }},
		{"city mismatch", "city_mismatch", func(e *transform.Event) { e.Location.City = "Ankara" }},
		{"country mismatch", "country_mismatch", func(e *transform.Event) { e.Location.Country = "DE" }},
		{"gross geo mismatch", "geo_mismatch", func(e *transform.Event) { e.Location.Lat = 40; e.Location.Lon = 30 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := validMeterUpdate()
			c.mut(&e)
			verr := Validate(e, reg)
			if verr == nil {
				t.Fatalf("expected rule %q, got nil", c.rule)
			}
			if verr.Rule != c.rule {
				t.Fatalf("expected rule %q, got %q (%s)", c.rule, verr.Rule, verr.Msg)
			}
		})
	}
}
