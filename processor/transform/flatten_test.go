package transform

import (
	"encoding/json"
	"testing"
	"time"
)

// The clean row's JSON keys are a contract with ClickHouse's ev.events_queue columns
// (JSONEachRow matches by name). This locks every key so a rename can't silently break
// ingestion.
func TestCleanEventKeysMatchSchema(t *testing.T) {
	want := []string{
		"event_id", "event_type", "station_id", "connector_id", "session_id",
		"timestamp", "ingested_at", "produced_at", "operator_id", "lat", "lon", "city", "country",
		"power_kw", "energy_kwh", "voltage_v", "current_a", "soc_percent",
		"vehicle_brand", "vehicle_model", "ev_id", "tariff_id", "cost_eur",
		"error_code", "component", "status", "is_peak_priced",
	}
	b, err := json.Marshal(CleanEvent{})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("clean event has %d keys, want %d", len(got), len(want))
	}
	for _, k := range want {
		if _, ok := got[k]; !ok {
			t.Errorf("missing clean key %q", k)
		}
	}
}

func TestFlattenMeterUpdate(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	e := Event{
		EventID: "e1", EventType: "METER_UPDATE", StationID: "TR-IST-0001",
		ConnectorID: 2, SessionID: "s1", Timestamp: "2026-07-01T09:59:00.000Z",
		OperatorID: "ZES",
		Location:   Location{Lat: 41, Lon: 29, City: "Istanbul", Country: "TR"},
		Meter:      &Meter{PowerKW: 50, EnergyKWh: 12.5, VoltageV: 400, CurrentA: 125, SocPercent: 60},
		TariffID:   "standard-v1",
	}
	ce := Flatten(e, ts)
	if ce.PowerKW != 50 || ce.EnergyKWh != 12.5 || ce.SocPercent != 60 {
		t.Errorf("meter not lifted: %+v", ce)
	}
	if ce.Lat != 41 || ce.City != "Istanbul" {
		t.Errorf("location not lifted: %+v", ce)
	}
	if ce.IngestedAt != "2026-07-01T10:00:00.000Z" {
		t.Errorf("ingested_at = %q, want 2026-07-01T10:00:00.000Z", ce.IngestedAt)
	}
	// Absent sub-objects must stay zero.
	if ce.VehicleBrand != "" || ce.ErrorCode != "" || ce.Status != "" {
		t.Errorf("absent sub-objects should be zero: %+v", ce)
	}
}

func TestFlattenFaultNoMeter(t *testing.T) {
	e := Event{
		EventID: "e2", EventType: "FAULT_ALERT", StationID: "TR-ANK-0007", ConnectorID: 1,
		Timestamp: "2026-07-01T09:59:00.000Z", OperatorID: "VoltRun",
		Fault: &Fault{ErrorCode: "OverVoltage", Component: "PowerModule"},
	}
	ce := Flatten(e, time.Now())
	if ce.ErrorCode != "OverVoltage" || ce.Component != "PowerModule" {
		t.Errorf("fault not lifted: %+v", ce)
	}
	if ce.PowerKW != 0 {
		t.Errorf("no meter -> power should be 0, got %v", ce.PowerKW)
	}
}
