package main

import (
	"testing"
	"time"

	"chargesquare/processor/transform"
)

var benchPayload = []byte(`{"event_id":"b1","event_type":"METER_UPDATE","station_id":"TR-IST-0001","connector_id":1,"session_id":"s1","timestamp":"2026-07-01T09:59:00.000Z","operator_id":"ChargeSquare","location":{"lat":41.0,"lon":29.0,"city":"Istanbul","country":"TR"},"meter":{"power_kw":50.0,"energy_kwh":12.5,"voltage_v":400,"current_a":125,"soc_percent":60},"tariff_id":"standard-v1"}`)

// BenchmarkFlattenValidate covers the processor hot path: JSON decode -> validate ->
// flatten. Phase-4 uses this to attribute the per-event CPU cost when profiling throughput.
func BenchmarkFlattenValidate(b *testing.B) {
	reg := &Registry{
		stations: map[string]int{"TR-IST-0001": 2},
		tariffs:  map[string]struct{}{"standard-v1": {}},
	}
	now := time.Now()
	b.ReportAllocs()
	b.SetBytes(int64(len(benchPayload)))
	for i := 0; i < b.N; i++ {
		e, derr := Decode(benchPayload)
		if derr != nil {
			b.Fatal(derr)
		}
		if verr := Validate(e, reg); verr != nil {
			b.Fatal(verr)
		}
		_ = transform.Flatten(e, now)
	}
}
