package main

import "testing"

// Station IDs must be globally unique: they are the PRIMARY KEY in the Postgres
// registry and the leading ORDER BY / partition key for per-station analytics in
// ClickHouse. A collision silently conflates two physical stations.
//
// Regression test for pad4() truncating the per-city counter at 4 digits, so
// TR-IST-10001 collided with TR-IST-0001. It only surfaced at load-test scale,
// where the busiest city's station count exceeds 9999.
func TestStationIDsUnique(t *testing.T) {
	cfg := &Config{
		Simulator: SimulatorConfig{
			StationCount:         50000, // busiest city (IST) lands well past 9999 here
			ConnectorsPerStation: [2]int{1, 4},
			Seed:                 42,
		},
		Cities: []City{
			{Code: "IST", Name: "Istanbul", Lat: 41.0082, Lon: 28.9784, Weight: 30},
			{Code: "ANK", Name: "Ankara", Lat: 39.9334, Lon: 32.8597, Weight: 12},
			{Code: "IZM", Name: "Izmir", Lat: 38.4237, Lon: 27.1428, Weight: 10},
			{Code: "BUR", Name: "Bursa", Lat: 40.1885, Lon: 29.0610, Weight: 6},
			{Code: "ANT", Name: "Antalya", Lat: 36.8969, Lon: 30.7133, Weight: 6},
			{Code: "ADA", Name: "Adana", Lat: 37.0000, Lon: 35.3213, Weight: 4},
			{Code: "KON", Name: "Konya", Lat: 37.8746, Lon: 32.4932, Weight: 3},
		},
	}

	stations := BuildStations(cfg, newRNG(cfg.Simulator.Seed, cfg))

	if len(stations) != cfg.Simulator.StationCount {
		t.Fatalf("built %d stations, want %d", len(stations), cfg.Simulator.StationCount)
	}

	seen := make(map[string]int, len(stations))
	for i, s := range stations {
		if first, dup := seen[s.ID]; dup {
			t.Fatalf("duplicate station ID %q at index %d (first seen at index %d)",
				s.ID, i, first)
		}
		seen[s.ID] = i
	}
}

// Operators must be assigned from the configured set and spread across stations,
// so A2/A4 "per operator" analytics are meaningful and not a single-operator
// degenerate. Also guards that operator assignment never perturbs the roster.
func TestOperatorsAssigned(t *testing.T) {
	ops := []string{"ChargeSquare", "VoltRun", "ZES", "Esarj", "Trugo"}
	cfg := &Config{
		Simulator: SimulatorConfig{
			StationCount:         2000,
			ConnectorsPerStation: [2]int{1, 4},
			Seed:                 42,
			Operators:            ops,
		},
		Cities: []City{
			{Code: "IST", Name: "Istanbul", Lat: 41.0082, Lon: 28.9784, Weight: 30},
			{Code: "ANK", Name: "Ankara", Lat: 39.9334, Lon: 32.8597, Weight: 12},
		},
	}

	allowed := make(map[string]bool, len(ops))
	for _, o := range ops {
		allowed[o] = true
	}

	stations := BuildStations(cfg, newRNG(cfg.Simulator.Seed, cfg))
	seen := make(map[string]int)
	for _, s := range stations {
		if !allowed[s.OperatorID] {
			t.Fatalf("station %s has operator %q not in configured set", s.ID, s.OperatorID)
		}
		seen[s.OperatorID]++
	}
	if len(seen) < 2 {
		t.Fatalf("expected multiple operators represented, got %d: %v", len(seen), seen)
	}
}
