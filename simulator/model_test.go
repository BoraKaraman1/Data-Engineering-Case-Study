package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

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

// stopSession must advance energy and SoC across the final interval since the last
// METER_UPDATE before pricing; otherwise the SESSION_STOP total and its billed cost
// undercount the tail of the session. Regression test for the F3 fix.
func TestStopSessionAdvancesFinalInterval(t *testing.T) {
	st := &Station{ID: "TR-IST-0001", OperatorID: "ChargeSquare", City: "Istanbul", Country: "TR"}
	conn := &Connector{ID: 1, PowerRating: 50, Type: "DC", status: "Charging"}
	start := time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC) // 10:00 -> outside the 17-21 peak window
	conn.session = &Session{
		ID:            "sess-test",
		BatteryKWh:    60,
		TariffID:      "standard-v1",
		StartedAt:     start,
		Soc:           50, // < 80 so chargingPower == TargetPowerKW (no taper crossing)
		TargetPowerKW: 50,
		EnergyKWh:     10, // 10 kWh already metered before the final interval
	}
	conn.lastMeterAt = start
	stop := start.Add(6 * time.Minute) // 0.1 h since the last meter tick

	e := st.stopSession(conn, stop)

	// Final interval: 50 kW * 0.1 h = 5 kWh added -> 15 kWh total.
	if want := round3(15.0); e.Meter.EnergyKWh != want {
		t.Fatalf("stop energy = %v, want %v (final interval not advanced)", e.Meter.EnergyKWh, want)
	}
	// standard-v1 base 0.39 EUR/kWh, off-peak hour -> 15 * 0.39 = 5.85.
	if want := round2(15.0 * 0.39); e.CostEur != want {
		t.Fatalf("stop cost = %v, want %v", e.CostEur, want)
	}
	if e.IsPeakPriced {
		t.Fatalf("10:00 stop must not be peak-priced")
	}
}

// Every id pickTariff can return must exist in tariffByID, else stopSession would
// price against a zero-value Tariff (free energy). Guards the F6 single catalog.
func TestPickTariffIDsInCatalog(t *testing.T) {
	g := newRNG(1, &Config{})
	for _, hour := range []int{9, 18} { // off-peak hour and the pickTariff peak branch
		for i := 0; i < 1000; i++ {
			id := g.pickTariff(hour)
			if _, ok := tariffByID[id]; !ok {
				t.Fatalf("pickTariff(%d) returned %q, absent from tariffByID", hour, id)
			}
		}
	}
}

// baseValidConfig returns a minimal config that passes LoadConfig validation. Tests
// marshal it to a temp YAML file (LoadConfig reads a path) then mutate one field to
// assert each validation rule rejects bad input.
func baseValidConfig() Config {
	return Config{
		Simulator: SimulatorConfig{
			StationCount:         10,
			ConnectorsPerStation: [2]int{1, 4},
			Seed:                 42,
			TargetEventsPerSec:   1000,
			TimeAcceleration:     60,
			DuplicateRate:        0.02,
			OutOfOrderRate:       0.01,
			FaultRatePerHour:     0.05,
			MeterIntervalSec:     [2]int{5, 30},
			SessionMinutes:       SessionDist{Mean: 35, Stddev: 18, Min: 5, Max: 180},
			PeakWindows: []PeakWindow{
				{Start: 7, End: 9, Weight: 3.0},
				{Start: 17, End: 20, Weight: 3.5},
			},
			BaseArrivalWeight: 1.0,
		},
		Cities: []City{
			{Code: "IST", Name: "Istanbul", Lat: 41.0, Lon: 28.9, Weight: 30},
			{Code: "ANK", Name: "Ankara", Lat: 39.9, Lon: 32.8, Weight: 12},
		},
	}
}

func writeConfig(t *testing.T, c Config) string {
	t.Helper()
	b, err := yaml.Marshal(c)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

func TestLoadConfigValidation(t *testing.T) {
	if _, err := LoadConfig(writeConfig(t, baseValidConfig())); err != nil {
		t.Fatalf("baseline valid config rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"zero city weight", func(c *Config) { c.Cities[1].Weight = 0 }},
		{"connectors min < 1", func(c *Config) { c.Simulator.ConnectorsPerStation = [2]int{0, 4} }},
		{"connectors max < min", func(c *Config) { c.Simulator.ConnectorsPerStation = [2]int{4, 2} }},
		{"meter interval min < 1", func(c *Config) { c.Simulator.MeterIntervalSec = [2]int{0, 30} }},
		{"session mean above max", func(c *Config) { c.Simulator.SessionMinutes.Mean = 999 }},
		{"session min <= 0", func(c *Config) { c.Simulator.SessionMinutes.Min = 0 }},
		{"peak end <= start", func(c *Config) { c.Simulator.PeakWindows[0].End = 7 }},
		{"peak weight zero", func(c *Config) { c.Simulator.PeakWindows[0].Weight = 0 }},
		{"duplicate rate > 1", func(c *Config) { c.Simulator.DuplicateRate = 1.5 }},
		{"out-of-order rate < 0", func(c *Config) { c.Simulator.OutOfOrderRate = -0.1 }},
		{"negative fault rate", func(c *Config) { c.Simulator.FaultRatePerHour = -1 }},
	}
	for _, tc := range cases {
		c := baseValidConfig()
		tc.mutate(&c)
		if _, err := LoadConfig(writeConfig(t, c)); err == nil {
			t.Errorf("%s: expected LoadConfig to reject, got nil error", tc.name)
		}
	}
}

// The real configs shipped in config/ must all pass the validation (guards against
// over-tightening the bounds so a legitimate config is rejected).
func TestRealConfigsLoad(t *testing.T) {
	paths, err := filepath.Glob("../config/scale-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, "../config/simulator.yaml")
	if len(paths) < 5 {
		t.Fatalf("expected simulator.yaml + 4 scale configs, found %v", paths)
	}
	for _, p := range paths {
		if _, err := LoadConfig(p); err != nil {
			t.Errorf("%s failed validation: %v", p, err)
		}
	}
}
