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

// stopSession advances energy and SoC across the final interval since the last
// METER_UPDATE before pricing, but only up to the planned EndsAt (a duration stop
// fires on the tick AFTER EndsAt, so `now` overshoots) and only while the pack
// isn't already full (chargingPower tapers to 8% of target at 100% SoC, not 0, so
// an unguarded full-battery stop would still over-add). Regression for the F3 fix
// plus the duration-cap / full-battery guards.
func TestStopSessionAdvancesFinalInterval(t *testing.T) {
	start := time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC) // 10:00 -> outside every peak window

	// Shared model constants: 50 kW target, 60 kWh pack, standard-v1 @ 0.39/kWh
	// (PeakMult 1.00, non-peak hour). Soc < 80 keeps chargingPower == 50 (no taper).
	cases := []struct {
		name        string
		soc         float64
		startEnergy float64
		endsAt      time.Time
		stop        time.Time
		wantEnergy  float64
		wantCost    float64
	}{
		{
			// EndsAt in the future -> not capped; 50 kW * 0.1 h = 5 kWh added ->
			// 15 kWh; 15 * 0.39 = 5.85.
			name:        "advances final interval",
			soc:         50,
			startEnergy: 10,
			endsAt:      start.Add(30 * time.Minute),
			stop:        start.Add(6 * time.Minute),
			wantEnergy:  round3(15.0),
			wantCost:    round2(15.0 * 0.39),
		},
		{
			// Full pack: chargingPower would still draw 50*0.08 = 4 kW, but the
			// Soc >= 100 guard skips the add entirely -> energy unchanged at 40 kWh;
			// 40 * 0.39 = 15.60.
			name:        "full battery adds nothing",
			soc:         100,
			startEnergy: 40,
			endsAt:      start.Add(30 * time.Minute),
			stop:        start.Add(6 * time.Minute),
			wantEnergy:  round3(40.0),
			wantCost:    round2(40.0 * 0.39),
		},
		{
			// Duration stops pass their effective timestamp (EndsAt) into stopSession:
			// 50 kW * 0.1 h = 5 kWh -> 15 kWh; 15 * 0.39 = 5.85.
			name:        "duration effective stop at EndsAt",
			soc:         50,
			startEnergy: 10,
			endsAt:      start.Add(6 * time.Minute),
			stop:        start.Add(6 * time.Minute),
			wantEnergy:  round3(15.0),
			wantCost:    round2(15.0 * 0.39),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &Station{ID: "TR-IST-0001", OperatorID: "ChargeSquare", City: "Istanbul", Country: "TR"}
			conn := &Connector{ID: 1, PowerRating: 50, Type: "DC", status: "Charging"}
			conn.session = &Session{
				ID:            "sess-test",
				BatteryKWh:    60,
				TariffID:      "standard-v1",
				StartedAt:     start,
				EndsAt:        tc.endsAt,
				Soc:           tc.soc,
				TargetPowerKW: 50,
				EnergyKWh:     tc.startEnergy,
			}
			conn.lastMeterAt = start

			e := st.stopSession(conn, tc.stop, peakCfg())

			if e.Meter.EnergyKWh != tc.wantEnergy {
				t.Fatalf("stop energy = %v, want %v", e.Meter.EnergyKWh, tc.wantEnergy)
			}
			if e.CostEur != tc.wantCost {
				t.Fatalf("stop cost = %v, want %v", e.CostEur, tc.wantCost)
			}
			wantTS := tc.stop.UTC().Format("2006-01-02T15:04:05.000Z07:00")
			if e.Timestamp != wantTS {
				t.Fatalf("stop timestamp = %q, want %q", e.Timestamp, wantTS)
			}
			if e.IsPeakPriced {
				t.Fatalf("hour-10 stop must not be peak-priced")
			}
		})
	}
}

func TestEffectiveStopAtForDurationStops(t *testing.T) {
	base := time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC)
	endsAt := base.Add(6 * time.Minute)
	overshotTick := base.Add(10 * time.Minute)

	cases := []struct {
		name string
		soc  float64
		now  time.Time
		want time.Time
	}{
		{"duration overshoot uses EndsAt", 50, overshotTick, endsAt},
		{"full battery uses tick time", 100, overshotTick, overshotTick},
		{"before EndsAt uses tick time", 50, base.Add(3 * time.Minute), base.Add(3 * time.Minute)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := effectiveStopAt(&Session{Soc: tc.soc, EndsAt: endsAt}, tc.now)
			if !got.Equal(tc.want) {
				t.Fatalf("effectiveStopAt = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestDurationStopUsesEffectiveTimestampForPricing(t *testing.T) {
	lastMeter := time.Date(2026, 1, 2, 16, 53, 0, 0, time.UTC)
	endsAt := time.Date(2026, 1, 2, 16, 59, 0, 0, time.UTC)
	observedTick := time.Date(2026, 1, 2, 17, 1, 0, 0, time.UTC)

	st := &Station{ID: "TR-IST-0001", OperatorID: "ChargeSquare", City: "Istanbul", Country: "TR"}
	conn := &Connector{ID: 1, PowerRating: 50, Type: "DC", status: "Charging"}
	conn.session = &Session{
		ID:            "sess-boundary",
		BatteryKWh:    60,
		TariffID:      "peak-rate-v2",
		StartedAt:     lastMeter.Add(-20 * time.Minute),
		EndsAt:        endsAt,
		Soc:           50,
		TargetPowerKW: 50,
		EnergyKWh:     10,
	}
	conn.lastMeterAt = lastMeter

	stopAt := effectiveStopAt(conn.session, observedTick)
	e := st.stopSession(conn, stopAt, peakCfg())

	wantTS := endsAt.UTC().Format("2006-01-02T15:04:05.000Z07:00")
	if e.Timestamp != wantTS {
		t.Fatalf("duration stop timestamp = %q, want %q", e.Timestamp, wantTS)
	}
	if e.IsPeakPriced {
		t.Fatalf("duration stop at 16:59 must not be peak-priced even if observed at 17:01")
	}
	if e.CostEur != round2(15.0*0.49) {
		t.Fatalf("duration stop cost = %v, want base-rate cost %v", e.CostEur, round2(15.0*0.49))
	}
}

// peakCfg is a minimal config carrying the shipped peak windows (07-09, 17-20).
// stopSession and pickTariff read only cfg.Simulator.PeakWindows for peak logic.
func peakCfg() *Config {
	return &Config{Simulator: SimulatorConfig{PeakWindows: []PeakWindow{
		{Start: 7, End: 9, Weight: 3.0},
		{Start: 17, End: 20, Weight: 3.5},
	}}}
}

// is_peak_priced must be ECONOMIC, not clock-based: it is A4's peak-revenue
// numerator, so it is set only when a real premium (PeakMult > 1) was billed, and
// the window is config-sourced so BOTH peak windows (07-09, 17-20) price. Setting
// lastMeterAt to the stop time zeroes the final-interval add, isolating pricing.
func TestStopSessionPeakPricingIsEconomic(t *testing.T) {
	const energy = 20.0 // no tail added, so billed energy == this

	cases := []struct {
		name     string
		tariffID string
		hour     int
		wantPeak bool
		wantCost float64
	}{
		{"peak tariff, evening window", "peak-rate-v2", 18, true, round2(energy * 0.49 * 1.35)},
		{"peak tariff, morning window", "peak-rate-v2", 8, true, round2(energy * 0.49 * 1.35)},
		{"peak tariff, outside window", "peak-rate-v2", 10, false, round2(energy * 0.49)},
		{"standard billed at base in window", "standard-v1", 18, false, round2(energy * 0.39)},
		{"fleet billed at base in window", "fleet-v1", 18, false, round2(energy * 0.34)},
		{"off-peak discounted in window", "off-peak-v1", 18, false, round2(energy * 0.29 * 0.80)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stop := time.Date(2026, 1, 2, tc.hour, 0, 0, 0, time.UTC)
			st := &Station{ID: "TR-IST-0001", OperatorID: "ChargeSquare", City: "Istanbul", Country: "TR"}
			conn := &Connector{ID: 1, PowerRating: 50, Type: "DC", status: "Charging"}
			conn.session = &Session{
				ID:            "sess-test",
				BatteryKWh:    60,
				TariffID:      tc.tariffID,
				StartedAt:     stop.Add(-20 * time.Minute),
				Soc:           50,
				TargetPowerKW: 50,
				EnergyKWh:     energy,
			}
			conn.lastMeterAt = stop // no final-interval energy added

			e := st.stopSession(conn, stop, peakCfg())

			if e.IsPeakPriced != tc.wantPeak {
				t.Fatalf("%s: IsPeakPriced = %v, want %v", tc.tariffID, e.IsPeakPriced, tc.wantPeak)
			}
			if e.CostEur != tc.wantCost {
				t.Fatalf("%s: cost = %v, want %v", tc.tariffID, e.CostEur, tc.wantCost)
			}
		})
	}
}

// Every id pickTariff can return must exist in tariffByID, else stopSession would
// price against a zero-value Tariff (free energy). Guards the F6 single catalog.
func TestPickTariffIDsInCatalog(t *testing.T) {
	g := newRNG(1, peakCfg())
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
