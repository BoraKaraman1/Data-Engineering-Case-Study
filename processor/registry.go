package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// stationMeta is the registry row for one station: the connector count plus the
// operator/geo identity an event must match. Loading it lets validate.go reject a
// well-formed event that names a real station but the wrong operator/city/country/coords
// (which would otherwise pollute analytics grouped by operator/city).
type stationMeta struct {
	numConnectors int
	operatorID    string
	city          string
	country       string
	lat           float64
	lon           float64
}

// Registry is an in-memory snapshot of the OLTP station/tariff registry, loaded once at
// startup. Immutable after load, so every worker reads it lock-free (no hot-path DB).
type Registry struct {
	stations map[string]stationMeta // station_id -> metadata
	tariffs  map[string]struct{}
}

// Station returns the station's connector count, preserving the pre-metadata signature so
// existing callers (validate.go's connector-range check) are unaffected.
func (r *Registry) Station(id string) (int, bool) {
	m, ok := r.stations[id]
	return m.numConnectors, ok
}

// StationMeta returns the full registry row for referential cross-checks.
func (r *Registry) StationMeta(id string) (stationMeta, bool) {
	m, ok := r.stations[id]
	return m, ok
}

func (r *Registry) TariffKnown(id string) bool {
	_, ok := r.tariffs[id]
	return ok
}

// LoadRegistryWithRetry polls Postgres until the registry is populated. The simulator
// seeds it concurrently at startup, so an empty result means "not seeded yet", not "no
// stations" -- without this wait, every event would dead-letter as an unknown station.
func LoadRegistryWithRetry(dsn string, timeout time.Duration) (*Registry, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		reg, err := loadRegistry(dsn)
		switch {
		case err != nil:
			lastErr = err
		case len(reg.stations) == 0:
			lastErr = fmt.Errorf("registry empty (simulator not seeded yet)")
		default:
			return reg, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("registry not ready after %s: %w", timeout, lastErr)
		}
		time.Sleep(2 * time.Second)
	}
}

func loadRegistry(dsn string) (*Registry, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return nil, err
	}

	reg := &Registry{stations: map[string]stationMeta{}, tariffs: map[string]struct{}{}}

	rows, err := db.Query(`SELECT station_id, num_connectors, operator_id, city, country, lat, lon FROM stations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var m stationMeta
		if err := rows.Scan(&id, &m.numConnectors, &m.operatorID, &m.city, &m.country, &m.lat, &m.lon); err != nil {
			return nil, err
		}
		reg.stations[id] = m
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	trows, err := db.Query(`SELECT tariff_id FROM tariffs`)
	if err != nil {
		return nil, err
	}
	defer trows.Close()
	for trows.Next() {
		var id string
		if err := trows.Scan(&id); err != nil {
			return nil, err
		}
		reg.tariffs[id] = struct{}{}
	}
	return reg, trows.Err()
}
