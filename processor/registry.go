package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// Registry is an in-memory snapshot of the OLTP station/tariff registry, loaded once at
// startup. Immutable after load, so every worker reads it lock-free (no hot-path DB).
type Registry struct {
	stations map[string]int // station_id -> connector count
	tariffs  map[string]struct{}
}

func (r *Registry) Station(id string) (int, bool) {
	n, ok := r.stations[id]
	return n, ok
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

	reg := &Registry{stations: map[string]int{}, tariffs: map[string]struct{}{}}

	rows, err := db.Query(`SELECT station_id, num_connectors FROM stations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		reg.stations[id] = n
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
