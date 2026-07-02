package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync/atomic"
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

// registrySnapshot is an immutable point-in-time view of the station/tariff roster. A
// refresh builds a fresh snapshot and swaps it in wholesale, so a reader sees either the
// old maps or the new ones -- never a half-updated map.
type registrySnapshot struct {
	stations map[string]stationMeta // station_id -> metadata
	tariffs  map[string]struct{}
}

// Registry is an in-memory view of the OLTP station/tariff registry. The active snapshot
// lives in an atomic pointer and is swapped wholesale (see Refresh), so every worker reads
// it lock-free (no hot-path DB) while a periodic refresh picks up new stations and tariff
// changes without a restart.
type Registry struct {
	snap atomic.Pointer[registrySnapshot]
}

// Station returns the station's connector count, preserving the pre-metadata signature so
// existing callers (validate.go's connector-range check) are unaffected.
func (r *Registry) Station(id string) (int, bool) {
	m, ok := r.snap.Load().stations[id]
	return m.numConnectors, ok
}

// StationMeta returns the full registry row for referential cross-checks.
func (r *Registry) StationMeta(id string) (stationMeta, bool) {
	m, ok := r.snap.Load().stations[id]
	return m, ok
}

func (r *Registry) TariffKnown(id string) bool {
	_, ok := r.snap.Load().tariffs[id]
	return ok
}

// Len reports the station count in the active snapshot (for main.go's startup log line).
func (r *Registry) Len() int {
	return len(r.snap.Load().stations)
}

// LoadRegistryWithRetry polls Postgres until the registry is populated, then returns a
// Registry holding that snapshot. The simulator seeds it concurrently at startup, so an
// empty result means "not seeded yet", not "no stations" -- without this wait, every event
// would dead-letter as an unknown station.
func LoadRegistryWithRetry(dsn string, timeout time.Duration) (*Registry, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		snap, err := loadRegistry(dsn)
		switch {
		case err != nil:
			lastErr = err
		case len(snap.stations) == 0:
			lastErr = fmt.Errorf("registry empty (simulator not seeded yet)")
		default:
			r := &Registry{}
			r.snap.Store(snap)
			return r, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("registry not ready after %s: %w", timeout, lastErr)
		}
		time.Sleep(2 * time.Second)
	}
}

// Refresh re-queries the roster and, only on a successful and non-empty load, atomically
// swaps in the new snapshot. On a query error or an empty result it keeps the current
// snapshot and returns the error: a transient DB blip or a mid-reseed truncation must not
// blank the roster and start dead-lettering every event.
func (r *Registry) Refresh(dsn string) error {
	snap, err := loadRegistry(dsn)
	if err != nil {
		return err
	}
	if len(snap.stations) == 0 {
		return fmt.Errorf("registry refresh returned no stations; keeping previous snapshot")
	}
	r.snap.Store(snap)
	return nil
}

// StartRegistryRefresher periodically refreshes the registry so new stations and tariff
// changes are picked up without a restart. It blocks until ctx is cancelled, so callers
// run it in a goroutine. A failed refresh is logged and retried on the next tick
// (non-fatal -- the previous snapshot keeps serving reads).
func StartRegistryRefresher(ctx context.Context, r *Registry, dsn string, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.Refresh(dsn); err != nil {
				log.Printf("registry refresh: %v", err)
				continue
			}
			s := r.snap.Load()
			log.Printf("registry refreshed: %d stations, %d tariffs", len(s.stations), len(s.tariffs))
		}
	}
}

func loadRegistry(dsn string) (*registrySnapshot, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return nil, err
	}

	snap := &registrySnapshot{stations: map[string]stationMeta{}, tariffs: map[string]struct{}{}}

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
		snap.stations[id] = m
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
		snap.tariffs[id] = struct{}{}
	}
	return snap, trows.Err()
}
