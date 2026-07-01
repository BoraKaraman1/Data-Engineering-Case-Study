package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// SeedRegistry writes the generated station/connector roster into Postgres so it is the
// OLTP source of truth. The seed is AUTHORITATIVE: it TRUNCATEs and re-inserts inside one
// transaction, so re-running on a reused volume with a changed (or SHRUNK) roster can't
// leave stale stations behind for the processor to accept as valid.
func SeedRegistry(dsn string, stations []*Station) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Clear the roster first (both tables in one statement to satisfy the FK). tariffs
	// live in a separate table seeded by the DB init and are untouched.
	if _, err := tx.Exec(`TRUNCATE TABLE connectors, stations`); err != nil {
		return fmt.Errorf("truncate registry: %w", err)
	}

	stStmt, err := tx.Prepare(`INSERT INTO stations
		(station_id, operator_id, city, country, lat, lon, num_connectors)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`)
	if err != nil {
		return err
	}
	defer stStmt.Close()

	conStmt, err := tx.Prepare(`INSERT INTO connectors
		(station_id, connector_id, power_kw_rating, connector_type)
		VALUES ($1,$2,$3,$4)`)
	if err != nil {
		return err
	}
	defer conStmt.Close()

	for _, s := range stations {
		if _, err := stStmt.Exec(s.ID, s.OperatorID, s.City, s.Country, s.Lat, s.Lon, len(s.Connectors)); err != nil {
			return fmt.Errorf("insert station %s: %w", s.ID, err)
		}
		for _, c := range s.Connectors {
			if _, err := conStmt.Exec(s.ID, c.ID, c.PowerRating, c.Type); err != nil {
				return fmt.Errorf("insert connector %s/%d: %w", s.ID, c.ID, err)
			}
		}
	}
	return tx.Commit()
}
