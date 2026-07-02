-- OLTP source of truth for reference data. This is the relational, mutable side of
-- the OLAP/OLTP split: the station and tariff REGISTRY lives here; the immutable
-- event firehose lives in ClickHouse. The simulator writes its generated roster
-- here on startup; the processor (Phase 2) loads valid station IDs from here once
-- into memory to validate events referentially (an event for an unknown station_id
-- is dead-lettered) without touching Postgres on the hot path.

CREATE TABLE IF NOT EXISTS stations (
    station_id     TEXT PRIMARY KEY,
    operator_id    TEXT NOT NULL,
    city           TEXT NOT NULL,
    country        TEXT NOT NULL,
    lat            DOUBLE PRECISION NOT NULL,
    lon            DOUBLE PRECISION NOT NULL,
    num_connectors SMALLINT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS connectors (
    station_id      TEXT NOT NULL REFERENCES stations(station_id),
    connector_id    SMALLINT NOT NULL,
    power_kw_rating REAL NOT NULL,
    connector_type  TEXT NOT NULL,
    PRIMARY KEY (station_id, connector_id)
);

CREATE TABLE IF NOT EXISTS tariffs (
    tariff_id         TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    price_per_kwh_eur REAL NOT NULL,
    peak_multiplier   REAL NOT NULL DEFAULT 1.0
);

-- Tariff ROWS are not seeded here. The simulator's registry-seed (SeedRegistry)
-- populates this table transactionally from its canonical Go tariffCatalog, so the
-- prices have a single source of truth shared with the cost_eur math -- no drift
-- between what the DB holds and what the simulator bills.

CREATE INDEX IF NOT EXISTS idx_stations_operator ON stations (operator_id);
CREATE INDEX IF NOT EXISTS idx_stations_city ON stations (city);
