-- ClickHouse runs every .sql here on first start (docker-entrypoint-initdb.d).
-- Design: ClickHouse SELF-INGESTS from Kafka. The processor (Phase 2) writes a
-- FLAT, validated, deduped JSON stream to `charging-events-clean`; a Kafka engine
-- table reads it and a materialized view moves rows into a ReplacingMergeTree.
--
-- Why this shape:
--   * No row-by-row INSERTs from app code (the classic way to kill ClickHouse at
--     scale). The Kafka engine batches internally.
--   * ReplacingMergeTree is the SECOND (authoritative) dedup layer: it collapses
--     rows sharing the full ORDER BY key -- identical for a genuine re-send of one
--     event -- catching duplicates the processor's Redis window missed. Redis dedup
--     is a best-effort optimization; exact reads use FINAL / uniqExact(event_id).
--   * Columnar + time-series codecs keep the meter firehose small and fast.

CREATE DATABASE IF NOT EXISTS ev;

-- ---------------------------------------------------------------------------
-- 1) Kafka engine table over the CLEAN topic (flat JSON, one event per line).
--    This is a *consumer*, not storage; it is drained by the MV below.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ev.events_queue
(
    event_id      String,
    event_type    LowCardinality(String),
    station_id    LowCardinality(String),
    connector_id  UInt8,
    session_id    String,
    timestamp     DateTime64(3, 'UTC'),
    ingested_at   DateTime64(3, 'UTC'),
    operator_id   LowCardinality(String),
    lat           Float64,
    lon           Float64,
    city          LowCardinality(String),
    country       LowCardinality(String),
    power_kw      Float32,
    energy_kwh    Float32,
    voltage_v     Float32,
    current_a     Float32,
    soc_percent   UInt8,
    vehicle_brand LowCardinality(String),
    vehicle_model LowCardinality(String),
    ev_id         String,
    tariff_id     LowCardinality(String),
    cost_eur      Float32,
    error_code    LowCardinality(String),
    component     LowCardinality(String),
    status        LowCardinality(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'redpanda:9092',
    kafka_topic_list    = 'charging-events-clean',
    kafka_group_name    = 'clickhouse-clean-ingest',
    kafka_format        = 'JSONEachRow',
    kafka_num_consumers = 3,
    kafka_max_block_size = 1048576,
    kafka_skip_broken_messages = 0,   -- the processor validates + flattens every row, so
                                      -- the clean topic is well-formed by construction:
                                      -- fail loud on any drift rather than silently drop.
    input_format_skip_unknown_fields = 1,
    date_time_input_format = 'best_effort';

-- ---------------------------------------------------------------------------
-- 2) Landing table. ReplacingMergeTree(ingested_at) keeps the newest row for any
--    identical ORDER BY tuple. Two copies of one event share event_id AND every
--    other field, so they collapse on merge.
--
--    Partition: month of event-time (supports monthly/yearly retention + pruning).
--    Order:     (station_id, connector_id, event_type, timestamp, event_id)
--               -> station/connector locality for the analytics queries, with
--                  event_id last so the dedup key is unique per logical event.
--    Codecs:    DoubleDelta for monotonic timestamps, Gorilla for slowly-changing
--               float sensor values (power/energy/voltage/current).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ev.events_raw
(
    event_id      String,
    event_type    LowCardinality(String),
    station_id    LowCardinality(String),
    connector_id  UInt8,
    session_id    String,
    timestamp     DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    ingested_at   DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    operator_id   LowCardinality(String),
    lat           Float64,
    lon           Float64,
    city          LowCardinality(String),
    country       LowCardinality(String),
    power_kw      Float32 CODEC(Gorilla, ZSTD(1)),
    energy_kwh    Float32 CODEC(Gorilla, ZSTD(1)),
    voltage_v     Float32 CODEC(Gorilla, ZSTD(1)),
    current_a     Float32 CODEC(Gorilla, ZSTD(1)),
    soc_percent   UInt8,
    vehicle_brand LowCardinality(String),
    vehicle_model LowCardinality(String),
    ev_id         String,
    tariff_id     LowCardinality(String),
    cost_eur      Float32,
    error_code    LowCardinality(String),
    component     LowCardinality(String),
    status        LowCardinality(String)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (station_id, connector_id, event_type, timestamp, event_id)
TTL toDateTime(timestamp) + INTERVAL 13 MONTH
SETTINGS index_granularity = 8192;

-- 3) Pump: Kafka engine -> landing table. Explicit column list (not SELECT *) so a
--    schema drift between the queue and the landing table fails loudly instead of
--    silently mismapping columns.
CREATE MATERIALIZED VIEW IF NOT EXISTS ev.events_mv TO ev.events_raw AS
SELECT
    event_id, event_type, station_id, connector_id, session_id,
    timestamp, ingested_at, operator_id, lat, lon, city, country,
    power_kw, energy_kwh, voltage_v, current_a, soc_percent,
    vehicle_brand, vehicle_model, ev_id, tariff_id, cost_eur,
    error_code, component, status
FROM ev.events_queue;

-- ---------------------------------------------------------------------------
-- 4) Dead-letter path. The processor publishes rejected payloads (schema/validation
--    failures) to `charging-events-dlq` as {raw_payload, error, ingested_at}.
--    Kept queryable so "show me what got rejected and why" is one SQL away.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ev.dead_letter_queue
(
    raw_payload String,
    error       String,
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'redpanda:9092',
    kafka_topic_list    = 'charging-events-dlq',
    kafka_group_name    = 'clickhouse-dlq-ingest',
    kafka_format        = 'JSONEachRow',
    kafka_skip_broken_messages = 100,
    input_format_skip_unknown_fields = 1,
    date_time_input_format = 'best_effort';

CREATE TABLE IF NOT EXISTS ev.dead_letter
(
    raw_payload String,
    error       LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ingested_at);

CREATE MATERIALIZED VIEW IF NOT EXISTS ev.dead_letter_mv TO ev.dead_letter AS
SELECT raw_payload, error, ingested_at FROM ev.dead_letter_queue;

-- ---------------------------------------------------------------------------
-- Aggregating materialized views for the A1-A6 reports (hourly energy, station
-- uptime, revenue, fault geography) are added in Phase 3 alongside the queries,
-- so the rollups and the questions they answer live together. The landing table,
-- partition strategy, and dedup model above are the foundation they build on.
-- ---------------------------------------------------------------------------
