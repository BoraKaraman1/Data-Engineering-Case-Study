-- Phase 3 aggregate: the APPROXIMATE fast-path revenue rollup for A4's dashboards.
--
-- Streaming trade-off (read before trusting the number): this MV fires on each INSERT
-- block into ev.events_raw and sums cost_eur into a SummingMergeTree BEFORE the
-- ReplacingMergeTree dedup collapses anything. A duplicate SESSION_STOP that escapes the
-- processor's Redis dedup window is collapsed in events_raw on the next merge, but this
-- rollup has ALREADY summed its cost_eur twice. So treat this view as a fast, slightly
-- approximate dashboard aggregate, NOT as the books.
--
-- AUTHORITATIVE exact revenue is the A4 report query (analytics/queries/A4_revenue.sql),
-- which reads ev.events_raw FINAL and therefore counts each event once. The dedup-safe
-- way to keep a pre-aggregate would be a REFRESHABLE materialized view that periodically
-- recomputes this rollup from events_raw FINAL, trading freshness for exactness.
--
-- Why revenue can be a streaming MV at all while the energy rollups (A1, A3) cannot:
-- cost_eur is a per-SESSION_STOP SCALAR, correct to sum one row per session within a
-- single insert block. Per-session ENERGY deltas need max-min across a whole session,
-- whose readings span many insert blocks, so a streaming MV can't compute them (block
-- locality). Those stay query-time.

CREATE TABLE IF NOT EXISTS ev.revenue_hourly
(
    hour        DateTime('UTC'),
    operator_id LowCardinality(String),
    city        LowCardinality(String),
    tariff_id   LowCardinality(String),
    is_peak     UInt8,
    revenue_eur Float64,
    sessions    UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, operator_id, city, tariff_id, is_peak);

CREATE MATERIALIZED VIEW IF NOT EXISTS ev.revenue_hourly_mv TO ev.revenue_hourly AS
SELECT
    toStartOfHour(timestamp) AS hour,
    operator_id,
    city,
    tariff_id,
    toUInt8((toHour(timestamp) >= 7 AND toHour(timestamp) < 9)
         OR (toHour(timestamp) >= 17 AND toHour(timestamp) < 20)) AS is_peak,
    sum(cost_eur) AS revenue_eur,
    count() AS sessions
FROM ev.events_raw
WHERE event_type = 'SESSION_STOP'
GROUP BY hour, operator_id, city, tariff_id, is_peak;
