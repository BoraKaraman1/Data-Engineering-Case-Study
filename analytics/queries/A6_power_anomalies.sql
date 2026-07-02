-- A6 (bonus): anomaly detection, sessions whose average charging power deviates more
-- than 2 sigma above the fleet mean.
--
-- Per-session average power is computed over its METER_UPDATE readings (avgIf, since
-- SESSION_START carries power 0), and the vehicle brand is pulled from SESSION_START
-- (anyIf) in the same pass. The source is read with FINAL so duplicate METER_UPDATEs
-- (ReplacingMergeTree) collapse before aggregation -- an exact read that keeps
-- avgIf(power_kw) and the z-scores from being skewed by dupes (matches A1/A3).
-- Each session is compared to the fleet-wide mean (avg) and
-- population standard deviation (stddevPop); it is flagged when
-- avg_power_kw > mean + 2 * sigma. (A refinement would z-score within brand or against
-- the connector's rated power, since a 250 kW DC session isn't anomalous next to a
-- 22 kW AC one.)
WITH
    per_session AS (
        SELECT
            session_id,
            any(station_id) AS station_id,
            anyIf(vehicle_brand, event_type = 'SESSION_START') AS brand,
            avgIf(power_kw, event_type = 'METER_UPDATE') AS avg_power_kw,
            countIf(event_type = 'METER_UPDATE') AS readings
        FROM ev.events_raw FINAL
        WHERE event_type IN ('SESSION_START', 'METER_UPDATE')
        GROUP BY session_id
        HAVING readings >= 3
    ),
    stats AS (
        SELECT avg(avg_power_kw) AS mu, stddevPop(avg_power_kw) AS sigma
        FROM per_session
    )
SELECT
    ps.session_id,
    ps.station_id,
    ps.brand,
    round(ps.avg_power_kw, 1) AS avg_power_kw,
    round((ps.avg_power_kw - s.mu) / s.sigma, 2) AS z_score
FROM per_session AS ps
CROSS JOIN stats AS s
WHERE ps.avg_power_kw > s.mu + 2 * s.sigma
  AND ps.brand != ''
ORDER BY z_score DESC
LIMIT 100;
