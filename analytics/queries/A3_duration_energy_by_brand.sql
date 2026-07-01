-- A3: Average charging duration and energy amount, by vehicle brand.
--
-- vehicle_brand rides only on SESSION_START, so it is pulled with anyIf. Duration is
-- SESSION_STOP time minus SESSION_START time per session. Energy is the per-session
-- delta max(energy_kwh) - min(energy_kwh) (NOT SUM, which would over-count the
-- cumulative register). Only completed sessions (a START and a STOP) are counted.
WITH sessions AS (
    SELECT
        session_id,
        anyIf(vehicle_brand, event_type = 'SESSION_START') AS brand,
        minIf(timestamp, event_type = 'SESSION_START') AS t_start,
        maxIf(timestamp, event_type = 'SESSION_STOP') AS t_stop,
        max(energy_kwh) - min(energy_kwh) AS energy_kwh
    FROM ev.events_raw FINAL
    WHERE event_type IN ('SESSION_START', 'METER_UPDATE', 'SESSION_STOP')
    GROUP BY session_id
    HAVING t_start > toDateTime('1971-01-01') AND t_stop >= t_start
)
SELECT
    brand,
    count() AS sessions,
    round(avg(dateDiff('second', t_start, t_stop)) / 60, 1) AS avg_duration_min,
    round(avg(energy_kwh), 2) AS avg_energy_kwh
FROM sessions
WHERE brand != ''
GROUP BY brand
ORDER BY sessions DESC;
