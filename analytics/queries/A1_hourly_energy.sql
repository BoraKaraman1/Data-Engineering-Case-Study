-- A1: Hourly energy consumption (kWh) over the last 7 days of event-time.
--
-- energy_kwh is the session's CUMULATIVE meter register, so summing it raw would
-- over-count by ~(readings per session). We instead take per-session INCREMENTS
-- (this reading minus the previous one within the same session, ordered by event
-- time) and attribute each increment to the hour it was delivered. SESSION_STOP is
-- included so the final interval of a session is captured. FINAL resolves the
-- ReplacingMergeTree dedup for an exact result.
--
-- The per-session delta is computed over ALL of a session's readings (no time filter
-- inside the CTE); only the attributed delta rows are filtered into the window by the
-- outer query. Filtering before the lag would make the first in-window reading subtract
-- 0 instead of its true prior register, over-counting at the window's leading edge.
--
-- The window end is the latest EVENT time in the data, not wall-clock now(): with
-- time_acceleration > 1 the simulated event clock runs ahead of wall time, so now()
-- would clip the window. Anchoring to max(timestamp) keeps it in event-time.
WITH
    assumeNotNull((SELECT max(timestamp) FROM ev.events_raw)) AS win_end,
    win_end - INTERVAL 7 DAY AS win_start,
    readings AS (
        SELECT
            session_id,
            timestamp,
            energy_kwh - lagInFrame(energy_kwh) OVER (
                PARTITION BY session_id ORDER BY timestamp
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS delta_kwh
        FROM ev.events_raw FINAL
        WHERE event_type IN ('SESSION_START', 'METER_UPDATE', 'SESSION_STOP')
    )
SELECT
    toStartOfHour(timestamp) AS hour,
    round(sum(greatest(delta_kwh, 0)), 3) AS energy_kwh   -- clamp resets/out-of-order to 0
FROM readings
WHERE timestamp BETWEEN win_start AND win_end
GROUP BY hour
ORDER BY hour;
