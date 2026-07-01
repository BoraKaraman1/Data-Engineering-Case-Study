-- A4: Revenue analysis and peak-hour contribution, broken down by operator, city and
-- tariff (the case's "tariff x energy x time-of-day").
--
-- Revenue is the billed cost_eur carried on SESSION_STOP (the source of truth, the
-- simulator already applied tariff x energy x time when it set it). Peak windows are
-- 07:00-09:00 and 17:00-20:00 in event time; peak_pct is the peak share of revenue.
-- FINAL for exact (dedup) revenue.
--
-- The window end is the latest EVENT time in the data, not wall-clock now(): with
-- time_acceleration > 1 the simulated clock runs ahead of wall time, so now() would
-- clip the window. Anchoring to max(timestamp) keeps it in event-time.
WITH
    assumeNotNull((SELECT max(timestamp) FROM ev.events_raw)) AS win_end,
    win_end - INTERVAL 30 DAY AS win_start,
    stops AS (
        SELECT
            operator_id,
            city,
            tariff_id,
            cost_eur,
            (toHour(timestamp) >= 7 AND toHour(timestamp) < 9)
              OR (toHour(timestamp) >= 17 AND toHour(timestamp) < 20) AS is_peak
        FROM ev.events_raw FINAL
        WHERE event_type = 'SESSION_STOP'
          AND timestamp BETWEEN win_start AND win_end
    )
SELECT
    operator_id,
    city,
    tariff_id,
    round(sum(cost_eur), 2) AS revenue_eur,
    round(sumIf(cost_eur, is_peak), 2) AS peak_revenue_eur,
    round(100 * sumIf(cost_eur, is_peak) / sum(cost_eur), 1) AS peak_pct,
    count() AS sessions
FROM stops
GROUP BY operator_id, city, tariff_id
ORDER BY revenue_eur DESC;
