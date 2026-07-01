-- A4: Revenue analysis and peak-rate contribution, broken down by operator, city and
-- tariff (the case's "tariff x energy x time-of-day").
--
-- Revenue is the billed cost_eur carried on SESSION_STOP (the source of truth, the
-- simulator already applied tariff x energy x time when it set it). "Peak" is revenue
-- the simulator actually billed at the peak multiplier, recorded per SESSION_STOP as
-- is_peak_priced (1 = peak rate applied). It is NOT re-derived from a wall-clock hour
-- downstream, so it cannot drift from the rate the customer was charged. peak_pct is the
-- peak-rate share of revenue. FINAL for exact (dedup) revenue.
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
            is_peak_priced
        FROM ev.events_raw FINAL
        WHERE event_type = 'SESSION_STOP'
          AND timestamp BETWEEN win_start AND win_end
    )
SELECT
    operator_id,
    city,
    tariff_id,
    round(sum(cost_eur), 2) AS revenue_eur,
    round(sumIf(cost_eur, is_peak_priced = 1), 2) AS peak_revenue_eur,
    round(100 * sumIf(cost_eur, is_peak_priced = 1) / sum(cost_eur), 1) AS peak_pct,
    count() AS sessions
FROM stops
GROUP BY operator_id, city, tariff_id
ORDER BY revenue_eur DESC;
