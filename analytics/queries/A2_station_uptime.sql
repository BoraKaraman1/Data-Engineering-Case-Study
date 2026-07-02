-- A2: Station uptime / downtime ratio and the most problematic stations per operator.
--
-- The status timeline is reconstructed PURELY from STATUS_CHANGE events (the
-- authoritative status signal the processor also uses), so session events can't
-- double-count. Each status segment runs from its STATUS_CHANGE until the next one on
-- the same connector (leadInFrame), or the window end for the final open segment.
--
-- A connector that was already Available/Faulted BEFORE the window opened would lose
-- that leading segment if we only read in-window rows. So `changes` is the UNION of
-- (a) STATUS_CHANGE rows within [win_start, win_end] and (b) each connector's LATEST
-- status before win_start (argMax by timestamp), carried forward and stamped at
-- win_start. (For a data set that starts inside the window, branch (b) is simply empty.)
--
-- NOTE: the window end is the latest EVENT time in the data, not wall-clock now().
-- With time_acceleration > 1 the simulated event clock runs ahead of wall time, so
-- using now() as the open segment's end would make it negative. Anchoring to
-- max(timestamp) keeps every segment in event-time; greatest(..., 0) is a belt-and-
-- braces guard against any out-of-order arrival. Downtime = seconds Faulted;
-- uptime_ratio = 1 - downtime / total.
WITH
    assumeNotNull((SELECT max(timestamp) FROM ev.events_raw)) AS win_end,
    win_end - INTERVAL 7 DAY AS win_start,
    changes AS (
        SELECT
            operator_id,
            station_id,
            connector_id,
            status,
            timestamp AS ts
        FROM ev.events_raw FINAL
        WHERE event_type = 'STATUS_CHANGE'
          AND timestamp BETWEEN win_start AND win_end
        UNION ALL
        SELECT
            any(operator_id) AS operator_id,
            station_id,
            connector_id,
            argMax(status, timestamp) AS status,
            win_start AS ts
        FROM ev.events_raw FINAL
        WHERE event_type = 'STATUS_CHANGE'
          AND timestamp < win_start
        GROUP BY station_id, connector_id
    ),
    segments AS (
        SELECT
            operator_id,
            station_id,
            status,
            ts AS seg_start,
            leadInFrame(ts, 1, win_end) OVER (
                PARTITION BY station_id, connector_id ORDER BY ts
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            ) AS seg_end
        FROM changes
    )
SELECT
    operator_id,
    station_id,
    sum(greatest(dateDiff('second', seg_start, seg_end), 0)) AS total_s,
    sumIf(greatest(dateDiff('second', seg_start, seg_end), 0), status = 'Faulted') AS downtime_s,
    round(1 - sumIf(greatest(dateDiff('second', seg_start, seg_end), 0), status = 'Faulted')
             / nullIf(sum(greatest(dateDiff('second', seg_start, seg_end), 0)), 0), 4) AS uptime_ratio  -- nullIf: 0 total_s (all segments zero-duration) -> NULL, not nan/inf
FROM segments
GROUP BY operator_id, station_id
ORDER BY downtime_s DESC, operator_id
LIMIT 20;
