-- A5: Geographic distribution of FAULT events (error density by city).
--
-- uniqExact(event_id) counts distinct faults, so it self-dedups (no FINAL needed even
-- if a duplicate FAULT_ALERT slipped past the Redis window). faults_per_station is a
-- density measure that normalises for how many stations a city has.
--
-- The window end is the latest EVENT time in the data, not wall-clock now(): with
-- time_acceleration > 1 the simulated clock runs ahead of wall time, so now() would
-- clip the window. Anchoring to max(timestamp) keeps it in event-time.
WITH
    assumeNotNull((SELECT max(timestamp) FROM ev.events_raw)) AS win_end,
    win_end - INTERVAL 30 DAY AS win_start
SELECT
    city,
    uniqExact(event_id) AS fault_count,
    uniqExact(station_id) AS stations_affected,
    round(uniqExact(event_id) / uniqExact(station_id), 2) AS faults_per_station,
    round(avg(lat), 4) AS lat,
    round(avg(lon), 4) AS lon
FROM ev.events_raw
WHERE event_type = 'FAULT_ALERT'
  AND timestamp BETWEEN win_start AND win_end
GROUP BY city
ORDER BY fault_count DESC;
