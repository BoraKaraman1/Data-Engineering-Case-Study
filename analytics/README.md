# Analytics (Phase 3)

Python + SQL layer for the analytical queries A1–A6 and the report. Delivered and
runnable against the live ClickHouse.

## Contents

- `queries/` holds one `.sql` per analytical question, run at query time against
  `ev.events_raw`:
  - **A1** hourly energy over the last 7 days (per-session energy **deltas**, not raw
    sums; window anchored to the latest event-time, not wall-clock `now()`)
  - **A2** station uptime / downtime ratio per operator, from the STATUS_CHANGE timeline
  - **A3** average session duration + energy by vehicle brand
  - **A4** revenue broken down by operator × city × tariff, with peak-rate share
    (tariff × energy × time-of-day) taken from the billed `is_peak_priced` flag,
    not a re-derived clock hour
  - **A5** geographic distribution of FAULT events, deduped by `event_id`
  - **A6** anomaly detection: sessions whose average power is > 2σ above the fleet mean,
    read with FINAL so duplicate METER_UPDATEs can't skew the average or z-scores (bonus)
- `report.ipynb` runs A1–A6 against ClickHouse (HTTP, `:8123`) and charts each result
- `output/` holds the generated results: `A1.csv … A6.csv` (one per query) plus a `.png`
  chart each
- `requirements.txt` lists `clickhouse-connect`, `pandas`, `matplotlib`, `jupyter`

## Run

```bash
pip install -r requirements.txt
jupyter nbconvert --to notebook --execute --inplace report.ipynb
```

The notebook resolves the repo root automatically (or honours `PIPELINE_ROOT`), so it
runs from the repo root or from `analytics/`; override the target with `CLICKHOUSE_HOST`
/ `CLICKHOUSE_PORT`. Each run overwrites the CSVs in `output/`.

## Correctness notes

- **Energy trap.** `energy_kwh` is the session's *cumulative* meter register, so every
  energy figure uses per-session deltas (`max−min` per session, or the consecutive-reading
  increment), never `SUM(energy_kwh)`, which would over-count by ~(readings per session).
  See `../docs/ARCHITECTURE.md` §6.
- **Event-time windows.** The simulator runs with `time_acceleration > 1`, so the event
  clock runs ahead of wall time. Time-windowed queries (A1/A2/A4/A5) anchor to
  `max(timestamp)` in the data rather than `now()`, which would clip the window.
- **Peak = billed, not derived.** A4's peak revenue comes from an `is_peak_priced` flag
  the simulator sets when it applies the peak multiplier, carried through to the clean
  row — so peak contribution matches what was charged, not a downstream clock-hour guess.
  See `../docs/ARCHITECTURE.md` §10.

Only the revenue rollup (`deploy/clickhouse/init/02_aggregates.sql`) is a streaming
materialized view; A1/A2/A3/A5/A6 are query-time exact analytics because they need
cross-block state (per-session deltas, status timelines) a streaming MV can't hold.
