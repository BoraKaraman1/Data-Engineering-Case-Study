# Batch layer (Airflow)

This is the batch layer. It is opt-in and runs on demand:

```bash
docker compose --profile airflow up
```

That opens the Airflow UI at http://localhost:8081. The default stack
(`docker compose up`) does not start it. It sits behind the `airflow` Compose
profile so a broken or slow-to-pull Airflow container can never appear in the
graded stack.

The `airflow` service builds a small image from `deploy/airflow/Dockerfile`
(base `apache/airflow:2.10.3-python3.12`) that bakes in the ClickHouse client
(`clickhouse-connect`, pinned in `deploy/airflow/requirements.txt`). The
dependency is installed once at image-build time, not re-installed on every
container boot.

## What it does not touch

Airflow does not participate in the streaming path. The always-on consumers are
supervised by the Kafka consumer-group coordinator (and by Kubernetes in
production), not by a scheduler. Airflow reads and rewrites ClickHouse partitions
on a schedule only, keeping the ingest hot path clear.

## The DAG

`ev_analytics_daily` is a linear chain of five tasks:

1. `freshness_check` is a data freshness gate: it fails the run if
   `ev.events_raw` is empty or its newest `ingested_at` is more than 24h old.
2. `optimize_closed_partition` runs `OPTIMIZE ... PARTITION <yyyymm> FINAL` on
   the previous closed calendar month's partition to eliminate the
   ReplacingMergeTree FINAL cost on reads. Partitions are monthly, so the
   current (active) month is never touched and no full-table optimize runs; it
   skips when that partition is already a single merged part.
3. `reconcile_revenue` recomputes yesterday's revenue exactly from
   `events_raw FINAL` (each event counted once) and overwrites
   `revenue_hourly` for the day, correcting the approximate streaming MV.
4. `dq_psi_gate` computes PSI (last 7 days vs the prior 7) for per-session
   energy delta and per-row power, plus the dead-letter rate, and fails on
   drift past the thresholds.
5. `enforce_ttl` logs partitions past the 13-month cutoff as export/drop
   candidates. The table's own TTL performs the actual drop.

## Why these tasks are batch, not stream

Merge/dedup compaction (`OPTIMIZE`) and the cross-day exact reconciliation are
scheduled, whole-partition operations. A per-message streaming consumer works
one event at a time inside a single insert block and cannot run a periodic
whole-day recompute or partition optimize. PSI drift and TTL enforcement are
periodic window comparisons, not per-event work.

## Executor reality

`airflow standalone` is a single container: scheduler plus webserver on SQLite
metadata, which provides the SequentialExecutor. This is adequate for a linear
chain (`t1 >> t2 >> t3 >> t4 >> t5`) triggered on demand. The spec mentioned
LocalExecutor, but LocalExecutor is incompatible with SQLite (`airflow db init`
fails), so it cannot be used. A production deployment would use Postgres as the
metadata database with LocalExecutor or CeleryExecutor.

The DAG has `schedule=None` so it does not trigger during grading. Trigger it
manually from the UI.
