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

It deliberately stays off the streaming path. The always-on consumers are
supervised by the Kafka consumer-group coordinator (and by Kubernetes in
production), not by a scheduler. Airflow only reads and rewrites ClickHouse
partitions on a schedule; it never sits in the ingest hot path.

## The DAG

`ev_analytics_daily` is a linear chain of five tasks:

1. `freshness_check` is a data freshness gate: it fails the run if
   `ev.events_raw` is empty or its newest `ingested_at` is more than 24h old.
2. `optimize_closed_partition` runs `OPTIMIZE ... PARTITION <yyyymm> FINAL` on
   the previous closed calendar month's partition, so reads stop paying the
   ReplacingMergeTree FINAL cost. Partitions are monthly, so it never touches
   the current (active) month and never runs a full-table optimize; it also
   skips when that partition is already a single merged part.
3. `reconcile_revenue` recomputes yesterday's revenue exactly from
   `events_raw FINAL` (each event counted once) and overwrites
   `revenue_hourly` for the day, correcting the approximate streaming MV.
4. `dq_psi_gate` computes PSI (last 7 days vs the prior 7) for per-session
   energy delta and per-row power, plus the dead-letter rate, and fails on
   drift past the thresholds.
5. `enforce_ttl` is report-only: it logs partitions past the 13-month cutoff as
   export/drop candidates. The table's own TTL performs the actual drop.

## Why these tasks are batch, not stream

Merge/dedup compaction (`OPTIMIZE`) and the cross-day exact reconciliation are
inherently scheduled, whole-partition operations. A per-message streaming
consumer works one event at a time inside a single insert block, so it cannot
run a periodic whole-day recompute or a partition optimize. PSI drift and TTL
enforcement are periodic window comparisons, not per-event work either.

## Executor reality

`airflow standalone` is a single container: scheduler plus webserver on SQLite
metadata, which gives the SequentialExecutor. That is sufficient for this DAG
because it is a linear chain (`t1 >> t2 >> t3 >> t4 >> t5`) triggered on demand.
The spec mentioned LocalExecutor, but LocalExecutor is incompatible with the
SQLite metadata DB (`airflow db init` fails and the container never comes up),
so forcing it would break the container. A production deployment would use a
real metadata database (Postgres) with LocalExecutor or CeleryExecutor.

The DAG is `schedule=None` on purpose so it never fires during grading. Trigger
it manually from the UI.
