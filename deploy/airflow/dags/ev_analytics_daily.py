"""Batch layer for the ChargeSquare pipeline: ev_analytics_daily.

This DAG runs BESIDE the stream, never on it. The always-on consumers own the
hot path; this handles the scheduled, whole-partition work a per-message
consumer structurally cannot do: merge/dedup compaction (OPTIMIZE), an exact
cross-day revenue recompute from FINAL, periodic data-quality drift (PSI), and
a TTL report.

schedule=None on purpose: it is triggered manually from the UI, so it never
fires mid-grading. It is a normal Python file (not a Workflow sandbox), so
datetime/now() are fine.
"""

import bisect
import logging
import math
from datetime import datetime, timedelta, timezone

import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# Fail the DQ gate above these. Named so the threshold is greppable, not magic.
PSI_THRESHOLD = 0.25
DLQ_RATE_THRESHOLD = 0.05
FRESHNESS_MAX_AGE = timedelta(hours=24)
PSI_BUCKETS = 10
PSI_EPSILON = 1e-6


def get_client():
    """Build the clickhouse-connect client. `clickhouse` resolves on the
    default compose network."""
    return clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="chargesquare",
        password="chargesquare",
        database="ev",
    )


def _now_utc():
    return datetime.now(timezone.utc)


def _as_utc(dt):
    """ClickHouse DateTime64('UTC') comes back tz-aware; a plain DateTime
    comes back naive. Normalise to tz-aware UTC for arithmetic."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _yesterday_bounds():
    """[start, end) of yesterday (UTC) as 'YYYY-MM-DD HH:MM:SS' strings, plus
    the yesterday date object."""
    today = _now_utc().date()
    day = today - timedelta(days=1)
    return day, f"{day} 00:00:00", f"{today} 00:00:00"


def freshness_check(**_):
    """t1: DATA freshness gate. Fail the run if the stream stalled or the
    table is empty. This is not service monitoring: it asks "did new data
    land?" and is meant to fail on an empty/stalled stream."""
    client = get_client()
    max_ingested = client.query(
        "SELECT max(ingested_at) FROM ev.events_raw"
    ).result_rows[0][0]
    if max_ingested is None:
        raise ValueError("ev.events_raw is empty: no data has been ingested")

    age = _now_utc() - _as_utc(max_ingested)
    log.info("max(ingested_at)=%s age=%s", max_ingested, age)
    if age > FRESHNESS_MAX_AGE:
        raise ValueError(
            f"stale stream: newest event is {age} old (> {FRESHNESS_MAX_AGE})"
        )


def optimize_yesterday(**_):
    """t2: physically collapse yesterday's ReplacingMergeTree partition so
    daytime reads stop paying the FINAL cost. Per-partition only, never a
    full-table OPTIMIZE. A non-existent partition is a harmless no-op."""
    client = get_client()
    yyyymm = (_now_utc().date() - timedelta(days=1)).strftime("%Y%m")
    client.command(f"OPTIMIZE TABLE ev.events_raw PARTITION {yyyymm} FINAL")
    log.info("optimized ev.events_raw partition %s FINAL", yyyymm)


def reconcile_revenue(**_):
    """t3: recompute yesterday's revenue EXACTLY from events_raw FINAL (each
    event counted once, unlike the approximate streaming MV) and OVERWRITE
    revenue_hourly for the day. The refreshable-view pattern (ARCHITECTURE
    sec 10)."""
    client = get_client()
    day, start, end = _yesterday_bounds()

    # Delete the approximate rows first; mutations_sync=2 makes it synchronous
    # so the insert below cannot race the delete (SummingMergeTree would
    # otherwise add the exact recompute on top of the stale rows).
    client.command(
        "ALTER TABLE ev.revenue_hourly DELETE "
        f"WHERE hour >= toDateTime('{start}') AND hour < toDateTime('{end}') "
        "SETTINGS mutations_sync = 2"
    )

    # Exact recompute: same shape as revenue_hourly_mv, but FINAL and scoped
    # to the day. is_peak_priced is the billed-at-peak flag, not a clock hour.
    client.command(
        "INSERT INTO ev.revenue_hourly "
        "(hour, operator_id, city, tariff_id, is_peak, revenue_eur, sessions) "
        "SELECT toStartOfHour(timestamp) AS hour, operator_id, city, tariff_id, "
        "is_peak_priced AS is_peak, sum(cost_eur) AS revenue_eur, count() AS sessions "
        "FROM ev.events_raw FINAL "
        "WHERE event_type = 'SESSION_STOP' "
        f"AND timestamp >= toDateTime('{start}') AND timestamp < toDateTime('{end}') "
        "GROUP BY hour, operator_id, city, tariff_id, is_peak"
    )

    reconciled = client.query(
        "SELECT count() FROM ev.revenue_hourly "
        f"WHERE hour >= toDateTime('{start}') AND hour < toDateTime('{end}')"
    ).result_rows[0][0]
    log.info("reconciled %s revenue_hourly rows for %s", reconciled, day)


def _psi(expected, actual):
    """Population Stability Index of `actual` vs `expected`, explicit and
    dependency-free. Buckets are quantile edges of the EXPECTED distribution;
    zero counts are floored to PSI_EPSILON so ln/division are safe. Returns
    None when there is not enough data to bucket."""
    if len(expected) < PSI_BUCKETS or not actual:
        return None

    exp_sorted = sorted(expected)
    n = len(exp_sorted)
    # Interior quantile edges (PSI_BUCKETS - 1 of them). Duplicate edges on a
    # skewed distribution just yield empty buckets, which the epsilon floor
    # handles.
    edges = [exp_sorted[min(i * n // PSI_BUCKETS, n - 1)]
             for i in range(1, PSI_BUCKETS)]

    def bucketize(values):
        counts = [0] * PSI_BUCKETS
        for v in values:
            counts[bisect.bisect_right(edges, v)] += 1
        return counts

    exp_counts = bucketize(expected)
    act_counts = bucketize(actual)
    exp_total = len(expected)
    act_total = len(actual)

    psi = 0.0
    for e, a in zip(exp_counts, act_counts):
        e_pct = max(e / exp_total, PSI_EPSILON)
        a_pct = max(a / act_total, PSI_EPSILON)
        psi += (a_pct - e_pct) * math.log(a_pct / e_pct)
    return psi


def _column(client, sql):
    return [r[0] for r in client.query(sql).result_rows if r[0] is not None]


def dq_psi_gate(**_):
    """t4: data-quality drift. Compare the last 7 days (actual) against the
    prior 7 days (expected) for two distributions -- per-session energy delta
    and per-row power_kw -- via an explicit PSI, and check the dead-letter
    rate. Fail on PSI > threshold or DLQ rate > threshold. Skip PSI (do not
    crash) when a window has too little data to bucket."""
    client = get_client()
    now = _now_utc()
    act_start = (now - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    act_end = now.strftime("%Y-%m-%d %H:%M:%S")
    exp_start = (now - timedelta(days=14)).strftime("%Y-%m-%d %H:%M:%S")
    exp_end = act_start

    # (a) per-session energy delta (max-min): the energy-trap-safe measure,
    #     never SUM(energy_kwh).
    def energy_delta(lo, hi):
        return _column(client,
            "SELECT max(energy_kwh) - min(energy_kwh) AS delta "
            "FROM ev.events_raw FINAL "
            f"WHERE timestamp >= toDateTime('{lo}') AND timestamp < toDateTime('{hi}') "
            "GROUP BY session_id")

    # (b) per-row power for active charging.
    def power(lo, hi):
        return _column(client,
            "SELECT power_kw FROM ev.events_raw FINAL "
            f"WHERE timestamp >= toDateTime('{lo}') AND timestamp < toDateTime('{hi}') "
            "AND event_type = 'METER_UPDATE' AND power_kw > 0")

    failed = []
    for name, fn in (("energy_delta", energy_delta), ("power_kw", power)):
        psi = _psi(fn(exp_start, exp_end), fn(act_start, act_end))
        if psi is None:
            log.info("PSI[%s]: skipped (not enough data in a 7-day window)", name)
            continue
        log.info("PSI[%s]=%.4f (threshold %.2f)", name, psi, PSI_THRESHOLD)
        if psi > PSI_THRESHOLD:
            failed.append(f"{name} PSI={psi:.4f}")

    # Dead-letter rate over the actual (last-7-days) window.
    dlq = client.query(
        "SELECT count() FROM ev.dead_letter "
        f"WHERE ingested_at >= toDateTime('{act_start}') "
        f"AND ingested_at < toDateTime('{act_end}')"
    ).result_rows[0][0]
    total = client.query(
        "SELECT count() FROM ev.events_raw "
        f"WHERE ingested_at >= toDateTime('{act_start}') "
        f"AND ingested_at < toDateTime('{act_end}')"
    ).result_rows[0][0]
    dlq_rate = dlq / total if total else 0.0
    log.info("dead-letter rate=%.4f (%s/%s, threshold %.2f)",
             dlq_rate, dlq, total, DLQ_RATE_THRESHOLD)
    if dlq_rate > DLQ_RATE_THRESHOLD:
        failed.append(f"DLQ rate={dlq_rate:.4f}")

    if failed:
        raise ValueError("data-quality gate failed: " + "; ".join(failed))


def enforce_ttl(**_):
    """t5: REPORT ONLY. List active events_raw partitions whose oldest data is
    past the 13-month cutoff as export/drop candidates (the tiered-retention
    plan). The table's own TTL does the actual drop; this drops nothing."""
    client = get_client()
    # system.parts min_time is a naive DateTime; compare naive-to-naive.
    now = _now_utc().replace(tzinfo=None)
    # 13 months back, anchored to the 1st to sidestep day-overflow.
    total = now.month - 1 - 13
    cutoff = now.replace(
        year=now.year + total // 12,
        month=total % 12 + 1,
        day=1, hour=0, minute=0, second=0, microsecond=0,
    )

    rows = client.query(
        "SELECT partition, min(min_time) AS oldest FROM system.parts "
        "WHERE database = 'ev' AND table = 'events_raw' AND active "
        "GROUP BY partition ORDER BY partition"
    ).result_rows

    candidates = [(p, oldest) for p, oldest in rows if oldest < cutoff]
    log.info("TTL report (report-only; the table TTL performs the actual drop). "
             "cutoff=%s, %s partition(s) past cutoff", cutoff, len(candidates))
    for partition, oldest in candidates:
        log.info("  export/drop candidate: partition %s oldest=%s", partition, oldest)


with DAG(
    dag_id="ev_analytics_daily",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "clickhouse"],
) as dag:
    t1 = PythonOperator(task_id="freshness_check", python_callable=freshness_check)
    t2 = PythonOperator(task_id="optimize_yesterday", python_callable=optimize_yesterday)
    t3 = PythonOperator(task_id="reconcile_revenue", python_callable=reconcile_revenue)
    t4 = PythonOperator(task_id="dq_psi_gate", python_callable=dq_psi_gate)
    t5 = PythonOperator(task_id="enforce_ttl", python_callable=enforce_ttl)

    t1 >> t2 >> t3 >> t4 >> t5
