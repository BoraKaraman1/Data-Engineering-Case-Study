# Architecture & Design Decisions

EV charging data pipeline — real-time + analytics. This document explains *why* each
piece is what it is, the alternatives considered, and what is deliberately left for
later. The analytics layer (Phase 3) is in §10 and the measured performance / scale
test (Phase 4) is in §11.

---

## 1. Requirements, restated

Two read patterns with very different shapes have to be served from one ingest stream:

- **Operational / real-time.** "What is connector X doing *right now*?" Point lookups
  on the latest state per connector, target **< 100 ms**, and the event has to be
  visible **< 1 s** after it happens.
- **Analytical.** Hourly/monthly/yearly aggregations over the full history — energy,
  uptime, revenue, fault geography — scanning large time ranges.

Plus: a synthetic source at **10k–100k events/sec**, **at-least-once** delivery
(duplicates are expected and must be handled), and late / out-of-order arrival.

One store cannot do both well. A row store tuned for low-latency point reads is poor
at scanning hundreds of millions of rows for an aggregate; a columnar OLAP store is
excellent at the scan and wrong for a hot single-key lookup. So the design splits the
stores by access pattern and keeps a single validated ingest path feeding both.

---

## 2. Architecture

```mermaid
flowchart LR
    SIM["Simulator (Go)\n100s–10000s stations\nsession state machine"]
    subgraph KAFKA["Redpanda (Kafka API)"]
      RAW["charging-events-raw\n12 partitions"]
      CLEAN["charging-events-clean\n12 partitions"]
      DLQ["charging-events-dlq"]
    end
    subgraph PROC["Processor (Go)"]
      RT["realtime consumer group\nvalidate → current state"]
      AN["analytics consumer group\nvalidate → dedup → flatten"]
    end
    REDIS[("Redis\ncurrent state\n<100ms")]
    PG[("PostgreSQL\nstation / tariff registry\nOLTP source of truth")]
    subgraph CH["ClickHouse"]
      QEUE["Kafka engine tables"]
      RAWT["events_raw\nReplacingMergeTree\npartition by month"]
      MV["revenue_hourly\nSummingMergeTree MV\n(A4 fast-path only)"]
      DL["dead_letter"]
    end
    PROM["Prometheus"]
    GRAF["Grafana"]
    RPT["A1–A6 report\n(query-time, FINAL)"]

    SIM -->|key = station_id| RAW
    RAW --> RT --> REDIS
    RAW --> AN -->|valid, deduped, flat| CLEAN
    AN -->|invalid| DLQ
    PG -. loaded once into memory .-> PROC
    SIM -. seeds roster .-> PG
    CLEAN --> QEUE --> RAWT --> MV
    RAWT -. query-time A1–A6 .-> RPT
    DLQ --> DL
    SIM -. /metrics .-> PROM
    PROC -. /metrics .-> PROM
    PROM --> GRAF
```

---

## 3. Store selection

### Kafka (Redpanda) — ingest backbone

A durable, partitioned log decouples a bursty producer from downstream consumers and
lets multiple independent consumers read the same stream at their own pace — which is
exactly how the real-time and analytics paths are separated (Section 5). Partitioning
by `station_id` gives **per-station ordering** without a global bottleneck.

**Redpanda** over Apache Kafka for this exercise: a single binary, no JVM and no
ZooKeeper/KRaft sidecar, so `docker compose up` is clean and the footprint on a
laptop is small. It is Kafka-API compatible, so nothing downstream is Redpanda-specific
and a production move to MSK / Confluent / Strimzi is a config change, not a rewrite.

### Redis — real-time current state

The operational question is a **point lookup of the latest value per connector**, not
a time-series scan. That is a key-value access pattern, and Redis serves it from memory
in well under a millisecond. The processor maintains one hash per connector
(`station:{id}:{conn}` → power, status, soc, session, last-seen), so "current state"
is a single `HGETALL`.

Alternatives considered: **TimescaleDB / InfluxDB** are time-series stores and would
work, but they are built for *range* queries over recent history; for a pure
latest-value lookup they are heavier than a key-value store and add query latency.
TimescaleDB is attractive because it is PostgreSQL (honoring the advert) — but the
access pattern, not the badge, should pick the store, and the pattern here is
key-value. **Cassandra** is over-provisioned for a working-set that fits in memory.

Duplicates on this path are harmless: re-applying the same `METER_UPDATE` to current
state is idempotent (last write wins), so the real-time path does not need strict
dedup, which keeps its latency low.

### ClickHouse — analytics

The analytical queries scan large time ranges and aggregate. Columnar storage reads
only the columns a query touches; vectorised execution and the MergeTree family make
A1–A6 fast even over hundreds of millions of rows. ClickHouse also **self-ingests from
Kafka** via its Kafka engine, so there is no row-by-row `INSERT` from application code
(the classic way to fall over at scale) — the engine batches internally.

Alternatives: **DuckDB** is excellent but embedded/single-process and not built for a
continuously-ingesting streaming sink. **BigQuery** is a managed warehouse — great
analytics, wrong fit for a self-contained local stack with a < 1 s freshness goal and
no streaming-insert cost model.

### PostgreSQL — registry (OLTP source of truth)

Reference data — stations, connectors, tariffs — is small, relational, and mutable.
It belongs in an OLTP store, not duplicated as the authority inside the event
firehose. This is the OLAP/OLTP split applied honestly: Postgres owns the *registry*;
ClickHouse owns the immutable *events*. The processor loads valid station IDs from
Postgres **once into memory** at startup for referential validation (an event for an
unknown station is dead-lettered) — so Postgres is never on the hot path and adds no
throughput risk. It is also the most cleanly **droppable** component if scope tightens.

---

## 4. Data model & schema

### Event schema: nested raw, flat clean

The simulator emits **nested** JSON to the raw topic (OCPI/OCPP-flavoured:
`location{}`, `meter{}`, `vehicle{}`, `fault{}`), which is realistic and what a real
device/CPO would send. The processor **flattens** it when producing to the clean
topic, because a flat row maps directly onto ClickHouse columns and avoids nested-JSON
parsing in the hot ingest path. So the transform earns its place: nested-and-realistic
on the way in, flat-and-analytics-friendly on the way to storage.

Key fields: `event_id` (UUID — the dedup key), `event_type`, `station_id`,
`connector_id`, `session_id`, `timestamp` (event-time, ms, UTC), plus the meter /
vehicle / location / fault sub-objects and `cost_eur` on stop.

### ClickHouse table design

```
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (station_id, connector_id, event_type, timestamp, event_id)
TTL toDateTime(timestamp) + INTERVAL 13 MONTH
```

- **ReplacingMergeTree(ingested_at)** is the *second* dedup layer. Two copies of an
  event share `event_id` and every other field, so they collapse to one on merge;
  `ingested_at` is the version that decides which copy survives. This catches any
  late-arriving duplicate that fell outside the processor's Redis dedup window.
  (Caveat: collapsing happens on background merge, so exact-once reads use `FINAL`
  or `argMax`/`GROUP BY` — applied in the A-queries where correctness needs it.)
- **Partition by month of event-time** matches the monthly/yearly reporting grain and
  lets the engine prune whole partitions; the 13-month TTL drops old data cheaply.
- **ORDER BY** leads with `station_id, connector_id` for query locality (most reports
  filter/group by station), with `event_id` last so the dedup key is unique per event.
- **Codecs**: `DoubleDelta` for monotonic timestamps, `Gorilla` for slowly-changing
  float sensor values (power/energy/voltage/current), `LowCardinality` for the small
  string domains (event_type, city, operator, tariff). These cut storage and I/O
  substantially on exactly the columns the firehose is dominated by.

---

## 5. Pipeline design

### Two consumer groups, not one

The real-time and analytics paths have opposite priorities: real-time wants **lowest
latency**, analytics wants **highest throughput** and exactly-once semantics. Putting
them in one consumer would couple a slow batch write to the latency-sensitive state
update. Kafka lets multiple consumer **groups** each read the full stream
independently, so:

- **realtime consumer group** → validate → update Redis current state. No strict
  dedup (idempotent), tuned for latency.
- **analytics consumer group** → validate → dedup → flatten → produce to clean topic
  (which ClickHouse drains). Batched, tuned for throughput.

Failure isolation falls out for free: if ClickHouse ingestion stalls, the analytics
group lags but the real-time view stays fresh, and vice versa. The simpler
single-consumer design is a valid MVP; this is the version that answers the case's
"single pipeline vs separate" question with the stronger trade-off.

### Deduplication (the at-least-once requirement)

The dedup **key** is `event_id` (UUID), not `session_id + timestamp` — the latter
collides across the many meter readings of one session and would wrongly drop distinct
events. Correctness is layered, and the point that matters is **where it lives**:

1. **Hot path (best-effort optimization):** the analytics consumer orders each event
   `EXISTS event_id → produce to clean → Mark event_id` (`SET`, `EX <ttl>`). Marking
   only *after* a durable produce is deliberate: a crash between produce and mark
   re-produces a *duplicate* (which the storage layer collapses) rather than dropping a
   *unique* event — the failure mode a bare `SET NX` *before* producing would cause, and
   which ClickHouse could never recover. The TTL is sized to the realistic redelivery
   window (a minute or two), *not* hours; the storage layer covers anything later.
   Duplicates share `station_id`, so they land on one partition and one worker —
   `EXISTS`/`Mark` never race.
2. **Storage (authoritative):** the landing table is `ReplacingMergeTree(ingested_at)`
   ordered by `(station_id, connector_id, event_type, timestamp, event_id)`. It collapses
   rows sharing that **full sort key** — identical for a genuine re-send of one event —
   during background merges; reads needing exactness before a merge use `FINAL` /
   `uniqExact(event_id)`. **This is the dedup authority; Redis is only load-shedding in
   front of it.**

A **Bloom / Cuckoo filter** was considered for the hot path (tiny memory at huge
cardinality) and rejected as the *primary* mechanism: its false positives would drop a
*unique* event, which violates at-least-once. It is viable only as a pre-filter in
front of an exact check, which is not worth the complexity here.

### Late & out-of-order events

Windows are computed on **event-time** (`timestamp`), while `ingested_at` (processing
time) is kept for lag metrics. An event arriving after its window has been published is
not lost: it lands in the same table, and the reports re-resolve it at query time via
`FINAL` / `uniqExact` reads once late rows merge. (The single streaming rollup, revenue,
is the one exception; its exact counterpart is the A4 `FINAL` query.)
Pathologically late events (beyond a grace bound) are still queryable but flagged, so a
report can choose to include or exclude them. The simulator deliberately injects a
configurable fraction of out-of-order and duplicate events so this path is exercised,
not assumed.

### Validation & dead-letter

Events are validated for schema (required fields, types, ranges) and **referentially**
against the in-memory station set from Postgres. Failures are not dropped silently —
they are published to `charging-events-dlq` as `{raw_payload, error, ingested_at}` and
landed in `ClickHouse.dead_letter`, so "what got rejected and why" is one SQL query.

---

## 6. The energy double-count trap (correctness)

`energy_kwh` in a `METER_UPDATE` is the session's **cumulative** meter register, not the
increment since the last reading. Naively `SUM(energy_kwh)` over raw meter rows counts
the running total once per reading and over-states energy by roughly the number of
readings per session — often 10×–50×. The analytics layer therefore computes **per-session
deltas** (`max(energy) − min(energy)` per session, or the increment between consecutive
readings via window functions, or the total carried on `SESSION_STOP`) and only then
aggregates to the hour/day/month. This is called out explicitly because it is the single
easiest way to ship plausible-looking but wrong numbers.

---

## 7. Language choice — Go (+ Python)

- **Go** for the simulator and processor: true parallelism (no GIL) is what makes a
  100k/sec hot path realistic; cheap goroutines fit the per-station/per-connector
  concurrency; a single static binary keeps the containers tiny and `compose up` fast;
  and `pprof` + built-in benchmarks directly serve the Phase-4 performance work. It is
  also a natural neighbour to the team's JVM stack.
- **Python** for the analytics/reporting layer (Phase 3): the right tool for the A1–A6
  notebook and charts, against ClickHouse over its HTTP interface.

The advert is Java-first; Go is the closest idiomatic fit for this latency/throughput
profile and is used here for the data-plane, with Python for analysis. A production
build on the team's JVM stack would port the same two-consumer-group design directly.

---

## 8. Path to production

- **Schema registry + a typed contract** (Avro/Protobuf) on the topics instead of
  free-form JSON, with compatibility enforcement.
- **dbt (dbt-clickhouse)** for the analytical models: A1–A6 and downstream marts as
  versioned, tested dbt models on top of the raw landing table, with the real-time
  rollups staying as materialized views. (The advert lists dbt as preferred; this is
  where it fits — batch transformation on top of the stream, not in the hot path.)
- **Kubernetes** for orchestration: the compose services map to Deployments/
  StatefulSets; the processor and simulator scale horizontally by adding consumers up
  to the partition count. (Compose is the deliverable here per the brief; K8s is the
  production target, not a local requirement.)
- **Exactly-once** end-to-end via Kafka transactions / idempotent producers where the
  business case justifies the throughput cost (today: at-least-once + idempotent
  consumers, which is the pragmatic default).
- **Tiered retention**: hot recent data in ClickHouse, older partitions to object
  storage (S3-backed MergeTree) behind the TTL.
- **Real observability SLOs**: alert on ingestion lag, consumer-group lag, dead-letter
  rate, and freshness, not just dashboards.

---

## 9. Limitations & honest trade-offs

- At-least-once, not exactly-once: chosen deliberately; dedup makes it effectively
  once for analytics, and the real-time path is idempotent.
- ReplacingMergeTree dedup is eventual (on merge); reads needing exactness pay the
  `FINAL` cost. Acceptable for reporting; called out rather than hidden.
- Postgres referential validation uses a snapshot loaded at processor start; new
  stations added mid-run would need a refresh (trivial to add; out of scope here).
- The simulator approximates charging physics (a simple taper curve, fixed nominal
  voltages); it is realistic enough to make the analytics meaningful, not a battery
  model.

---

## 10. Analytics layer (Phase 3)

The six analytical questions are answered by the queries in `analytics/queries/`
(A1–A6), run at query time against `events_raw`, plus a Python notebook
(`analytics/report.ipynb`) that executes them and writes `analytics/output/A1..A6.csv`.

**Only revenue is a streaming aggregate.** `deploy/clickhouse/init/02_aggregates.sql`
defines exactly one materialized view — `revenue_hourly` (SummingMergeTree) — as a *fast,
slightly approximate* dashboard rollup. `cost_eur` is a per-`SESSION_STOP` scalar, so
summing one row per session inside a single insert block is correct. It is only
*approximate* because the MV fires before the ReplacingMergeTree dedup, so a duplicate
that escapes the Redis window is counted twice; the **authoritative** revenue is the A4
query, which reads `events_raw FINAL`. A dedup-safe pre-aggregate would be a *refreshable*
MV that periodically recomputes from `FINAL`.

**A1, A2, A3, A5, A6 are query-time exact analytics, not MVs — deliberately.** Each needs
state spanning *many* insert blocks, which a streaming MV (block-local) cannot hold:

- energy (A1, A3) is a per-session **delta** of the cumulative meter register — `max−min`
  or the increment between consecutive readings across a whole session, whose readings
  arrive over many blocks;
- uptime (A2) reconstructs each connector's **status timeline** (segment durations between
  STATUS_CHANGE events), again cross-block, and carries forward the state active before the
  window opens;
- fault geography (A5) and power anomalies (A6) dedup by `event_id` / compute fleet-wide
  statistics over the full history.

Expressing these exactly means reading `events_raw` (with `FINAL` / `uniqExact` where a
pre-merge duplicate would otherwise show) at query time — which ClickHouse does quickly.

**Event-time, not wall-clock.** With `time_acceleration > 1` the simulated event clock runs
ahead of wall time, so the time-windowed queries (A1 = 7 days, A4/A5 = 30 days) anchor the
window to `max(timestamp)` in the data, not `now()`, which would otherwise clip it.

**The energy double-count trap** (§6) is the correctness spine of this layer: no query uses
`SUM(energy_kwh)`.

---

## 11. Performance — measured scale test (Phase 4)

The harness (`scripts/scale_test.sh`) drives the simulator through four presets by swapping
`CONFIG_PATH` (recreating `registry-seed` → `simulator` → `processor` per preset so the
processor's in-memory registry matches the new roster), then records produced vs clean
**throughput**, end-to-end **transport lag** percentiles, **authoritative Redpanda
consumer-group lag** (not the processor's best-effort gauge), and A1/A4 **query latency** to
`benchmarks/results.csv`. It **preflights** every dependency (Prometheus, ClickHouse, Redis,
Redpanda, and the four preset files) and hard-fails rather than emit plausible-looking numbers
off a broken stack, and it **resets to a clean slate** (`docker compose down -v && up -d`)
before the first preset so the 1k row measures steady state, not a drained backlog.
`produced_eps` is the Redpanda raw-topic **offset delta over the measure window** — events the
broker actually *accepted* — not the simulator's async *enqueue* counter
(`simulator_events_produced_total`, still logged as a cross-check next to
`simulator_produce_errors_total`). Measured on a single laptop (macOS + Docker Desktop;
Redpanda in `dev-container` mode, `--smp=2 --memory=2G`), 40s warm-up / 80s measure:

| preset | target ev/s | produced_eps | clean_eps | realtime_lag | analytics_lag | a1_ms | a4_ms | redis_ms |
|-------:|------------:|-------------:|----------:|-------------:|--------------:|------:|------:|---------:|
| 1k   | 1,000   | 1,020   | 1,000 | 337        | 497        | 145 | 132 | 97 |
| 10k  | 10,000  | 10,196  | 1,862 | 120,745    | 1,065,213  | 162 | 158 | 83 |
| 50k  | 50,000  | 51,160  | 1,912 | 5,015,689  | 7,116,807  | 202 | 150 | 92 |
| 100k | 100,000 | 103,472 | 1,827 | 16,740,957 | 20,070,705 | 265 | 201 | 94 |

**The clean 1k baseline.** From a clean slate the 1k row is honest steady state: `clean_eps`
(1,000) tracks `produced_eps` (1,020) one-for-one, `analytics_lag` sits at ~500 (not the
hundreds-of-thousands a backlog-drain used to show), and `lag_p50` is **0.68 s**. That is the
"it keeps up" reference the higher presets are measured against. Every preset above 1k then
accumulates its own backlog inside the window **by design** — that is the honest saturation
ceiling, surfaced rather than hidden.

**The bottleneck.** `produced_eps` matches the enqueue counter to within noise at every level —
even 100k (103,472 broker-accepted vs 103,466 enqueued) — so the broker genuinely *accepts* the
firehose all the way up. But **`clean_eps` — the analytics path's throughput — pins at
~1.8–1.9k/s regardless of input**, so `analytics_lag` (consumer-group backlog) grows without
bound (~0.5k → 20M). This is **not** a CPU limit on the transform:
`go test -bench=BenchmarkFlattenValidate` clocks the decode → validate → flatten hot path at
**~2.5 µs/op (147 MB/s, 18 allocs/op)** — ~400k ev/s on one core. The ceiling is the analytics
handler's **synchronous, one-message-per-produce** write to the clean topic: each event blocks
on a broker ack, so throughput is `workers ÷ ack-latency`, not CPU. A `pprof` CPU profile taken
while the processor drained the 100k backlog (saved to `benchmarks/profile-summary.txt`)
confirms it: the processor is **I/O / network-syscall bound**, not JSON-bound — flat time is
dominated by the network-write syscall (`Syscall6`, ~29%), which the cumulative view attributes
to kafka-go's offset-commit/produce path and go-redis current-state writes (plus Snappy
compression of the produce batches), while `encoding/json` decode is a minor ~6–8%. The
transform is not the wall; the synchronous produce/commit I/O is.

Three measurement caveats, read honestly: the `processor_transport_lag_seconds` histogram tops
out at its 10s bucket, so under a multi-million-event backlog `lag_p95` / `lag_p99` saturate at
10.0 — the consumer-group lag column is the truer backlog signal. Even the clean 1k row reads
`lag_p95` = 10.0, because the 5-minute histogram window still catches the brief post-restart
consumer-group rebalance ramp, while its `lag_p50` = 0.68 s reflects true steady state. Next,
`redis_ms` (~83–97 ms) is dominated by `docker compose exec` process spawn, not Redis (a native
`HGETALL` is sub-millisecond, so the <100 ms point-read latency SLA holds). Finally `a1_ms` /
`a4_ms` in the committed CSV (145–265 ms) carry that **same ~90 ms `docker compose exec` spawn
tax** — the harness timed the whole `docker exec`, not the query — so the true pipeline-scale
query time is roughly measured − 90 ms (~0.05–0.18 s at ~1.4M rows; a server-side
`clickhouse-client --time` on this table reads A1 at ~90 ms and A4 at ~52 ms). That committed
CSV predates the `ch_query_ms()` fix below (it now parses `clickhouse-client --time`, the
server-side elapsed, instead of wall-clock-wrapping the exec); the accurate *at-scale* A1/A4
figures are the out-of-band numbers in **What actually holds up**.

**What actually holds up — and what doesn't.** The realtime path is honest only at 1k: there
the realtime group keeps up (`realtime_lag` 337), current state is delivered `<1 s` after the
event, and a `HGETALL` is fresh. At **≥10k it saturates identically to analytics** — the
realtime consumer group falls 120k → 5M → 16.7M events behind — so **Redis current state goes
stale by hours**. The point read stays `<100 ms`, but *the data it returns is not current*, so
the **`<1 s` freshness SLA (Task 2c) fails above ~1k**. Both paths share the same
~few-thousand-events/s ceiling — a per-event Redis CAS on the realtime side, a per-event
synchronous produce on the analytics side, and a **per-message synchronous offset commit** on
both — so the two-consumer-group split buys **independent scaling and failure isolation**, not
freshness: it does **not** keep either path fresh above ~1k on this single node.

The read side is where the two-store split does pay off — but state the honest scale of the
store. The pipeline fed the landing table to only **~1.4M rows**, *not* the tens of millions an
earlier draft implied, precisely because the analytics-produce bottleneck **starved ClickHouse**:
only ~1.4M of the produced firehose was ever flattened and landed; the rest never left Kafka (it
is the ~16–20M raw-topic consumer-group backlog above). So the CSV's `a1_ms` / `a4_ms` are
demo-scale, not scale. To answer *query durations at scale* honestly, a separate **out-of-band
load (not pipeline-fed)** replayed the real ~1.4M rows ×14 to **~19.7M rows** (each replica given
a unique `event_id` / `session_id` and its event-time shifted back 0–72 h so per-session deltas
stay valid and the copies stay inside A1's 7-day window), then re-measured server-side with
`clickhouse-client --time` (no `docker exec` overhead). At ~20M rows scanned, **A1 (7-day hourly
energy, `FINAL`) runs in ~0.8 s and A4 (30-day revenue, `FINAL`) in ~0.3 s** — columnar scans
stay sub-second an order of magnitude past what the pipeline actually fed, which is the point of
the OLAP store. (That load was measured out-of-band and then deleted; it never touched the
committed analytics CSVs.)

**Path to 100k**, in priority order: (1) **batch the analytics produce** — fetch N messages,
emit them in one `WriteMessages` call, commit the last offset — to amortise the ack latency
that currently caps analytics throughput (highest-leverage change by far); (2) **batch the
realtime path the same way** — fetch N, apply the current-state CAS in one pipelined
round-trip, commit the last offset once — since the realtime group shares the identical
per-event-write + per-message-commit ceiling, so without this the `<1 s` freshness SLA stays
broken above ~1k even after the analytics fix; (3) scale processor replicas horizontally to
the partition count (12) and raise partitions beyond that; (4) ClickHouse `async_insert` +
larger Kafka-engine blocks on the ingest side; (5) a faster JSON decoder (`jsoniter`/`sonic`)
to push the per-event floor below 2.5 µs; (6) off the laptop, run Redpanda with real resources
and RF ≥ 3 instead of the 2-core dev-container. The honest headline: this build **measures**
its ceiling and explains it, rather than claiming a 100k it never reached.
