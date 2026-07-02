# ChargeSquare (EV Charging Data Pipeline)

[![CI](https://github.com/BoraKaraman1/Data-Engineering-Case-Study/actions/workflows/ci.yml/badge.svg)](https://github.com/BoraKaraman1/Data-Engineering-Case-Study/actions/workflows/ci.yml)

Real-time + analytics pipeline for EV charging telemetry: a Go simulator produces a
realistic charging-event firehose into Kafka; a Go processor (validate → dedup →
route) feeds a Redis current-state store and a ClickHouse analytics store;
PostgreSQL holds the station/tariff registry; Prometheus + Grafana observe the lot.

```
                                  ┌──────────────► Redis  (current state, <100ms point reads)
                                  │                 (realtime consumer group)
 simulator ──► Kafka (raw) ──► processor
   (Go)        Redpanda          (Go)  ──► Kafka (clean) ──► ClickHouse  (analytics, OLAP)
                  │               │                            (Kafka engine → ReplacingMergeTree → revenue MV)
                  │               └──► Kafka (dlq) ──────────► ClickHouse.dead_letter
                  │
 PostgreSQL ◄─────┘ station/tariff registry (OLTP source of truth; loaded once into memory)

 Prometheus scrapes simulator + processor  →  Grafana dashboards
```

Full rationale (store selection, dedup, late/out-of-order, partitioning, the energy
double-count trap, path to production) is in **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

**Start here: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — the final report tying the
whole pipeline together (design decisions, the measured scale curve, production path).

---

## Build status

This is built and verified in phases that mirror the case tasks, so each layer can
be run and checked before the next is added.

| Phase | Scope | Status |
|------|-------|--------|
| 1 | Infra (compose, all stores) + **Task 1 simulator** | **done** |
| 2 | **Task 2 processor**: validate, dedup, route → Redis + clean topic + dlq | **done** |
| 3 | **Task 3 queries** A1–A6 + revenue MV + Grafana dashboard + Python report | **done** |
| 4 | **Task 4** scale test 1k→100k ev/s + bottleneck analysis | **done** |
| 5 | **Task 5** final architecture report + production readiness | **done** |

The processor bridges **raw → clean**: it validates every event, dead-letters the bad
ones, dedups on `event_id`, projects current state into Redis (latency-first consumer
group), and feeds ClickHouse via the clean topic (throughput-first group). The registry
is loaded by a one-shot `registry-seed` service that the simulator and processor both
wait for, so the processor always sees a complete, fresh roster.

---

## Quick start

Requires Docker + Docker Compose. One command brings up the whole stack (the
simulator's `go.sum` is committed, so the image builds hermetically, no manual
step first):

```bash
docker compose up --build
```

Services:

| Service | URL | Notes |
|--------|-----|-------|
| Redpanda Console | http://localhost:8080 | inspect topics, partitions, live messages |
| ClickHouse HTTP | http://localhost:8123 | user `chargesquare` / pass `chargesquare`, db `ev` |
| Postgres | localhost:5432 | `chargesquare` / `chargesquare` |
| Redis | localhost:6379 | |
| Prometheus | http://localhost:9090 | |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Simulator metrics | http://localhost:9101/metrics | |

Kafka from your laptop (not from inside the compose network) is on `localhost:19092`.

---

## Verify Phase 1

**1. The simulator is producing.** Watch its metrics tick up:

```bash
curl -s localhost:9101/metrics | grep simulator_events_produced_total
curl -s localhost:9101/metrics | grep -E 'simulator_(active_sessions|duplicates|out_of_order)'
```

**2. Events are landing on the raw topic.** Read a few straight off Kafka:

```bash
docker compose exec redpanda rpk topic consume charging-events-raw --num 5 --brokers localhost:9092
```

You should see nested JSON: `SESSION_START`, a stream of `METER_UPDATE`s with a
*cumulative* `energy_kwh` and rising `soc_percent`, `HEARTBEAT`s, the occasional
`FAULT_ALERT`, and `SESSION_STOP` carrying `cost_eur`. Or browse them in the
Redpanda Console at http://localhost:8080.

**3. The registry seeded into Postgres:**

```bash
docker compose exec postgres psql -U chargesquare -d chargesquare \
  -c "select count(*) stations from stations; select count(*) connectors from connectors;"
```

**4. ClickHouse schema is present** (populated by the processor):

```bash
docker compose exec clickhouse clickhouse-client -u chargesquare --password chargesquare \
  -q "show tables from ev"
```

---

## Verify Phase 2 (processor)

**1. The clean topic is flat, with `ingested_at` + `status` and no nested objects:**

```bash
docker compose exec redpanda rpk topic consume charging-events-clean --num 3 --brokers localhost:9092
```

**2. ClickHouse is landing deduped rows.** `count()` tracks `uniqExact(event_id)`,
proving no duplicates survive (Redis dedup + ReplacingMergeTree):

```bash
docker compose exec clickhouse clickhouse-client -u chargesquare --password chargesquare \
  -q "select count(), uniqExact(event_id) from ev.events_raw"
```

**3. Redis current-state.** A hash per connector; `TTL` ≤ 300 is the 5-minute freshness
window (key exists ⟺ seen in the last 5 min):

```bash
docker compose exec redis redis-cli --scan --pattern 'station:*' | head -1
docker compose exec redis redis-cli HGETALL station:TR-IST-0001:1
```

**4. Dead-letter path.** Invalid events land here with a specific reason:

```bash
docker compose exec clickhouse clickhouse-client -u chargesquare --password chargesquare \
  -q "select error, count() from ev.dead_letter group by error order by 2 desc"
```

**5. Processor + both consumer groups are healthy:**

```bash
curl -s localhost:9102/metrics | grep -E 'processor_(clean_produced|dlq|duplicates_dropped)_total'
docker compose exec redpanda rpk group describe realtime analytics --brokers localhost:9092
```

---

## Run the analytics (Phase 3)

The six analytical queries live in `analytics/queries/` (A1–A6). A Jupyter notebook runs
them against the live ClickHouse (HTTP, `:8123`) and writes one CSV per query:

```bash
pip install -r analytics/requirements.txt
jupyter nbconvert --to notebook --execute --inplace analytics/report.ipynb
```

Results land in `analytics/output/A1.csv … A6.csv` (plus a `.png` chart each). The
notebook finds the repo root automatically (or set `PIPELINE_ROOT`), so it runs from the
repo root or from `analytics/`. Point it elsewhere with `CLICKHOUSE_HOST` /
`CLICKHOUSE_PORT`. Every energy figure uses per-session **deltas**, never
`SUM(energy_kwh)`. See the energy-trap note below and ARCHITECTURE §6.

![A1 hourly energy](analytics/output/A1.png)

The remaining charts:

- [A2 — station uptime/downtime and worst stations per operator](analytics/output/A2.png)
- [A3 — average charging duration and energy by vehicle brand](analytics/output/A3.png)
- [A4 — revenue by operator/city/tariff and peak-rate share](analytics/output/A4.png)
- [A5 — geographic distribution of faults (error density by city)](analytics/output/A5.png)
- [A6 — power anomalies (sessions >2σ above the fleet mean)](analytics/output/A6.png)

---

## Scale presets (for the Phase-4 load test)

Everything tunable lives in `config/simulator.yaml`. The achievable event rate is a
function of `station_count` × `time_acceleration`; `target_events_per_sec` is a
*cap* the pacer enforces, so you can hold a precise controlled input rate while
measuring the pipeline.

| Target rate | station_count | time_acceleration | target_events_per_sec |
|------------:|--------------:|------------------:|----------------------:|
| demo (realistic) | 200 | 60 | 5000 |
| ~1k/s | 1000 | 60 | 1000 |
| ~10k/s | 5000 | 120 | 10000 |
| ~50k/s | 20000 | 300 | 50000 |
| ~100k/s | 40000 | 600 | 100000 |

**Run the scale test:** `bash scripts/scale_test.sh` (override the windows with
`WARMUP=120 MEASURE=90 bash scripts/scale_test.sh`). For each preset it recreates
`registry-seed` -> `simulator` -> `processor` for the new roster, then records produced vs
clean throughput, transport-lag percentiles, authoritative Redpanda consumer-group lag,
and A1/A4 query latency to **`benchmarks/results.csv`**. The checked-in results are the final
tuned four-row curve (1k, 10k, 50k, 100k): analytics batching keeps `clean_eps` near input rate,
realtime Redis batching keeps consumer-group lag bounded, and the 100k row supports the `<1 s`
current-state freshness target on this local compose run. The measured curve, caveats, and
path to 100k / production are in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) §11.

---

## Results

Clean-topic throughput tracks the driven input rate the whole way up the curve —
998.8182033053945 / 9998.254608923313 / 50208.66893329212 / 102658.74545454545 ev/s at
the 1k / 10k / 50k / 100k presets. Realtime consumer-group lag stays bounded at every
step (351 / 710 / 2281 / 12782 events, which at each preset's rate is well under a second
of backlog), so current-state freshness holds under 1s. Full before/after batching
analysis: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) §11.

| preset | target_eps | produced_eps | clean_eps | lag_p50 | lag_p95 | lag_p99 | realtime_lag | analytics_lag | a1_ms | a4_ms | redis_ms |
|--------|-----------|--------------|-----------|---------|---------|---------|--------------|---------------|-------|-------|----------|
| 1k | 1000 | 1019.4888888888889 | 998.8182033053945 | 0.4285786376802818 | 10.0 | 10.0 | 351 | 466 | 23 | 17 | 88 |
| 10k | 10000 | 10197.511111111111 | 9998.254608923313 | 0.17504045821204253 | 10.0 | 10.0 | 710 | 1353 | 129 | 57 | 88 |
| 50k | 50000 | 51240.71111111111 | 50208.66893329212 | 0.08708749088729392 | 10.0 | 10.0 | 2281 | 2193 | 2104 | 803 | 92 |
| 100k | 100000 | 104592.45555555556 | 102658.74545454545 | 4.39809260131872 | 10.0 | 10.0 | 12782 | 7633 | 8634 | 3195 | 141 |

*`*_eps` = events/sec; `lag_p50/p95/p99` = end-to-end transport lag in seconds;
`realtime_lag`/`analytics_lag` = Kafka consumer-group backlog in events; `a1_ms`/`a4_ms`
= ClickHouse server-side query time in ms; `redis_ms` = current-state point-read latency
in ms.*

## Batch layer (optional)

`docker compose --profile airflow up` brings up Airflow at http://localhost:8081
with the on-demand `ev_analytics_daily` DAG (freshness gate, per-partition
OPTIMIZE, exact revenue reconciliation, PSI data-quality gate, TTL report). It is
not started by the default stack. See [deploy/airflow/README.md](deploy/airflow/README.md).

---

## Repo layout

```
docker-compose.yml          one-command stack
config/simulator.yaml       all simulator knobs (count, rate, peaks, faults, operators)
config/processor.yaml       processor knobs (topics, groups, dedup/state TTL, workers)
deploy/
  clickhouse/init/          schema: Kafka engine → ReplacingMergeTree, dead-letter
  postgres/init/            station/connector/tariff registry
  prometheus/               scrape config
  grafana/provisioning/     datasource + dashboard provider
simulator/                  Go: event generator → raw topic (Task 1)
processor/                  Go: validate → dedup → Redis + clean topic + dlq (Task 2)
docs/ARCHITECTURE.md        design decisions + justifications (the report)
analytics/                  A1–A6 queries + Python report (Phase 3)
```

---

## Notes & limitations

- `energy_kwh` is **cumulative within a session** by design (it models a meter
  register). Summing raw `METER_UPDATE` rows therefore over-counts energy badly;
  the analytics queries (Phase 3) compute per-session deltas. See ARCHITECTURE.
- The simulator counts produced events on Kafka *enqueue* (async writer); failures
  are tracked separately in `simulator_produce_errors_total`.
