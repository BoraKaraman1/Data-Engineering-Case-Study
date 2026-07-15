# Code-Review & Hardening Log

This pipeline was built in phases and then hardened across several independent,
adversarial code-review passes before and after submission. This log records each round,
every finding, and the response.

It deliberately includes the findings that were **declined** and the reasoning for each. A
reviewed system is defined as much by what it refuses to change as by what it fixes. Each
finding was triaged against the design; some were implemented, some were declined as
trade-offs.

Nothing here is decorative. Every "fixed" below is backed by a test, a live-stack check,
or a measured number, never a hand-written one.

---

## Verification methodology

A finding is not "fixed" until something objective proves it:

- **Go** — `gofmt -l`, `go vet ./...`, `go build ./...`, and `go test -race ./...` on both
  modules (`simulator`, `processor`). New behaviour ships with a new test (e.g. the
  registry atomic-swap test, the delay-queue ordering test, the `stopSession` table test).
- **Live stack** — `docker compose up`, then inject malformed events and confirm they land
  in the dead-letter queue with the right reason; confirm `count()` vs `uniqExact(event_id)`
  track; restart the processor under load and confirm no loss.
- **Scale** — `scripts/scale_test.sh` regenerates `benchmarks/results.csv` from a clean
  slate (`down -v && up`). **Numbers are always measured, never edited by hand.** When a
  measurement finding lands, the scale test is re-run and the results file is regenerated
  wholesale.
- **Airflow DAG** — `py_compile` + `tabnanny` + a real `DagBag` import inside the built
  Airflow image.

---

## Review rounds at a glance

| Round | Source | Findings | Fixed | Declined (with rationale) |
|------:|--------|:--------:|:-----:|:-------------------------:|
| Build | commits `7916a7a … 988e95e` | — | phases 1–4 + perf batching | — |
| R1 | CI & Airflow review (`a007089`) | 5 | 5 | 0 |
| R2 | Pipeline code review — F1–F10 (`4481073`) | 10 | 10 | 0 |
| R3 | Simulator & Airflow correctness (external review) | 3 | 3 | 0 |
| R4 | Pre-submission review (external review) | 9 + `is_late` eval | 7 | 3 |
| R5 | Post-re-measurement review (external review) | 4 | 4 | 0 |
| R6 | External review (working tree) | 2 | 1 | 1 |
| R7 | Post-submission anti-pattern audit (3 parallel reviewers) | 7 | 3 | 4 |

**40 review findings across seven rounds, plus one feature-request evaluation. 33
implemented, 8 deliberately declined** and documented below. Fixes from later reviews caught
gaps in earlier ones: F3 was refined by R3's `stopSession` fix; R4's freshness claim (2.1)
exposed a measurement problem; R4's store-write-lag changes left artifacts stale, caught in
R5; R3's PSI event-time anchor was extended to `reconcile_revenue` in R6; R7 caught a latent
race the R4 5.1 registry refresh had made possible. The iteration shows the cumulative
effect of each round.

---

## R7 — Post-submission anti-pattern audit (most recent)

Three independent reviewers swept the codebase in parallel — one each over the processor,
the simulator, and the SQL/DAG/harness layer — hunting a specific defect class: duplicated
logic, redundant data fetches, redundant passes, missing projections, and copy-paste
compounding. Seven material findings: three fixed, four declined. Two categories also came
back clean across all three sweeps: no `SELECT *` or over-fetching anywhere (every scan
projects named columns), and no per-item calls where a batch API exists.

### Fixed

**Validation fetched the same station twice — and a comment claimed it was safe (Medium).**
`Validate` called `reg.Station()` for the connector count and then `reg.StationMeta()` for
the referential cross-check: two independent `snap.Load()`s for one station. A concurrent
registry refresh (R4 5.1) could swap the snapshot between them, so a station removed
mid-reseed would be judged against a zero-value row and dead-lettered as
`operator_mismatch` instead of `unknown_station` — while the comment asserted the second
lookup "cannot miss", an invariant two separate atomic loads do not provide. *Fix:* a
single `StationMeta` read serves both checks (`numConnectors` was already in the row); the
now-unused `Station` accessor was removed and its tests migrated to `StationMeta`.
Verified: `gofmt`/`go vet` clean, `go test -race` green on both modules.

**Event timestamp parsed up to three times per event (Medium, hot path).** `Validate`
parsed the RFC3339 timestamp and discarded the result; both consumer paths then re-parsed
the identical string — the realtime copy even carried a `// stay safe` guard for a parse
that could no longer fail. At target load that is thousands of redundant parses per second
across the two groups. *Fix:* `Validate` now returns the parsed `time.Time`; both handlers
consume it, the dead re-parse branch is gone, and the accept-path unit test pins the
returned value.

**Wire-struct tag drift between the mirrored schemas (Low).** R4 4.1 accepted the
simulator/processor schema mirroring as a deliberate contract boundary, guarded by a
field-name test — and the one thing that guard cannot see had drifted:
`is_peak_priced,omitempty` in the simulator vs no `omitempty` in the processor.
Behaviourally inert today (the processor only ever decodes this struct; the DLQ writes raw
bytes), but drift on a documented boundary should be zero. *Fix:* tags aligned on the
decode side — a no-op by construction, confirmed by build + tests.

### Declined (documented, not implemented)

**A2 scans `events_raw FINAL` twice.** The uptime query reads STATUS_CHANGE rows through
two UNION ALL branches (in-window segments vs carried-forward pre-window state), paying
the expensive `FINAL` twice. A single scan bounded at `win_end` with
`greatest(ts, win_start)` clamping yields the same segments in one pass. Declined: the
query is measured correct, the two-branch form states the carried-forward intent
explicitly, and rewriting it needs live re-verification for a query that is not a
bottleneck. The single-scan form is the named optimisation if A2 ever grows slow.

**Shared `events` package for the mirrored wire structs.** Re-raised by this audit; the
R4 4.1 decision stands (a schema *contract*, not compile-time coupling), now with the tag
alignment above and the schema registry still the production answer.

**The `max(timestamp)` window anchor exists in six places** (four A-queries, two DAG
helpers), each with its own copy of the rationale comment. A shared `ev.event_anchor` view
would single-source it, but the A-queries are deliberately standalone files (the scale
harness executes them verbatim) and all six sites are pinned by tests and CI. Deferred,
with the view named as the fix.

**Simulator-internal duplication.** Connector-status literals appear ~16 times across the
`stepGroup`/model split of the status state machine, `stopSession` mirrors `meterTick`'s
integration step, and `heartbeat` inlines `baseEvent`. All cold-path or already
correctness-tested (R3's `stopSession` table test, the pricing tests); consolidating is
real cleanup but pure churn risk in support code.

---

## R6 — External review

An independent pass over the working tree identified two issues the R5 round had not covered,
both in the batch / consumer layer: one Medium correctness bug and one Medium resilience
trade-off. One was fixed, one documented as deliberate. (Two High findings from the same
review, the `is_peak_priced` premium bug and the shared-Redis eviction risk, were already
logged and fixed under R5.)

### Fixed

**Batch revenue reconcile ran on a wall-clock window (Medium).** `reconcile_revenue`
(Airflow t3) filters the event-time `timestamp` column but scoped it with a wall-clock
"yesterday." Under the simulator's accelerated event clock, it reconciled an empty window,
the same class of bug R3 had already fixed for the PSI gate but left unfixed here. *Fix:*
anchor to `max(timestamp)` (the previous full event-time day) via a `_reconcile_bounds`
helper, with an empty-table guard, matching the PSI gate and A1/A2/A4/A5. Verified
`py_compile` + `tabnanny` clean; the DagBag import runs in CI.

### Declined / deferred (documented, not implemented)

**Analytics redelivery forces a consumer-group rejoin on a downstream-write failure (Medium).**
On a flush error the reader is closed and recreated so kafka-go redelivers the uncommitted
batch on a new generation. Under a sustained ClickHouse or clean-topic outage, the group
can rebalance repeatedly instead of draining smoothly on recovery. This is correct
(at-least-once: never commit before a durable produce) and harmless on transient blips.
Retrying the already-in-hand batch in place (no rejoin, same ordering) is the cleaner
approach, but it changes the authoritative commit path and needs live fault-injection
testing (stop the sink, confirm `count() == uniqExact(event_id)` on recovery) before
landing. That risk is not acceptable right before submission. *Response:* documented in
ARCHITECTURE §9 with the retry-in-place upgrade path named; deferred, not rushed.

### Also — two low-severity nits noted, not changed

`processor_events_consumed_total` is counted pre-validation on the realtime path but
post-produce on analytics, so the two group label-values are not directly comparable
(cosmetic, a metrics-semantics choice). And the A-queries' `assumeNotNull(max(timestamp))`
errors on a *totally* empty table rather than returning empty — only reachable before any
ingest, which the notebook never hits. Both logged for a future pass rather than churned
pre-submission.

---

## R5 — Post-re-measurement review

Four findings surfaced after the R4 store-write-lag work landed. Two of them are regressions
from that work — the `results.csv` column rename left an artifact test and a dashboard panel
behind — plus one analytics-correctness bug and one architecture issue. All four fixed; none
declined.

### Fixed

**Stale Phase-4 artifact tests (High, self-inflicted).** R4's `results.csv` rename
(`lag_p*` → `clean_lag_*` / `rt_apply_*` / `ch_fresh_ms`) left `tests/test_phase4_artifacts.py`
asserting the old 12-column schema, so CI's Python job would fail. Updated both schema
assertions and added a requirement that the harness scrape the new
`processor_state_apply_lag_seconds` metric.

**Grafana dashboard missing the realtime store-write lag (Medium).** `ops-pipeline.json`
plotted only the clean-topic write lag. Added a produce→Redis-apply panel
(`processor_state_apply_lag_seconds`, p50/p95/p99) and retitled the existing one to make
clear it is the clean-topic write.

**`is_peak_priced` set regardless of the premium (High).** `stopSession` flagged
`is_peak_priced=1` for any stop in a hardcoded `[17,21)` window, independent of the tariff's
`PeakMult`. So `standard-v1`/`fleet-v1` (mult 1.00) were flagged peak while billed at base,
and `off-peak-v1` (0.80) was flagged peak while *discounted* — making A4's "peak-hour revenue
contribution %" (a scored deliverable) wrong-by-construction, and the window matched neither
the spec (07–09, 17–20) nor the config. *Fix:* gate the flag on an actual premium
(`PeakMult > 1.0`) and source the window from `cfg.Simulator.PeakWindows` (both peaks) via an
`inPeakWindow` helper, so the config is the single source of truth for "peak" across arrival
weighting, pricing, and tariff selection. Added economic-flag tests and regenerated the
analytics so A4 reflects the correction.

**One Redis, one eviction policy, two opposite workloads (High, architecture).** Write-once
dedup keys are safe to evict because ClickHouse's `ReplacingMergeTree` is the backstop.
Must-persist current-state hashes have no backstop; Redis is the store. Both shared one
1 GB `allkeys-lru` instance. At the 100k preset, the ~12M-key dedup working set can exceed
the cap, and `allkeys-lru` evicts across all keys, including live state, silently breaking
the "current status in the last 5 minutes" guarantee. *Fix:* split into two instances:
`redis` (state, `noeviction`) and `redis-dedup` (`allkeys-lru`). A dedup flood can never
evict must-persist state, and each workload gets the memory lifecycle it needs.

### Also — a mislabeled artifact

**`benchmarks/profile-summary.txt` read as current but was pre-batching.** Its symbols
(`runGroup.func1`, `realtimeHandler.handle`, `commitLoopImmediate`) are from the old
per-message code, and its "20–27M-event backlog" contradicts the re-measured `results.csv`
(bounded ~10k). Rather than regenerate it — that backlog no longer exists to capture — it was
relabeled as the explicit **pre-batching baseline**: the diagnosis that motivated the H1/H2
micro-batching, with a pointer to `results.csv` as the "after."

---

## R4 — Pre-submission review

Nine findings from an independent review of the working tree. Each was triaged against the
design before any code changed; two were declined as deliberate trade-offs.

### Fixed

**2.2 — Real-time consumer starved at low traffic (High).**
Both consumer groups shared one `kafka-go` reader config with `MinBytes: 10e3` and no
`MaxWait`, so `MaxWait` defaulted to **10 s**. At low traffic the realtime group's first
fetch could block up to 10 s, violating the `<1 s` current-state SLO. *Fix:* split the
reader config — realtime `MinBytes: 1`, `MaxWait ≈ 50 ms`; analytics keeps the throughput
tuning but with an explicit 1 s `MaxWait`. Config-driven with defaults.

**2.1 — Ingestion lag measured to the wrong point (High).** The load test measured
produce→clean-topic lag (a 10 s-capped histogram whose p95/p99 saturated), not true
produce→store-write lag. *Fix:* add a `produced_at` column carried end-to-end from the raw
Kafka produce time, add a realtime produce→Redis-apply histogram, widen buckets to 120 s,
and add a produce→ClickHouse-queryable freshness probe. The scale test was then re-run and
`results.csv` regenerated with real numbers. (See the flagship section below for detailed
results.)

**2.3 — Dedup wording imprecise (Medium).** Docs called `event_id` "the dedup key," which
reads as if `ReplacingMergeTree` dedups on `event_id` alone; it collapses the full ORDER BY
tuple. *Fix (docs):* `event_id` is the *logical* identity; storage dedup is *physical* (the
whole sort key). They coincide because an at-least-once duplicate is a byte-identical
re-send; exact analytics stay `event_id`-exact via `uniqExact`/`argMax`/`FINAL` regardless.

**3.2 — Divide-by-zero in A2/A4 (Low).** Uptime and peak-share ratios could produce
`nan`/`inf` on zero-duration or zero-revenue groups. *Fix:* wrap the denominators in
`nullIf(…, 0)` so a degenerate group yields `NULL` (honest "undefined"), not `nan`.

**3.3 — `event_id` only checked non-empty (Low).** A malformed id becomes a Redis key and a
ClickHouse row. *Fix:* a dependency-free UUID-shape guard (`bad_event_id` rule → DLQ), with
test cases.

**5.1 — Registry loaded once at startup (Medium).** New stations / tariff changes required a
restart. *Fix:* a config-gated periodic refresh via an `atomic.Pointer` snapshot swap —
reads stay lock-free (consistent with the "never on the hot path" design), keeps the old
snapshot on a failed/empty reload, with a race-tested unit test.

**5.2 — One goroutine + timer per out-of-order event (Medium).** At 100k/s × 1% delayed,
thousands of goroutines/timers could pile up. *Fix:* a single bounded min-heap delay queue
drained by one worker — behaviour-preserving (identical delay draw, fire time, metric,
cancel-drop), just bounded.

### Declined (deliberate trade-offs — documented, not implemented)

**3.1 — Real-time path commits offsets even if the Redis write fails.** This is the
documented best-effort state projection: it validates but does not dead-letter, and commits
after one bounded retry because current state self-heals from the next event (≤30 s meter
cadence) plus the key TTL. Building a state DLQ / replay topic would contradict the design
and add machinery this case does not need. *Response:* documented as deliberate; the
log-compacted state-rebuild topic is named as the production upgrade path.

**4.1 — Event schema mirrored between simulator and processor.** Intentional: they are
independently deployed services that should share a schema *contract*, not a compile-time
code dependency. Drift is guarded by `processor/transform/flatten_test.go` (field-names
pinned to the ClickHouse columns); the production answer is a schema registry (already a
named future item). Refactoring two Go modules into a shared type right before submission is
exactly the wrong risk. *Response:* documented as a deliberate boundary.

---

## R3 — Simulator & Airflow correctness (external review)

**`stopSession` final-interval energy — over-counting (follow-up to F3).** F3 had advanced
energy across the final interval to `now` unconditionally. This review caught that it
overshot: a duration-triggered stop fires on the tick *after* `EndsAt`, and a full battery
should add nothing. *Fix:* cap the advance at `min(now, EndsAt)` and skip when `SoC == 100`;
rewritten as a three-case table test (advances / full-battery-noop / caps-at-EndsAt).

**Airflow PSI windows on wall-clock instead of event-time.** The data-quality gate compared
distributions using `now()`-relative windows, but the simulator runs at `time_acceleration
> 1`, so the event clock runs ahead. *Fix:* anchor the PSI windows to `max(timestamp)`
(matching A1/A2/A4/A5). The DLQ-rate check deliberately uses its own wall-clock window on
`ingested_at` (processing-time the processor stamps in real time), a two-clock split
commented in the DAG.

**Airflow PSI `power_kw` not read with `FINAL`.** The drift metric could double-count a
redelivered `METER_UPDATE`. *Fix:* read from `ev.events_raw FINAL`. (A no-op on clean data;
correct for the `ReplacingMergeTree` contract if at-least-once ever redelivers.)

---

## R2 — Pipeline code review (F1–F10, commit `4481073`)

| # | Finding | Fix |
|---|---------|-----|
| F1 | Scale-test preflight ran service probes before the stack was up | Split `preflight_files` (before `down -v`) from `preflight_services` (after `wait_healthy`) |
| F2 | A6 power-anomaly query didn't dedup | `FROM ev.events_raw FINAL` |
| F3 | `SESSION_STOP` dropped the final interval's energy/cost | Advance energy across the last interval (later refined in R3 to cap + skip-full) |
| F4 | No referential check on operator/city/country/coords | Cross-check each event against the seeded station; mismatches → DLQ (`operator_mismatch`, `city_mismatch`, `country_mismatch`, `geo_mismatch`) |
| F5 | Docs over-claimed a late-event watermark/grace bound | Corrected: no watermark; late/out-of-order resolved entirely at query time |
| F6 | Tariff catalog duplicated (hardcoded Postgres INSERT vs Go) | Single-sourced in Go; seeded transactionally; DDL-only in SQL |
| F7 | No config validation | Validate weights / bounds / peak windows / rates in `LoadConfig` |
| F8 | Both consumer groups double-counted validation errors | Realtime emits a coarse `realtime_invalid_skipped` counter; authoritative per-rule `validation_errors` only on the analytics path |
| F9 | A leftover topic with the wrong partition count was silently reused | `redpanda-init` asserts the real partition counts (12/12/3) and fails fast |
| F10 | `HEARTBEAT` wrote a connector-shaped `station:{id}:0` key | Routed to its own `station_liveness:{id}` namespace |

---

## R1 — CI & Airflow review (commit `a007089`)

1. **PSI computed over the raw window risked OOM** → moved bucketing ClickHouse-side
   (`quantilesTDigest` edges + `arraySum(arrayMap(…))` bucket counts); PSI-from-counts in
   Python.
2. **`OPTIMIZE … FINAL` on the whole table** → target only the closed (previous-day) monthly
   partition.
3. **Energy-delta query didn't filter event types** → restrict to the meter-bearing types.
4. **CI didn't validate the DAG** → a CI job installs Airflow + `clickhouse-connect` and runs
   a `DagBag` import.
5. **`_PIP_ADDITIONAL_REQUIREMENTS` at container start** (fragile, needs network) → baked
   `clickhouse-connect` into a custom Airflow image (`deploy/airflow/Dockerfile`).

*(The CI itself, the README results surfacing, and the opt-in Airflow batch layer were added
in `b80f26c`; the CI is what then caught several of the issues above.)*

---

## Feature request evaluated and declined — `is_late`

The review floated an `is_late` flag on each event. **Recommendation: do not implement**, on
four grounds: (1) it is **analytically redundant** — lateness is already answerable at query
time by comparing `ingested_at`/processing order to event-time, so the flag stores a derived
value; (2) it **hard-codes a non-retunable grace bound** into the hot path and the schema,
where the current design resolves late/out-of-order *entirely at read time*; (3) it adds
hot-path and storage cost for every event to answer a question only a minority of analyses
ask; (4) under `time_acceleration > 1` a wall-clock grace bound produces **sim-clock
artifacts**. Documented rather than built.

---

## Why this matters

The hard parts of this case are correctness under real distributed-systems conditions:
at-least-once dedup, late/out-of-order arrival, the two-store split, and the cumulative-energy
trap. Those were solved in the build phases and then stress-tested by the rounds above,
including catching a regression in a prior fix (F3 → R3) and a measurement that pointed at
the wrong instrument (R4 2.1). The declined findings show the same rigor applied in reverse.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the design rationale each of these findings was
checked against, and `benchmarks/results.csv` for the measured scale curve referenced in R4.
