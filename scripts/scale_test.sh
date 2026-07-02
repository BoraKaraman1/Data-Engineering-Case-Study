#!/usr/bin/env bash
#
# Phase 4 scale test. Drives the simulator at increasing target rates and, at each
# level, records production vs processed throughput, THREE wall-clock store-write lags
# (clean-topic write, Redis current-state apply, ClickHouse queryable freshness), Kafka
# consumer-group lag for BOTH processor groups, and A1/A4 query latency + a Redis
# point-read latency. Results append to benchmarks/results.csv.
#
# Throughput honesty:
#   - produced_eps is the Redpanda RAW-topic offset delta over the measure window
#     (broker-ACCEPTED events), NOT the simulator's async ENQUEUE counter. The
#     enqueue counter simulator_events_produced_total is still read as a cross-check
#     and simulator_produce_errors_total is surfaced as a red flag.
#   - Authoritative consumer-group lag comes from `rpk group describe` (the processor's
#     own best-effort lag gauge is NOT used here).
#
# Correctness of the baseline: the run starts from a CLEAN SLATE (down -v && up -d) so
# the 1k row measures steady state rather than draining a pre-existing backlog. Presets
# after 1k still accumulate their own backlog inside the window -- that is the honest
# saturation ceiling, not hidden.
#
# Drive the rate by swapping the simulator's config via CONFIG_PATH; the simulator +
# registry-seed both honour it, so the roster matches the rate.
#
# Usage:  scripts/scale_test.sh [preset ...]      (default: 1k 10k 50k 100k)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
COMPOSE="docker compose"
PROM="http://localhost:9090/api/v1/query"
OUT="benchmarks/results.csv"
WARMUP="${WARMUP:-60}"
MEASURE="${MEASURE:-120}"
PRESETS=("${@:-}")
[ -z "${PRESETS[*]}" ] && PRESETS=(1k 10k 50k 100k)

mkdir -p benchmarks

now_ms() { python3 -c 'import time;print(int(time.time()*1000))'; }

prom() {  # instant value for a PromQL query; prints a finite number (0 for empty/NaN).
          # ABORTS (returns non-zero) if Prometheus is unreachable, so a REQUIRED metric
          # can never be silently substituted with a fake 0.
    local body v
    if ! body="$(curl -sf --data-urlencode "query=$1" "$PROM" 2>/dev/null)"; then
        echo "ERROR: Prometheus unreachable for query: $1" >&2
        return 1
    fi
    v="$(printf '%s' "$body" | python3 -c '
import sys, json
r = json.load(sys.stdin).get("data", {}).get("result", [])
v = float(r[0]["value"][1]) if r else 0.0
if v != v or v in (float("inf"), float("-inf")):  # NaN / inf -> no data
    v = 0.0
print(v)
')" || { echo "ERROR: could not parse Prometheus response for: $1" >&2; return 1; }
    printf '%s\n' "$v"
}

group_lag() {  # total lag for a consumer group, straight from Redpanda. A reported 0 is
               # legitimate (caught up / not yet consuming); a BROKEN rpk is not masked to
               # 0 silently -- it warns loudly so the run is not trusted blindly.
    local out v
      out="$($COMPOSE exec -T redpanda rpk group describe "$1" --brokers redpanda:9092)" || {
          echo "ERROR: rpk group describe $1 failed" >&2
          return 1
      }

      v="$(printf '%s' "$out" | awk '/TOTAL-LAG/{print $2}')"
      if [ -z "$v" ]; then
          echo "ERROR: rpk group describe $1 did not report TOTAL-LAG" >&2
          return 1
      fi

      printf '%s\n' "$v"
  }

ch_query_ms() {  # SERVER-SIDE query time in ms for a .sql file against ClickHouse (floored to
                 # 1ms). Uses `clickhouse-client --time`, which prints the server elapsed
                 # seconds to stderr, so the recorded number is the QUERY duration -- NOT the
                 # ~90ms `docker compose exec` process spawn that a wall-clock wrapper folds in
                 # (the same exec tax already discounted for redis_ms). A query FAILURE is a
                 # hard error (a1_ms/a4_ms are required) -- not a fast fake time.
    local sql secs ms
    sql="$(cat "$1")"
    # --time writes the server elapsed seconds to stderr; the result rows go to /dev/null.
    if ! secs="$($COMPOSE exec -T clickhouse clickhouse-client -u chargesquare --password chargesquare --time -q "$sql" 2>&1 >/dev/null)"; then
        echo "ERROR: ClickHouse query failed: $1" >&2
        return 1
    fi
    ms="$(printf '%s' "$secs" | python3 -c '
import sys, re
t = re.findall(r"[0-9]+\.?[0-9]*", sys.stdin.read())  # last numeric token = elapsed seconds
if not t:
    sys.exit(1)
print(max(1, int(float(t[-1]) * 1000)))
')" || { echo "ERROR: could not parse clickhouse-client --time output for: $1" >&2; return 1; }
    echo "$ms"
}

ch_freshness_ms() {  # produce->ClickHouse-queryable store-write lag: now() - the newest queryable
                     # produced_at (the raw Kafka produce wall-clock carried into events_raw), in ms.
                     # An instantaneous gauge sampled 5x and medianed: at saturation the newest
                     # queryable row is old, so freshness grows and honestly tracks the backlog.
                     # Empty table -> 0. A query FAILURE is a hard error (the metric is required).
    local i v samples
    samples=""
    for i in 1 2 3 4 5; do
        if ! v="$($COMPOSE exec -T clickhouse clickhouse-client -u chargesquare --password chargesquare -q \
            "SELECT toUInt64(greatest(ifNull(dateDiff('millisecond', max(produced_at), now64(3)), 0), 0)) FROM ev.events_raw" 2>/dev/null | tr -d '\r\n')"; then
            echo "ERROR: ClickHouse freshness query failed" >&2
            return 1
        fi
        samples="$samples $v"
        sleep 1
    done
    printf '%s' "$samples" | python3 -c '
import sys
xs = sorted(int(x) for x in sys.stdin.read().split() if x.strip().isdigit())
print(xs[len(xs)//2] if xs else 0)
' || { echo "ERROR: could not median freshness samples" >&2; return 1; }
}

redis_read_ms() {  # wall-clock ms for a current-state point read (floored to 1ms). Optional
                   # sample: if no state key exists yet, report the 1ms floor rather than abort.
    local key t0 t1 dt
    key="$($COMPOSE exec -T redis redis-cli --scan --pattern 'station:*:[1-9]*' 2>/dev/null | head -1 | tr -d '\r\n')" || true
    [ -n "${key:-}" ] || { echo 1; return 0; }
    t0="$(now_ms)"
    if ! $COMPOSE exec -T redis redis-cli HGETALL "$key" >/dev/null 2>&1; then
        echo "WARN: redis HGETALL failed; recording redis_ms floor" >&2
        echo 1; return 0
    fi
    t1="$(now_ms)"
    dt=$((t1 - t0))
    [ "$dt" -ge 1 ] || dt=1
    echo "$dt"
}

wait_healthy() {  # block until the stateful deps report healthy (bail after a bound)
    local deadline=$((SECONDS + 300)) svc st ok
    echo "    waiting for redpanda/clickhouse/postgres/redis to be healthy..."
    while :; do
        ok=1
        for svc in redpanda clickhouse postgres redis; do
            st="$($COMPOSE ps "$svc" --format '{{.Health}}' 2>/dev/null || true)"
            [ "$st" = "healthy" ] || ok=0
        done
        [ "$ok" = 1 ] && break
        [ "$SECONDS" -lt "$deadline" ] || { echo "ERROR: dependencies not healthy in time" >&2; exit 1; }
        sleep 3
    done
    echo "    dependencies healthy"
}

preflight_files() {  # fail fast (exit 1) if a scale preset is missing. Runs BEFORE the stack
                     # is brought up -- it only reads local config, so it needs no services.
    echo ">>> preflight: scale presets"
    local preset
    for preset in 1k 10k 50k 100k; do
        [ -f "config/scale-${preset}.yaml" ] \
            || { echo "ERROR: missing config/scale-${preset}.yaml" >&2; exit 1; }
    done
    echo "    preflight files OK"
}

preflight_services() {  # fail fast (exit 1) if a dependency is unreachable. Runs AFTER the
                        # stack is healthy -- these probes need the services already running.
    echo ">>> preflight: dependencies"
    curl -sf localhost:9090/-/ready >/dev/null 2>&1 \
        || { echo "ERROR: Prometheus not ready at localhost:9090" >&2; exit 1; }
    $COMPOSE exec -T clickhouse clickhouse-client -u chargesquare --password chargesquare -q 'SELECT 1' >/dev/null 2>&1 \
        || { echo "ERROR: ClickHouse not reachable" >&2; exit 1; }
    $COMPOSE exec -T redis redis-cli ping 2>/dev/null | grep -q PONG \
        || { echo "ERROR: Redis not reachable" >&2; exit 1; }
    $COMPOSE exec -T redpanda rpk cluster info --brokers redpanda:9092 >/dev/null 2>&1 \
        || { echo "ERROR: Redpanda not reachable" >&2; exit 1; }
    echo "    preflight OK"
}

# File checks need no running services, so they gate startup; the service-reachability
# checks can only run once the stack is up (below, after wait_healthy).
preflight_files

# Clean slate so the 1k baseline measures steady state, not a drained backlog (F2).
# Uses the already-built images (no --build).
echo ">>> resetting to clean slate (docker compose down -v && up -d)..."
$COMPOSE down -v
$COMPOSE up -d
wait_healthy

preflight_services

echo "preset,target_eps,produced_eps,clean_eps,clean_lag_p50,clean_lag_p95,clean_lag_p99,rt_apply_p50,rt_apply_p95,rt_apply_p99,ch_fresh_ms,realtime_lag,analytics_lag,a1_ms,a4_ms,redis_ms" > "$OUT"

for preset in "${PRESETS[@]}"; do
    cfg="/app/config/scale-${preset}.yaml"
    target="$(python3 -c "import re,sys;print(re.sub(r'[^0-9]','',sys.argv[1]))" "$preset")000"
    echo ">>> preset ${preset} (target ~${target} ev/s) via ${cfg}"
    # Reseed the registry into Postgres for the new roster and wait for it to finish,
    # THEN recreate the simulator AND the processor. The processor loads the station
    # registry into memory once at startup, so without a restart it would dead-letter
    # every event from a station the previous (smaller) roster didn't contain.
    CONFIG_PATH="$cfg" $COMPOSE up -d --force-recreate --no-deps registry-seed
    $COMPOSE wait registry-seed
    CONFIG_PATH="$cfg" $COMPOSE up -d --force-recreate --no-deps simulator processor

    echo "    warm-up ${WARMUP}s..."; sleep "$WARMUP"

    # produced_eps = broker-ACCEPTED throughput: raw-topic offset delta over the window.
    off0="$(prom 'sum(redpanda_kafka_max_offset{redpanda_topic="charging-events-raw"})')"
    enq0="$(prom 'sum(simulator_events_produced_total)')"        # enqueue-rate cross-check
    perr0="$(prom 'sum(simulator_produce_errors_total)')"
    echo "    measuring ${MEASURE}s..."; sleep "$MEASURE"
    off1="$(prom 'sum(redpanda_kafka_max_offset{redpanda_topic="charging-events-raw"})')"
    enq1="$(prom 'sum(simulator_events_produced_total)')"
    perr1="$(prom 'sum(simulator_produce_errors_total)')"

    produced="$(python3 -c "print(max(0.0, ($off1 - $off0) / $MEASURE))")"
    enq_rate="$(python3 -c "print(round(max(0.0, ($enq1 - $enq0) / $MEASURE), 1))")"
    perr="$(python3 -c "print(int(max(0.0, $perr1 - $perr0)))")"
    echo "    produced_eps=${produced} (broker offset delta)  enqueue_rate=${enq_rate}/s (simulator_events_produced_total)"
    if [ "$perr" -gt 0 ]; then
        echo "WARN: ${perr} simulator_produce_errors_total during the ${preset} window -- broker rejected produces (load-shedding / red flag)" >&2
    fi

    clean="$(prom 'sum(rate(processor_clean_produced_total[1m]))')"
    # Store-write lag #1: produce -> durably in the clean topic (analytics path).
    clean_p50="$(prom 'histogram_quantile(0.5, sum(rate(processor_transport_lag_seconds_bucket[5m])) by (le))')"
    clean_p95="$(prom 'histogram_quantile(0.95, sum(rate(processor_transport_lag_seconds_bucket[5m])) by (le))')"
    clean_p99="$(prom 'histogram_quantile(0.99, sum(rate(processor_transport_lag_seconds_bucket[5m])) by (le))')"
    # Store-write lag #2: produce -> Redis current-state apply (realtime path SLO).
    rt_p50="$(prom 'histogram_quantile(0.5, sum(rate(processor_state_apply_lag_seconds_bucket[5m])) by (le))')"
    rt_p95="$(prom 'histogram_quantile(0.95, sum(rate(processor_state_apply_lag_seconds_bucket[5m])) by (le))')"
    rt_p99="$(prom 'histogram_quantile(0.99, sum(rate(processor_state_apply_lag_seconds_bucket[5m])) by (le))')"
    # Store-write lag #3: produce -> ClickHouse queryable (analytics store freshness).
    ch_fresh_ms="$(ch_freshness_ms)"
    rt_lag="$(group_lag realtime)"
    an_lag="$(group_lag analytics)"
    a1_ms="$(ch_query_ms analytics/queries/A1_hourly_energy.sql)"
    a4_ms="$(ch_query_ms analytics/queries/A4_revenue.sql)"
    redis_ms="$(redis_read_ms)"

    row="${preset},${target},${produced},${clean},${clean_p50},${clean_p95},${clean_p99},${rt_p50},${rt_p95},${rt_p99},${ch_fresh_ms},${rt_lag},${an_lag},${a1_ms},${a4_ms},${redis_ms}"
    echo "    $row"
    echo "$row" >> "$OUT"
done

echo "done -> $OUT"
