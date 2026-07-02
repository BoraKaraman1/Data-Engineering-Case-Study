import csv
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read_text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def compact(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def require_match(pattern: str, text: str, message: str) -> re.Match:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
    if not match:
        raise AssertionError(message)
    return match


class Phase4ArtifactsTest(unittest.TestCase):
    def test_scale_presets_exist_and_encode_expected_rates(self):
        expected = {
            "1k": {"target_events_per_sec": 1000, "station_count": 1000, "time_acceleration": 60.0},
            "10k": {"target_events_per_sec": 10000, "station_count": 5000, "time_acceleration": 120.0},
            "50k": {"target_events_per_sec": 50000, "station_count": 20000, "time_acceleration": 300.0},
            "100k": {"target_events_per_sec": 100000, "station_count": 40000, "time_acceleration": 600.0},
        }

        for preset, values in expected.items():
            path = ROOT / f"config/scale-{preset}.yaml"
            self.assertTrue(path.exists(), f"missing scale preset {path}")
            text = path.read_text(encoding="utf-8")
            for key, expected_value in values.items():
                match = require_match(
                    rf"^\s*{key}\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*$",
                    text,
                    f"{path} must define {key}",
                )
                actual = float(match.group(1)) if "." in match.group(1) else int(match.group(1))
                self.assertEqual(actual, expected_value, f"{path} has wrong {key}")

            for required_key in (
                "duplicate_rate",
                "out_of_order_rate",
                "fault_rate_per_hour",
                "meter_interval_sec",
                "operators",
            ):
                self.assertIn(required_key, text, f"{path} dropped simulator realism knob {required_key}")

    def test_scale_harness_collects_required_measurements_with_real_redpanda_lag(self):
        script = read_text("scripts/scale_test.sh")
        lowered = script.lower()

        self.assertIn("set -euo pipefail", lowered, "scale harness should fail fast on command errors")
        self.assertRegex(script, r"PRESETS=.*1k.*10k.*50k.*100k", "default presets should cover 1k->100k")
        self.assertIn("/app/config/scale-${preset}.yaml", script)
        self.assertIn('CONFIG_PATH="$cfg"', script, "harness should drive rate by swapping CONFIG_PATH")
        self.assertNotRegex(script, r"group_lag\(\).*?recording lag as 0", "group_lag must not mask rpk failures as zero")
        self.assertNotRegex(script, r"\$\{rt_lag:-0\}|\$\{an_lag:-0\}", "required lag fields must not fallback to zero")
        self.assertRegex(script, r"TOTAL-LAG", "group_lag must require TOTAL-LAG in rpk output")
        self.assertRegex(script, r"return 1|exit 1", "group_lag failures must make the harness fail")

        registry_pos = script.find("registry-seed")
        wait_pos = script.find("wait registry-seed")
        restart_pos = script.find("simulator processor")
        self.assertGreaterEqual(registry_pos, 0, "harness must recreate registry-seed for each preset")
        self.assertGreater(wait_pos, registry_pos, "harness must wait for registry-seed before restarting services")
        self.assertGreater(restart_pos, wait_pos, "harness must restart simulator and processor after reseed")

        self.assertIn("rpk group describe", lowered)
        self.assertIn("group_lag realtime", lowered)
        self.assertIn("group_lag analytics", lowered)
        self.assertNotIn(
            "processor_consumer_lag",
            lowered,
            "Phase 4 must use real Redpanda lag, not kafka-go's best-effort processor gauge",
        )

        for token in (
            "simulator_events_produced_total",
            "processor_clean_produced_total",
            "processor_transport_lag_seconds_bucket",
            "processor_state_apply_lag_seconds",
            "histogram_quantile(0.5",
            "histogram_quantile(0.95",
            "histogram_quantile(0.99",
            "analytics/queries/A1_hourly_energy.sql",
            "analytics/queries/A4_revenue.sql",
            "redis-cli HGETALL",
            "benchmarks/results.csv",
        ):
            self.assertIn(token.lower(), lowered, f"scale harness is missing {token}")

        header_match = require_match(
            r'echo\s+"([^"]+)"\s*>\s*"\$OUT"',
            script,
            "scale harness should write a CSV header to benchmarks/results.csv",
        )
        header = header_match.group(1).split(",")
        self.assertEqual(
            header,
            [
                "preset",
                "target_eps",
                "produced_eps",
                "clean_eps",
                "clean_lag_p50",
                "clean_lag_p95",
                "clean_lag_p99",
                "rt_apply_p50",
                "rt_apply_p95",
                "rt_apply_p99",
                "ch_fresh_ms",
                "realtime_lag",
                "analytics_lag",
                "a1_ms",
                "a4_ms",
                "redis_ms",
            ],
        )

    def test_processor_hot_path_benchmark_exists_for_phase4_bottleneck_claims(self):
        bench = read_text("processor/flatten_bench_test.go")
        for token in (
            "func BenchmarkFlattenValidate",
            "Decode(",
            "Validate(",
            "transform.Flatten",
            "ReportAllocs",
            "SetBytes",
        ):
            self.assertIn(token, bench, f"processor benchmark missing {token}")

    def test_phase4_results_csv_contains_measured_scale_curve(self):
        path = ROOT / "benchmarks/results.csv"
        self.assertTrue(
            path.exists(),
            "Phase 4 must produce benchmarks/results.csv; run scripts/scale_test.sh and commit the measured output",
        )

        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))

        self.assertGreaterEqual(len(rows), 4, "results.csv should contain the 1k, 10k, 50k, and 100k presets")
        expected_columns = {
            "preset",
            "target_eps",
            "produced_eps",
            "clean_eps",
            "clean_lag_p50",
            "clean_lag_p95",
            "clean_lag_p99",
            "rt_apply_p50",
            "rt_apply_p95",
            "rt_apply_p99",
            "ch_fresh_ms",
            "realtime_lag",
            "analytics_lag",
            "a1_ms",
            "a4_ms",
            "redis_ms",
        }
        self.assertEqual(set(rows[0].keys()), expected_columns)
        self.assertEqual({row["preset"] for row in rows}, {"1k", "10k", "50k", "100k"})

        target_by_preset = {"1k": 1000, "10k": 10000, "50k": 50000, "100k": 100000}
        numeric_columns = expected_columns - {"preset"}
        for row in rows:
            self.assertEqual(int(row["target_eps"]), target_by_preset[row["preset"]])
            for column in numeric_columns:
                self.assertNotIn(row[column].upper(), {"", "NA", "N/A"}, f"{column} missing for {row['preset']}")
                value = float(row[column])
                self.assertGreaterEqual(value, 0, f"{column} must be non-negative for {row['preset']}")

            self.assertGreater(float(row["produced_eps"]), 0, f"produced_eps must be measured for {row['preset']}")
            self.assertGreater(float(row["clean_eps"]), 0, f"clean_eps must be measured for {row['preset']}")
            self.assertGreater(float(row["a1_ms"]), 0, f"A1 latency must be measured for {row['preset']}")
            self.assertGreater(float(row["a4_ms"]), 0, f"A4 latency must be measured for {row['preset']}")

    def test_phase4_docs_report_honest_bottleneck_and_path_to_100k(self):
        readme = compact(read_text("README.md"))
        arch = compact(read_text("docs/ARCHITECTURE.md"))
        combined = f"{readme} {arch}"

        self.assertRegex(readme, r"scripts/scale_test\.sh|scale test", "README must document how to run Phase 4")
        self.assertIn("benchmarks/results.csv", combined)
        self.assertRegex(combined, r"1k.*10k.*50k.*100k|100k.*50k.*10k.*1k")
        self.assertRegex(combined, r"bottleneck|saturation|consumer[- ]group lag|backlog")
        self.assertRegex(combined, r"path to 100k|100k path|to 100k")
        self.assertRegex(combined, r"laptop|mac|local machine|dev-container|single-node")
        self.assertRegex(combined, r"produced_eps|clean_eps|throughput")
        self.assertRegex(combined, r"lag_p95|p95|p99|ingestion lag|transport lag")
        self.assertRegex(combined, r"a1_ms|a4_ms|query latency")
        self.assertRegex(combined, r"pprof|benchmarkflattenvalidate|go test .*bench")
        self.assertNotIn(
            "section to be appended: phase 4",
            combined,
            "architecture report still contains the Phase 4 placeholder",
        )


if __name__ == "__main__":
    unittest.main()
