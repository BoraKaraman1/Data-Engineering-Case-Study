import csv
import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read_text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def compact_sql(sql: str) -> str:
    sql = re.sub(r"--.*?$", " ", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"\s+", " ", sql).strip().lower()


def find_one(pattern: str) -> Path:
    matches = sorted(ROOT.glob(pattern))
    if not matches:
        raise AssertionError(f"missing artifact matching {pattern!r}")
    if len(matches) > 1:
        raise AssertionError(f"expected one artifact matching {pattern!r}, found {matches}")
    return matches[0]


def query_sql(label: str) -> str:
    path = find_one(f"analytics/queries/{label}*.sql")
    text = path.read_text(encoding="utf-8")
    if "todo" in text.lower() or "placeholder" in text.lower():
        raise AssertionError(f"{path} still contains TODO/placeholder text")
    return compact_sql(text)


def collect_strings(value) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        out = []
        for item in value:
            out.extend(collect_strings(item))
        return out
    if isinstance(value, dict):
        out = []
        for item in value.values():
            out.extend(collect_strings(item))
        return out
    return []


class Phase3ArtifactsTest(unittest.TestCase):
    def test_a1_hourly_energy_uses_session_deltas_not_raw_cumulative_sum(self):
        sql = query_sql("A1")
        self.assertIn("energy_kwh", sql)
        self.assertIn("session_id", sql)
        self.assertRegex(sql, r"(tostartofhour|date_trunc\s*\(\s*'hour'|interval\s+1\s+hour)")
        self.assertRegex(sql, r"(lag|lead|max\s*\(\s*energy_kwh\s*\)\s*-\s*min\s*\(\s*energy_kwh\s*\))")
        self.assertRegex(sql, r"(meter_update|session_stop)")
        self.assertNotRegex(sql, r"sum\s*\(\s*energy_kwh\s*\)")

    def test_a2_uptime_uses_status_change_timeline_and_operator_grouping(self):
        sql = query_sql("A2")
        self.assertIn("status_change", sql)
        self.assertIn("operator_id", sql)
        self.assertIn("status", sql)
        self.assertRegex(sql, r"(lead|lag|datediff|date_diff)")
        self.assertRegex(sql, r"(faulted|available|downtime|uptime)")

    def test_a3_duration_and_energy_by_vehicle_brand(self):
        sql = query_sql("A3")
        self.assertIn("vehicle_brand", sql)
        self.assertIn("session_start", sql)
        self.assertIn("session_stop", sql)
        self.assertRegex(sql, r"(datediff|date_diff|timestamp.*-.*timestamp|stop.*-.*start)")
        self.assertRegex(sql, r"(max\s*\(\s*energy_kwh\s*\)\s*-\s*min\s*\(\s*energy_kwh\s*\)|argmax|anyif)")
        self.assertNotRegex(sql, r"sum\s*\(\s*energy_kwh\s*\)")

    def test_a4_revenue_uses_billed_cost_and_peak_breakdown(self):
        sql = query_sql("A4")
        self.assertIn("cost_eur", sql)
        self.assertIn("tariff_id", sql)
        self.assertRegex(sql, r"(operator_id|city)")
        # Peak is the rate the simulator actually billed (is_peak_priced on SESSION_STOP),
        # not a wall-clock hour window re-derived downstream (H3).
        self.assertIn("is_peak_priced", sql)
        self.assertRegex(sql, r"(peak|tohour|extract\s*\(\s*hour)")

    def test_a5_fault_geography_is_deduped(self):
        sql = query_sql("A5")
        self.assertIn("fault_alert", sql)
        self.assertRegex(sql, r"(city|lat|lon)")
        self.assertRegex(sql, r"(uniqexact\s*\(\s*event_id\s*\)|group\s+by[^;]*event_id|final)")

    def test_a6_anomaly_detection_uses_two_sigma_rule(self):
        sql = query_sql("A6")
        self.assertRegex(sql, r"(stddev|stddevsamp|stddevpop|var_samp|var_pop)")
        self.assertRegex(sql, r"(avg|mean)")
        self.assertRegex(sql, r"(\+\s*2\s*\*|2\s*\*)")
        self.assertRegex(sql, r"(power_kw|avg_power)")

    def test_revenue_aggregate_mv_is_block_safe(self):
        sql = compact_sql(read_text("deploy/clickhouse/init/02_aggregates.sql"))
        self.assertIn("revenue_hourly", sql)
        self.assertIn("materialized view", sql)
        self.assertIn("summingmergetree", sql)
        self.assertIn("session_stop", sql)
        self.assertIn("cost_eur", sql)
        self.assertNotRegex(sql, r"sum\s*\(\s*energy_kwh\s*\)")

    def test_notebook_runs_a1_to_a6_and_writes_csv_outputs(self):
        notebook_path = ROOT / "analytics/report.ipynb"
        self.assertTrue(notebook_path.exists(), "missing analytics/report.ipynb")
        notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
        self.assertEqual(notebook.get("nbformat"), 4)
        source = "\n".join(collect_strings(notebook.get("cells", []))).lower()
        self.assertIn("clickhouse", source)
        self.assertIn("to_csv", source)
        self.assertIn("analytics/output", source)
        self.assertRegex(source, r"(cumulative|energy trap|energy_kwh)")
        for label in ("a1", "a2", "a3", "a4", "a5", "a6"):
            self.assertIn(label, source, f"notebook does not reference {label.upper()}")

    def test_csv_outputs_exist_and_are_nonempty(self):
        for label in ("A1", "A2", "A3", "A4", "A5", "A6"):
            path = find_one(f"analytics/output/{label}*.csv")
            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.reader(handle))
            self.assertGreaterEqual(len(rows), 2, f"{path} must contain a header and data")
            self.assertTrue(any(cell.strip() for cell in rows[0]), f"{path} has an empty header")

    def test_ops_grafana_dashboard_uses_real_redpanda_lag(self):
        dashboard_path = ROOT / "deploy/grafana/dashboards/ops-pipeline.json"
        self.assertTrue(dashboard_path.exists(), "missing ops Grafana dashboard")
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        text = "\n".join(collect_strings(dashboard)).lower()
        for metric in (
            "simulator_events_produced_total",
            "processor_clean_produced_total",
            "processor_transport_lag_seconds",
            "processor_dlq_total",
            "processor_duplicates_dropped_total",
        ):
            self.assertIn(metric, text)
        self.assertNotIn(
            "processor_consumer_lag",
            text,
            "ops dashboard must not treat the processor's best-effort lag gauge as authoritative",
        )
        self.assertRegex(text, r"redpanda.*(lag|consumer|group|offset)|(lag|consumer|group|offset).*redpanda")

        prometheus = read_text("deploy/prometheus/prometheus.yml").lower()
        self.assertIn("redpanda", prometheus)
        self.assertRegex(prometheus, r"redpanda:9644")

    def test_scale_harness_uses_rpk_group_describe_for_authoritative_lag(self):
        script = read_text("scripts/scale_test.sh").lower()
        self.assertIn("rpk group describe", script)
        self.assertIn("realtime", script)
        self.assertIn("analytics", script)
        self.assertNotIn("processor_consumer_lag", script)


if __name__ == "__main__":
    unittest.main()
