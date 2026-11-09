"""Exercise the exporter and local receiver with temporary reports and real HTTP."""

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from monitoring.server import make_server, snapshot


class ObservabilityTest(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.runs = Path(temp.name)

    def report(self, run_id: str, status: str = "passed") -> Path:
        output = self.runs / run_id / "result.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        report = {
            "run_id": run_id,
            "dag_id": "lakehouse_pro",
            "status": status,
            "seconds": 12.5,
        }
        if status == "passed":
            report["data_validation"] = {
                "status": "passed",
                "accepted": 2,
                "quarantined": 1,
                "bronze_rows": 3,
                "silver_rows": 2,
                "gold_accidents": 2,
                "dbt_fact_rows": 2,
                "dbt_models": 3,
                "dbt_data_tests": 7,
            }
        output.write_text(json.dumps(report))
        return output

    def start_server(self, role: str) -> str:
        server = make_server("127.0.0.1", 0, role, self.runs)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_port}"

    def test_exporter_serves_passed_counts_over_http(self):
        self.report("portfolio_first")
        base = self.start_server("exporter")
        with urlopen(base + "/metrics", timeout=4) as response:
            data = response.read().decode()
            self.assertEqual(response.status, 200)
            self.assertIn("version=0.0.4", response.headers["Content-Type"])
        self.assertIn("prf_airflow_last_run_success 1\n", data)
        self.assertIn("prf_airflow_bronze_rows 3\n", data)
        self.assertIn("prf_quarantined_rows 1\n", data)
        self.assertIn("prf_airflow_dbt_data_tests 7\n", data)

    def test_latest_failure_does_not_export_previous_passing_counts(self):
        passed = self.report("portfolio_later_name")
        failed = self.report("portfolio_earlier_name", "failed")
        os.utime(passed, (100, 100))
        os.utime(failed, (200, 200))
        result = snapshot(self.runs)
        self.assertEqual(result["prf_airflow_report_available"], 1)
        self.assertEqual(result["prf_airflow_last_run_success"], 0)
        self.assertNotIn("prf_accepted_rows", result)

    def test_incomplete_or_corrupt_report_is_not_a_success(self):
        self.assertEqual(snapshot(self.runs), {"prf_airflow_report_available": 0})
        output = self.report("portfolio_first")
        valid = output.read_bytes()
        output.write_text('{"status":')
        self.assertEqual(snapshot(self.runs), {"prf_airflow_report_available": 0})
        output.write_bytes(valid)
        self.assertEqual(snapshot(self.runs)["prf_airflow_last_run_success"], 1)

    def test_webhook_rejects_bad_payload_and_records_notification(self):
        base = self.start_server("receiver")

        def send(body: dict):
            return urlopen(
                Request(
                    base + "/alerts",
                    data=json.dumps(body).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                ),
                timeout=4,
            )

        with self.assertRaises(HTTPError) as err:
            send({"alerts": [{"status": "firing"}]})
        self.assertEqual(err.exception.code, 400)
        err.exception.close()
        with send(
            {
                "alerts": [
                    {
                        "labels": {"alertname": "PRFQuarantineNonzero"},
                        "status": "firing",
                        "startsAt": "2026-11-06T21:46:40Z",
                    }
                ],
            }
        ) as response:
            self.assertEqual(json.load(response), {"received": 1})
        with urlopen(base + "/notifications", timeout=4) as response:
            events = json.load(response)["notifications"]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["alertname"], "PRFQuarantineNonzero")
        self.assertEqual(events[0]["status"], "firing")


if __name__ == "__main__":
    unittest.main()
