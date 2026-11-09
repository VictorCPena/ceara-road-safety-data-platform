"""Read completed Airflow reports and receive local Alertmanager notifications."""

from __future__ import annotations

import argparse
import json
import math
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock

METRICS = {
    "prf_airflow_report_available": "Whether the newest completed report is readable and valid.",
    "prf_airflow_last_run_success": "Whether the newest completed run passed data validation.",
    "prf_airflow_last_run_duration_seconds": "Duration reported by the run and verification helper.",
    "prf_airflow_last_report_timestamp_seconds": "Modification time of the newest completed report.",
    "prf_accepted_rows": "Accepted PRF rows in the newest passing run.",
    "prf_quarantined_rows": "Quarantined PRF rows in the newest passing run.",
    "prf_airflow_bronze_rows": "Bronze rows in the newest passing run.",
    "prf_airflow_silver_rows": "Silver rows in the newest passing run.",
    "prf_airflow_gold_accidents": "Accidents reconciled in the Gold aggregate.",
    "prf_airflow_dbt_fact_rows": "Rows reconciled in the dbt fact table.",
    "prf_airflow_dbt_models": "Successful dbt models in the newest passing run.",
    "prf_airflow_dbt_data_tests": "Passing dbt data tests in the newest passing run.",
}
COUNTS = {
    "accepted": "prf_accepted_rows",
    "quarantined": "prf_quarantined_rows",
    "bronze_rows": "prf_airflow_bronze_rows",
    "silver_rows": "prf_airflow_silver_rows",
    "gold_accidents": "prf_airflow_gold_accidents",
    "dbt_fact_rows": "prf_airflow_dbt_fact_rows",
    "dbt_models": "prf_airflow_dbt_models",
    "dbt_data_tests": "prf_airflow_dbt_data_tests",
}
ALERT_NAMES = {
    "PRFQuarantineNonzero",
    "LakehouseLatestRunFailed",
    "LakehouseReportUnavailable",
    "LakehouseExporterDown",
}
MAX_BODY_BYTES = 65536


def snapshot(runs: Path) -> dict[str, int | float]:
    """Read the most recently written completed runner report."""
    unavailable = {"prf_airflow_report_available": 0}
    try:
        latest = max(
            runs.glob("portfolio_*/result.json"), key=lambda path: path.stat().st_mtime_ns, default=None
        )
        if latest is None:
            return unavailable
        # Reports are tiny; bound reads and handle partial writes during a scrape.
        with latest.open("rb") as stream:
            content = stream.read(MAX_BODY_BYTES + 1)
            mtime = latest.stat().st_mtime
        if len(content) > MAX_BODY_BYTES:
            return unavailable
        report = json.loads(content)
        if report["dag_id"] != "lakehouse_pro" or report["run_id"] != latest.parent.name:
            return unavailable
        status = report["status"]
        seconds = report["seconds"]
        if status not in {"passed", "failed"} or type(seconds) not in {int, float}:
            return unavailable
        if not math.isfinite(seconds) or seconds < 0:
            return unavailable
        result = {
            "prf_airflow_report_available": 1,
            "prf_airflow_last_run_success": int(status == "passed"),
            "prf_airflow_last_run_duration_seconds": seconds,
            "prf_airflow_last_report_timestamp_seconds": mtime,
        }
        if status == "passed":
            validation = report["data_validation"]
            if validation["status"] != "passed":
                return unavailable
            for key, metric in COUNTS.items():
                value = validation[key]
                if type(value) is not int or value < 0:
                    return unavailable
                result[metric] = value
        return result
    except (OSError, UnicodeError, ValueError, TypeError, KeyError, OverflowError):
        return unavailable


def prometheus_text(values: dict[str, int | float]) -> bytes:
    return "".join(
        f"# HELP {name} {METRICS[name]}\n# TYPE {name} gauge\n{name} {value}\n"
        for name, value in values.items()
    ).encode()


def notification_events(payload: dict) -> list[dict]:
    alerts = payload["alerts"]
    if not isinstance(alerts, list) or not alerts:
        raise ValueError("Expected a nonempty alerts list")
    events = []
    for alert in alerts:
        name = alert["labels"]["alertname"]
        status = alert["status"]
        started = datetime.fromisoformat(alert["startsAt"])
        if name not in ALERT_NAMES or status not in {"firing", "resolved"} or started.tzinfo is None:
            raise ValueError("Unexpected alert name, state or timestamp")
        events.append(
            {
                "alertname": name,
                "status": status,
                "starts_at": started.timestamp(),
                "received_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return events


def make_server(host: str, port: int, role: str, runs: Path) -> ThreadingHTTPServer:
    notifications: deque[dict] = deque(maxlen=64)
    lock = Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            # Scrape requests are frequent. Only accepted alert events are logged.
            pass

        def send(self, status: int, body: bytes, content_type: str):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, status: int, body: dict):
            self.send(status, json.dumps(body).encode(), "application/json")

        def do_GET(self):
            if self.path == "/healthz":
                self.send_json(200, {"status": "ok", "role": role})
            elif role == "exporter" and self.path == "/metrics":
                self.send(200, prometheus_text(snapshot(runs)), "text/plain; version=0.0.4; charset=utf-8")
            elif role == "receiver" and self.path == "/notifications":
                with lock:
                    events = list(notifications)
                self.send_json(200, {"notifications": events})
            else:
                self.send_json(404, {"error": "not_found"})

        def do_POST(self):
            if role != "receiver" or self.path != "/alerts":
                self.send_json(404, {"error": "not_found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= MAX_BODY_BYTES:
                    raise ValueError("Invalid body length")
                self.connection.settimeout(5)
                events = notification_events(json.loads(self.rfile.read(length)))
            except (OSError, ValueError, TypeError, KeyError, OverflowError):
                self.send_json(400, {"error": "invalid_alert_payload"})
                return
            with lock:
                notifications.extend(events)
            for event in events:
                print(json.dumps({"event": "alert_notification_received", **event}), flush=True)
            self.send_json(200, {"received": len(events)})

    return ThreadingHTTPServer((host, port), Handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=["exporter", "receiver"])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9108)
    parser.add_argument("--runs", type=Path, default=Path("/airflow-work/airflow-runs"))
    args = parser.parse_args()
    with make_server(args.host, args.port, args.role, args.runs) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
