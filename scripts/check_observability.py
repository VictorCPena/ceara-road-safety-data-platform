"""Validate the local scrape, dashboard, alert and webhook using real HTTP APIs."""

import argparse
from datetime import datetime
import json
import os
import time
from urllib.parse import urlencode
from urllib.request import urlopen


ALERT = "PRFQuarantineNonzero"
PORTS = {
    "prometheus": ("PRF_OBS_PROMETHEUS_PORT", 19090),
    "alertmanager": ("PRF_OBS_ALERTMANAGER_PORT", 19093),
    "grafana": ("PRF_OBS_GRAFANA_PORT", 13000),
    "receiver": ("PRF_OBS_RECEIVER_PORT", 19108),
}


def get(port: int, path: str):
    with urlopen(f"http://127.0.0.1:{port}{path}", timeout=4) as response:
        return json.load(response)


def check(ports: dict[str, int]) -> dict:
    def value(expression: str, service: str = "prometheus") -> float:
        endpoint = "/api/v1/query"
        if service == "grafana":
            endpoint = "/api/datasources/proxy/uid/lakehouse-prom" + endpoint
        payload = get(ports[service], endpoint + "?" + urlencode({"query": expression}))
        series = payload["data"]["result"]
        if payload["status"] != "success" or len(series) != 1:
            raise ValueError(f"{service} has no single sample for {expression}")
        return float(series[0]["value"][1])

    if value('up{job="lakehouse-airflow"}') != 1:
        raise ValueError("Exporter scrape unavailable")
    if value("prf_airflow_last_run_success") != 1:
        raise ValueError("Latest runner report did not pass")
    bronze = int(value("prf_airflow_bronze_rows"))
    accepted = int(value("prf_accepted_rows"))
    quarantined = int(value("prf_quarantined_rows"))
    models = int(value("prf_airflow_dbt_models"))
    tests = int(value("prf_airflow_dbt_data_tests"))
    if bronze != accepted + quarantined or quarantined <= 0:
        raise ValueError("The quarantine alert demo requires a passing report with quarantined rows")
    if value(f'ALERTS{{alertname="{ALERT}",alertstate="firing"}}') != 1:
        raise ValueError("Prometheus is waiting for the quarantine alert to fire")
    active = get(ports["alertmanager"], "/api/v2/alerts")
    current = next(
        (
            alert
            for alert in active
            if alert["labels"]["alertname"] == ALERT and alert["status"]["state"] == "active"
        ),
        None,
    )
    if current is None:
        raise ValueError("Alertmanager has not received the active alert")
    starts_at = datetime.fromisoformat(current["startsAt"]).timestamp()
    receipts = get(ports["receiver"], "/notifications")["notifications"]
    receipt = next(
        (
            event
            for event in reversed(receipts)
            if event["alertname"] == ALERT
            and event["status"] == "firing"
            and abs(event["starts_at"] - starts_at) < 1
        ),
        None,
    )
    if receipt is None:
        raise ValueError("Waiting for the matching webhook receipt")
    dashboard = get(ports["grafana"], "/api/dashboards/uid/prf-airflow")["dashboard"]
    if not dashboard["panels"] or value("prf_accepted_rows", "grafana") != accepted:
        raise ValueError("Grafana dashboard or datasource query unavailable")
    return {
        "status": "passed",
        "prometheus_scrape_up": True,
        "source": "latest completed passing report in the Airflow work volume",
        "bronze_rows": bronze,
        "accepted_rows": accepted,
        "quarantined_rows": quarantined,
        "dbt_models": models,
        "dbt_data_tests": tests,
        "grafana_dashboard_uid": dashboard["uid"],
        "grafana_datasource_query_verified": True,
        "alertmanager_active_alert": ALERT,
        "webhook_receipt": receipt,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait", type=int, default=150, help="Seconds to wait for first scrape/alert.")
    args = parser.parse_args()
    if not 1 <= args.wait <= 300:
        parser.error("--wait must be in 1..300 seconds")
    ports = {service: int(os.environ.get(var, default)) for service, (var, default) in PORTS.items()}
    deadline = time.monotonic() + args.wait
    previous_error = None
    while True:
        try:
            result = check(ports)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            if str(exc) != previous_error:
                print(json.dumps({"event": "observability_wait", "detail": str(exc)}), flush=True)
                previous_error = str(exc)
            if time.monotonic() >= deadline:
                raise SystemExit(
                    "Observability verification timed out. Inspect service status and logs."
                ) from exc
            time.sleep(5)
        else:
            print(json.dumps(result, indent=2), flush=True)
            return


if __name__ == "__main__":
    main()
