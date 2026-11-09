"""Trigger and verify the local DAG through Airflow's real authenticated REST API.

Run inside the airflow-api-server container. Never prints passwords or JWTs.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

BASE = "http://127.0.0.1:8080"
DAG = "lakehouse_pro"


def request(path: str, method: str = "GET", body=None, token: str | None = None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    payload = None if body is None else json.dumps(body).encode()
    req = Request(BASE + path, data=payload, headers=headers, method=method)
    try:
        with urlopen(req, timeout=20) as response:
            return json.load(response)
    except HTTPError as exc:
        # Never log a request/response body containing authentication details.
        raise RuntimeError(f"Airflow HTTP {exc.code}: {method} {path}") from None


def run(timeout: int = 900) -> dict:
    run_id = "portfolio_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
    output = Path("/app/work/airflow-runs") / run_id
    output.mkdir(parents=True, exist_ok=False)
    result = {
        "status": "running",
        "dag_id": DAG,
        "run_id": run_id,
        "transport": "authenticated Airflow REST API; scheduler/LocalExecutor",
        "started_at_utc": datetime.now(UTC).isoformat(),
        "limitations": "Local portfolio run; no distributed/high-availability claim",
    }
    started = time.monotonic()
    try:
        home = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow/runtime"))
        users = json.loads((home / "simple_auth_manager_passwords.json.generated").read_text())
        token = request("/auth/token", "POST", {"username": "admin", "password": users["admin"]})[
            "access_token"
        ]
        # Wait for initial DAG serialization by the separate DAG processor.
        deadline = time.monotonic() + min(timeout, 180)
        while True:
            try:
                request(f"/api/v2/dags/{DAG}", token=token)
                break
            except RuntimeError as exc:
                if "HTTP 404:" not in str(exc) or time.monotonic() >= deadline:
                    raise
                print(json.dumps({"event": "waiting_for_dag_registration", "dag_id": DAG}), flush=True)
                time.sleep(5)
        request(f"/api/v2/dags/{DAG}", "PATCH", {"is_paused": False}, token)
        request(
            f"/api/v2/dags/{DAG}/dagRuns",
            "POST",
            {"dag_run_id": run_id, "logical_date": None, "conf": {}},
            token,
        )
        result["triggered"] = True
        path = f"/api/v2/dags/{DAG}/dagRuns/{quote(run_id, safe='')}"
        last_state = None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = request(path, token=token)["state"]
            if state != last_state:
                print(
                    json.dumps({"event": "airflow_run_state", "run_id": run_id, "state": state}), flush=True
                )
                last_state = state
            if state in {"success", "failed"}:
                break
            time.sleep(5)
        else:
            raise TimeoutError("DAG timeout; the run was left intact for inspection")
        tasks = request(path + "/taskInstances", token=token)["task_instances"]
        result["tasks"] = [{"task_id": t["task_id"], "state": t["state"]} for t in tasks]
        if state != "success" or {t["task_id"] for t in tasks} != {"batch_prf", "dbt_quality"}:
            raise RuntimeError("DAG failed or task inventory does not match the expected pipeline")
        if any(t["state"] != "success" for t in tasks):
            raise RuntimeError("At least one task did not finish successfully")
        check = subprocess.run(
            ["/opt/airflow/project-venv/bin/python", "/app/scripts/check_airflow_outputs.py"],
            text=True,
            capture_output=True,
            timeout=120,
            check=True,
        )
        result["data_validation"] = json.loads(check.stdout)
        result["status"] = "passed"
        return result
    except BaseException as exc:
        # Only the exception type is retained; dependency errors can contain sensitive paths/configuration.
        result.update(status="failed", error_type=type(exc).__name__)
        if isinstance(exc, (RuntimeError, TimeoutError)):
            result["error"] = str(exc)
        raise
    finally:
        result["seconds"] = time.monotonic() - started
        (output / "result.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=900)
    args = parser.parse_args()
    if not 30 <= args.timeout <= 1800:
        parser.error("--timeout must be in 30..1800 seconds")
    run(args.timeout)
