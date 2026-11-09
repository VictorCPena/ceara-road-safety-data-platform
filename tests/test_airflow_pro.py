"""Airflow DAG contract and safe configuration lifecycle.

Install apache-airflow==3.3.2 + pytest + PyYAML in an isolated environment.
Missing Airflow means skipped, never an approved orchestration integration.
"""

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_secret_preparation_is_private_idempotent_and_preserves_existing(tmp_path, capsys):
    prepare = load_script("prepare_airflow_pro").prepare
    csv = tmp_path / "data/raw/datatran2024.csv"
    csv.parent.mkdir(parents=True)
    csv.write_text("fixture")
    path = prepare(tmp_path)
    before = path.read_bytes()
    assert path.stat().st_mode & 0o777 == 0o600
    prepare(tmp_path)
    assert path.read_bytes() == before
    assert "AIRFLOW_PRO_DB_PASSWORD=" not in capsys.readouterr().out
    path.write_text("UNKNOWN=keep\n")
    with pytest.raises(ValueError, match="incomplete"):
        prepare(tmp_path)
    assert path.read_text() == "UNKNOWN=keep\n"


def test_preparation_refuses_symlink(tmp_path):
    prepare = load_script("prepare_airflow_pro").prepare
    csv = tmp_path / "data/raw/datatran2024.csv"
    csv.parent.mkdir(parents=True)
    csv.write_text("fixture")
    original = tmp_path / "user-secret"
    original.write_text("preserve")
    (tmp_path / ".env.airflow-pro").symlink_to(original)
    with pytest.raises(ValueError, match="symlink"):
        prepare(tmp_path)
    assert original.read_text() == "preserve"


def test_batch_failure_blocks_analytics_and_tasks_are_bounded():
    pytest.importorskip("airflow", reason="Dedicated Airflow environment required")
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(ROOT / "orchestration/dags/lakehouse_pro.py"))
    assert not bag.import_errors
    dag = bag.dags["lakehouse_pro"]
    batch, dbt = dag.task_dict["batch_prf"], dag.task_dict["dbt_quality"]
    assert batch.downstream_task_ids == {"dbt_quality"}
    assert dbt.trigger_rule == "all_success"
    assert dag.max_active_runs == dag.max_active_tasks == 1
    for task in (batch, dbt):
        assert task.execution_timeout.total_seconds() > 0
        assert task.append_env is True
        assert task.env["DBT_SEND_ANONYMOUS_USAGE_STATS"] == "false"
        assert task.do_xcom_push is False


def test_compose_isolates_state_and_does_not_disable_authentication():
    yaml = pytest.importorskip("yaml")
    cfg = yaml.safe_load((ROOT / "compose.airflow-pro.yaml").read_text())
    assert cfg["name"] == "ceara-lakehouse-airflow-pro"
    api = cfg["services"]["airflow-api-server"]
    assert api["ports"] == ["127.0.0.1:${AIRFLOW_PRO_PORT:-18080}:8080"]
    assert api["environment"]["AIRFLOW__API__BASE_URL"] == "http://localhost:${AIRFLOW_PRO_PORT:-18080}"
    assert api["environment"]["AIRFLOW__CORE__EXECUTION_API_SERVER_URL"] == "http://airflow-api-server:8080/execution/"
    assert cfg["services"]["postgres"].get("ports") is None
    assert api["environment"]["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS"] == "false"
    assert api["build"]["dockerfile"] == "Dockerfile.airflow-pro"
    assert "airflow_pro_work:/app/work" in api["volumes"]
    assert cfg["services"]["airflow-init"]["restart"] == "no"


def test_http_auth_errors_do_not_echo_password_or_token(monkeypatch):
    from urllib.error import HTTPError
    from io import BytesIO

    api = load_script("run_airflow_pro")

    def fail(*args, **kwargs):
        raise HTTPError(
            "http://127.0.0.1/auth/token",
            401,
            "denied",
            {},
            BytesIO(json.dumps({"password": "sensitive"}).encode()),
        )

    monkeypatch.setattr(api, "urlopen", fail)
    with pytest.raises(RuntimeError) as exc:
        api.request("/auth/token", "POST", {"password": "sensitive"})
    assert "401" in str(exc.value) and "sensitive" not in str(exc.value)
