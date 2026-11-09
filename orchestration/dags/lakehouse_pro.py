"""Local PRF batch + dbt orchestration; compose.airflow-pro.yaml isolates its state."""

import os
import shlex
from datetime import datetime, timedelta, timezone
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT = os.environ.get("LAKEHOUSE_PROJECT_DIR", "/app")
PYTHON = os.environ.get("LAKEHOUSE_PYTHON", "/opt/airflow/project-venv/bin/python")
DBT = os.environ.get("LAKEHOUSE_DBT", "/opt/airflow/project-venv/bin/dbt")
CSV = os.environ.get("LAKEHOUSE_CSV", "/app/data/raw/datatran2024.csv")
TASK_ENV = {
    "TZ": "UTC",
    "SPARK_LOCAL_IP": "127.0.0.1",
    "PYSPARK_PYTHON": PYTHON,
    "PYSPARK_DRIVER_PYTHON": PYTHON,
    "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    "DO_NOT_TRACK": "true",
}

with DAG(
    dag_id="lakehouse_pro",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["portfolio"],
) as dag:
    batch = BashOperator(
        task_id="batch_prf",
        bash_command=f"{shlex.quote(PYTHON)} -m lakehouse.demo --csv {shlex.quote(CSV)}",
        cwd=PROJECT,
        env=TASK_ENV,
        append_env=True,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=10),
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    analytics = BashOperator(
        task_id="dbt_quality",
        bash_command=(
            f"{shlex.quote(DBT)} --log-path {shlex.quote(os.path.join(PROJECT, 'work/dbt-logs'))} "
            "build --project-dir dbt_lakehouse --profiles-dir dbt_lakehouse "
            f"--target-path {shlex.quote(os.path.join(PROJECT, 'work/dbt-target'))}"
        ),
        cwd=PROJECT,
        env=TASK_ENV,
        append_env=True,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=5),
    )
    batch >> analytics
