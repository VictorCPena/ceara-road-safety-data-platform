from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


PROJECT_DIR = "/app"

PYTHON = "/opt/airflow/project-venv/bin/python"
DBT = "/opt/airflow/project-venv/bin/dbt"


def python_module(
    task_id: str,
    module: str,
    retries: int = 1,
    pool: str = "default_pool",
) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=(
            "set -euo pipefail; "
            f"cd {PROJECT_DIR}; "
            f"{PYTHON} -m {module}"
        ),
        retries=retries,
        retry_delay=timedelta(minutes=2),
        pool=pool,
    )


with DAG(
    dag_id="ceara_road_safety_full_pipeline",
    description=(
        "End-to-end road safety data pipeline: "
        "PRF + IBGE + Open-Meteo -> Silver -> Quality -> Gold"
    ),
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="America/Fortaleza",
    ),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=[
        "data-engineering",
        "ceara",
        "road-safety",
        "prf",
        "ibge",
        "open-meteo",
        "dbt",
    ],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    with TaskGroup(
        group_id="prf",
        tooltip="PRF ingestion, Silver transformation and quality",
    ) as prf:

        prf_ingest = python_module(
            task_id="ingest",
            module="src.ingestion.prf.ingest",
            retries=2,
        )

        prf_transform_occurrence = python_module(
            task_id="transform_occurrence",
            module="src.transform.prf.occurrence_to_silver",
            pool="heavy_etl",
        )

        prf_transform_person = python_module(
            task_id="transform_person",
            module="src.transform.prf.person_to_silver",
            pool="heavy_etl",
        )

        prf_transform_all_causes = python_module(
            task_id="transform_all_causes",
            module="src.transform.prf.all_causes_to_silver",
            pool="heavy_etl",
        )

        quality_occurrence = python_module(
            task_id="quality_occurrence",
            module="src.quality.prf.check_occurrence_silver",
        )

        quality_person = python_module(
            task_id="quality_person",
            module="src.quality.prf.check_person_silver",
        )

        quality_all_causes = python_module(
            task_id="quality_all_causes",
            module="src.quality.prf.check_all_causes_silver",
        )

        prf_ingest >> [
            prf_transform_occurrence,
            prf_transform_person,
            prf_transform_all_causes,
        ]

        prf_transform_occurrence >> quality_occurrence
        prf_transform_person >> quality_person
        prf_transform_all_causes >> quality_all_causes

    with TaskGroup(
        group_id="ibge",
        tooltip="IBGE municipalities and population",
    ) as ibge:

        ibge_ingest_municipalities = python_module(
            task_id="ingest_municipalities",
            module="src.ingestion.ibge.municipalities",
            retries=2,
        )

        ibge_transform_municipalities = python_module(
            task_id="transform_municipalities",
            module="src.transform.ibge.municipality_to_silver",
            pool="heavy_etl",
        )

        quality_municipalities = python_module(
            task_id="quality_municipalities",
            module="src.quality.ibge.check_municipality_silver",
        )

        ibge_ingest_population = python_module(
            task_id="ingest_population",
            module="src.ingestion.ibge.population",
            retries=2,
        )

        ibge_transform_population = python_module(
            task_id="transform_population",
            module="src.transform.ibge.population_to_silver",
            pool="heavy_etl",
        )

        quality_population = python_module(
            task_id="quality_population",
            module="src.quality.ibge.check_population_silver",
        )

        (
            ibge_ingest_municipalities
            >> ibge_transform_municipalities
            >> quality_municipalities
        )

        (
            ibge_ingest_population
            >> ibge_transform_population
            >> quality_population
        )

    municipality_match = python_module(
        task_id="validate_prf_ibge_municipality_match",
        module="src.quality.ibge.check_prf_municipality_match",
    )

    with TaskGroup(
        group_id="weather",
        tooltip="Open-Meteo weather enrichment and quality",
    ) as weather:

        weather_ingest = python_module(
            task_id="ingest",
            module="src.ingestion.open_meteo.accident_weather",
            retries=2,
        )

        weather_transform = python_module(
            task_id="transform",
            module="src.transform.open_meteo.accident_weather_to_silver",
            pool="heavy_etl",
        )

        quality_weather = python_module(
            task_id="quality",
            module="src.quality.open_meteo.check_accident_weather_silver",
        )

        (
            weather_ingest
            >> weather_transform
            >> quality_weather
        )

    dbt_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command=(
            "set -euo pipefail; "
            f"cd {PROJECT_DIR}; "
            f"{DBT} build "
            "--project-dir analytics "
            "--profiles-dir analytics"
        ),
        retries=1,
        retry_delay=timedelta(minutes=2),
        pool="heavy_etl",
    )

    end = EmptyOperator(
        task_id="pipeline_completed",
    )

    start >> prf
    start >> ibge

    quality_occurrence >> municipality_match
    quality_municipalities >> municipality_match

    quality_occurrence >> weather

    quality_occurrence >> dbt_gold
    quality_person >> dbt_gold
    quality_all_causes >> dbt_gold

    quality_municipalities >> dbt_gold
    quality_population >> dbt_gold

    municipality_match >> dbt_gold
    quality_weather >> dbt_gold

    dbt_gold >> end
