#!/usr/bin/env bash

set -euo pipefail

echo
echo "============================================================"
echo "CEARÁ ROAD SAFETY DATA PLATFORM"
echo "FULL DATA PIPELINE"
echo "============================================================"

echo
echo "[1/8] PRF - INGESTION"
python -m src.ingestion.prf.ingest


echo
echo "[2/8] PRF - BRONZE → SILVER"

python -m src.transform.prf.occurrence_to_silver
python -m src.transform.prf.person_to_silver
python -m src.transform.prf.all_causes_to_silver


echo
echo "[3/8] PRF - DATA QUALITY"

python -m src.quality.prf.check_occurrence_silver
python -m src.quality.prf.check_person_silver
python -m src.quality.prf.check_all_causes_silver


echo
echo "[4/8] IBGE - INGESTION"

python -m src.ingestion.ibge.municipalities
python -m src.ingestion.ibge.population


echo
echo "[5/8] IBGE - BRONZE → SILVER"

python -m src.transform.ibge.municipality_to_silver
python -m src.transform.ibge.population_to_silver


echo
echo "[6/8] IBGE - DATA QUALITY"

python -m src.quality.ibge.check_municipality_silver
python -m src.quality.ibge.check_population_silver
python -m src.quality.ibge.check_prf_municipality_match


echo
echo "[7/8] OPEN-METEO"

python -m src.ingestion.open_meteo.accident_weather
python -m src.transform.open_meteo.accident_weather_to_silver
python -m src.quality.open_meteo.check_accident_weather_silver


echo
echo "[8/8] DBT - GOLD + TESTS"

dbt build \
    --project-dir analytics \
    --profiles-dir analytics


echo
echo "============================================================"
echo "PIPELINE COMPLETED SUCCESSFULLY"
echo "============================================================"
