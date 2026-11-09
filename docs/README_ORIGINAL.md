# Ceará Road Safety Data Platform

[![CI](https://github.com/VictorCPena/ceara-road-safety-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/VictorCPena/ceara-road-safety-data-platform/actions/workflows/ci.yml)

End-to-end data engineering and analytics platform for road-safety intelligence in Ceará, Brazil.

The project ingests public road-accident data from the Brazilian Federal Highway Police (PRF), enriches it with municipality and population data from IBGE and historical weather data from Open-Meteo, validates and models the data through a Bronze → Silver → Gold architecture, orchestrates the workflow with Apache Airflow, and exposes the analytical layer through the **Observatório Viário CE** Streamlit application.

---

## What this project answers

The platform is designed to support questions such as:

- Which municipalities concentrate the highest number of accidents, deaths, and severe injuries?
- How do municipalities compare after adjusting for population?
- Which federal highways and 10 km road segments concentrate the most severe events?
- At what hours and days of the week are accidents more frequent?
- Which causes and accident types appear most often together?
- What profiles of people and vehicles are most frequently involved?
- How do observed weather conditions relate to accident occurrence and severity?
- Where are the main spatial hotspots in Ceará?

The analysis is descriptive and exploratory. It does not infer causal risk without an appropriate exposure denominator.

---

## Architecture

```text
PRF / IBGE / Open-Meteo
          │
          ▼
┌──────────────────────────┐
│          Bronze          │
│ immutable raw snapshots  │
│ checksum + manifest      │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│          Silver          │
│ typed / cleaned data     │
│ canonical entities       │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│      Quality Gates       │
│ schema / coverage /      │
│ integrity validation     │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│           Gold           │
│      DuckDB + dbt        │
│ facts / dimensions /     │
│ analytical marts         │
└────────────┬─────────────┘
             │
       ┌─────┴─────┐
       ▼           ▼
 Apache Airflow   Observatório
 orchestration    Viário CE
                  Streamlit
```

---

## Current validation status

The current dbt project contains:

| Metric | Current status |
|---|---:|
| dbt models | 19 |
| dbt data tests | 128 |
| dbt build nodes | 147 |
| CI jobs | Data Platform + Airflow |
| CI runner | Ubuntu 24.04 |

The latest validated build completes with all dbt nodes passing.

---

## Data coverage

### PRF

The ingestion layer currently processes PRF public datasets for:

- 2024
- 2025
- 2026

Raw records processed:

- **194,443 accident occurrences**
- **523,621 person-involvement records**

For Ceará, the analytical layer currently contains:

- **3,559 accidents**
- **184 IBGE municipalities**
- municipality matching between PRF and IBGE
- weather enrichment for georeferenced Ceará accidents

> 2026 is a partial year in the current dataset and must not be compared with full years without considering the observation window.

### IBGE

IBGE is used for:

- municipality identifiers
- municipality names
- regional metadata
- population
- municipal GeoJSON boundaries

### Open-Meteo

Open-Meteo is used to enrich accident events with historical weather information such as:

- temperature
- relative humidity
- precipitation
- visibility
- wind speed
- weather classification

---

## Gold analytical layer

The Gold layer is built with dbt on DuckDB.

### Core dimensions and facts

- `dim_date`
- `dim_municipality`
- `fct_accident`
- `fct_person_involvement`
- `fct_vehicle_involvement`
- `fct_municipality_population`
- `fct_accident_weather`
- `bridge_accident_cause`
- `bridge_accident_type`

### Analytical marts

- `mart_municipality_road_safety_yearly`
- `mart_weather_road_safety`
- `mart_weather_road_safety_pt`
- `mart_accident_time_patterns`
- `mart_highway_safety`
- `mart_highway_segment_safety`
- `mart_accident_cause_type`
- `mart_vehicle_road_safety`
- `mart_vehicle_make_model_safety`
- `mart_accident_map`

These marts move analytical logic out of the presentation layer and make the dashboard consume curated business-ready datasets.

---

## Observatório Viário CE

The repository includes a Streamlit analytical product called **Observatório Viário CE**.

The interface is organized into five areas.

### 1. Visão geral

Executive view with:

- accident KPIs
- deaths and severe injuries
- annual evolution
- vector map
- major signals in the selected slice

### 2. Território

Combines spatial analyses that were previously separated:

- municipal choropleth
- municipality comparison
- population-adjusted rates
- federal-highway ranking
- critical 10 km highway segments
- spatial hotspots

The map uses a local Ceará municipal GeoJSON and Altair/Vega-Lite rather than depending on external tile services.

### 3. Ocorrências

Focuses on how and when accidents happen:

- accident causes
- accident types
- cause × type combinations
- day phase
- hour × weekday patterns
- weather conditions
- precipitation context

### 4. Envolvidos

Profiles people and vehicles:

- sex
- age groups
- person type
- physical condition
- vehicle type
- manufacturer
- model
- vehicle age
- deaths and serious injuries by vehicle type

Vehicle manufacturer/model data is normalized before presentation and missing values remain traceable instead of being silently discarded.

### 5. Dados & metodologia

Documents:

- sources
- architecture
- limitations
- analytical assumptions
- technology stack

---

## Vector map

The map layer is designed as a lightweight analytical visualization rather than a generic web map.

```text
IBGE Ceará GeoJSON
       +
Gold analytical data
       │
       ▼
join by ibge_code
       │
       ▼
Altair / Vega-Lite
├── municipal choropleth
├── highway-segment hotspots
└── optional individual accidents
```

The municipal geometry is stored locally at:

```text
dashboard/assets/ceara_municipalities.geojson
```

If it needs to be regenerated:

```bash
python scripts/download_ceara_geojson.py
```

---

## Vehicle normalization

PRF source data can contain manufacturer and model in the same raw value, for example:

```text
HONDA/CG 160 FAN
VW/GOL 1.0
```

The Gold layer separates and normalizes them into:

```text
vehicle_make   vehicle_model
Honda          CG 160 FAN
Volkswagen     GOL 1.0
```

Unknown values remain represented as missing data and are excluded only from rankings where treating them as a manufacturer/model would be misleading.

---

## Data-quality strategy

Quality checks cover multiple levels.

### Dimensional integrity

Examples:

- municipality key relationships
- unique accident keys
- unique involvement keys
- unique population keys

### Domain validation

Examples:

- valid weather groups
- valid year status
- valid temporal ranges
- valid highway-segment boundaries
- reasonable vehicle ages

### Coverage validation

Examples:

- municipality matching
- accident-weather coverage
- map coordinate quality

### Analytical validation

Examples:

- non-negative metrics
- one primary cause per accident
- accident-type ordering
- municipality-year row expectations

---

## Orchestration

Apache Airflow orchestrates the end-to-end data workflow.

Current local architecture:

```text
PostgreSQL
    │
    ▼
Airflow
├── API Server
├── Scheduler
├── DAG Processor
└── Triggerer
    │
    ▼
Extraction → Silver → Quality → dbt Gold
```

The main DAG is:

```text
ceara_road_safety_full_pipeline
```

Heavy ETL work is constrained through an Airflow pool to avoid local resource exhaustion.

---

## Continuous integration

GitHub Actions validates both the data platform and Airflow environment.

The workflow performs checks such as:

- Docker image build
- Python syntax validation
- deterministic CI fixture generation
- dbt build against fixture data
- Airflow image build
- DAG import validation
- expected DAG validation

The workflow is pinned to:

```yaml
runs-on: ubuntu-24.04
```

to avoid unexpected changes from the moving `ubuntu-latest` label.

---

## Technology stack

### Data engineering

- Python
- pandas
- PyArrow
- Parquet
- DuckDB
- dbt
- PostgreSQL
- Apache Airflow

### Data sources and enrichment

- PRF public datasets
- IBGE
- Open-Meteo

### Product / visualization

- Streamlit
- Plotly
- Altair
- Vega-Lite
- GeoJSON

### Infrastructure

- Docker
- Docker Compose
- GitHub Actions

---

## Repository structure

```text
.
├── analytics/
│   ├── models/
│   │   └── gold/
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── dashboard/
│   ├── app.py
│   └── assets/
│       └── ceara_municipalities.geojson
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── warehouse/
│
├── dags/
├── metadata/
│
├── scripts/
│   ├── run_pipeline.sh
│   ├── create_ci_fixtures.py
│   └── download_ceara_geojson.py
│
├── src/
├── tests/
│   └── fixtures/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── Dockerfile
├── Dockerfile.dashboard
├── docker-compose.yml
├── requirements.txt
└── requirements-dashboard.txt
```

Generated data and local warehouse files are intentionally excluded from Git.

---

## Run locally

### Prerequisites

- Python 3.11+
- Docker
- Docker Compose

On macOS, this project has also been tested using Colima as the Docker runtime.

### 1. Clone

```bash
git clone https://github.com/VictorCPena/ceara-road-safety-data-platform.git
cd ceara-road-safety-data-platform
```

### 2. Create a Python environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dashboard dependencies

```bash
pip install -r requirements-dashboard.txt
```

### 4. Build the data platform image

```bash
docker compose build pipeline
```

### 5. Run dbt validation

```bash
make dbt
```

### 6. Run the dashboard

```bash
streamlit run dashboard/app.py
```

Then open:

```text
http://localhost:8501
```

---

## Rebuilding after analytical changes

The pipeline Docker image copies the `analytics/` directory during build.

Therefore, after changing dbt models locally, rebuild the image before validating:

```bash
docker compose build pipeline
make dbt
```

This prevents local tests from accidentally running an older dbt project embedded in a stale Docker image.

---

## Important analytical limitations

This project intentionally keeps its claims conservative.

- PRF data represents accidents recorded on **Brazilian federal highways**.
- It does not represent every traffic accident in Ceará.
- 2026 is currently a partial observation period.
- Vehicle counts do not provide a fleet-exposure denominator.
- A high number of accidents involving a vehicle type does not mean that vehicle type is intrinsically more dangerous.
- Highway-segment hotspots represent observed concentration, not causal risk.
- Weather association does not establish causality.
- Cause and accident-type relationships can be many-to-many.

---

## Next phase

The local data platform, automated tests, CI pipeline, and analytical product are complete.

The next infrastructure phase is deployment to an Oracle Cloud Free Tier environment, including:

```text
Oracle VM
├── Docker
├── PostgreSQL
├── Airflow
├── data pipeline
├── DuckDB Gold
└── Streamlit
```

The public dashboard will be exposed separately from the administrative Airflow interface.

---

## Author

**Victor Pena**

Data Science / Data Engineering

GitHub: [VictorCPena](https://github.com/VictorCPena)
