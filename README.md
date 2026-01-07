# 🌫️ Air Quality ETL — AQICN → PostgreSQL

This repository contains a small ETL pipeline that extracts air-quality measurements from the AQICN API, cleans and normalizes the data, and loads it into a PostgreSQL table. A lightweight Airflow DAG is included to orchestrate the pipeline when running under Airflow.

**Key components**
- Extraction: `scripts/extract.py` (calls AQICN API)
- Transformation: `scripts/transform.py` (cleans and validates)
- Loading: `scripts/load.py` (inserts into PostgreSQL)
- Orchestration: `dags/air_quality_pipeline.py` (Airflow DAG)
- Tests: `tests/` (unit tests for extract/transform/load)

**Repository layout**
```
.
├── dags/
│   └── air_quality_pipeline.py
├── data/
│   ├── raw_air_quality.csv
│   └── cleaned_air_quality.csv
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Quick setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Environment variables

Set the API token and PostgreSQL connection details. Example names used by the scripts:

- `AQICN_API_TOKEN` — AQICN API token
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

On Windows (Command Prompt):

```bat
set AQICN_API_TOKEN=your_token_here
set POSTGRES_HOST=localhost
set POSTGRES_PORT=5432
set POSTGRES_DB=air_quality
set POSTGRES_USER=postgres
set POSTGRES_PASSWORD=secret
```

On PowerShell:

```powershell
$Env:AQICN_API_TOKEN = 'your_token_here'
```

3. (Optional) Start Airflow via the provided `docker-compose.yml` if you want to run the DAG:

```bash
docker-compose up -d
# Airflow web UI is usually at http://localhost:8080
```

## Usage

You can run the three pipeline stages separately for development and debugging.

Extract (example):

```bash
python scripts/extract.py --output data/raw_air_quality.csv
```

Transform (example):

```bash
python scripts/transform.py --input data/raw_air_quality.csv --output data/cleaned_air_quality.csv
```

Load (example):

```bash
python scripts/load.py --input data/cleaned_air_quality.csv
```

Notes:
- The exact CLI arguments depend on the scripts' implementations; run `python scripts/<script>.py --help` for details.
- The DAG `dags/air_quality_pipeline.py` orchestrates these steps when deployed to Airflow.

## Deployment / Airflow

The repository includes a `docker-compose.yml` that can start an Airflow instance suitable for local testing. Place the DAG in the Airflow DAGs folder, set environment variables, and enable the DAG in the Airflow UI.

## Troubleshooting & notes

- If API calls fail, verify `AQICN_API_TOKEN` and network connectivity.
- If database connections fail, verify Postgres is running and the connection environment variables are correct.
- The pipeline expects CSV files under `data/` for local runs; these are also useful for tests and debugging.

