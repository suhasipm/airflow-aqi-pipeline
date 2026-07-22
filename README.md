# 🌫️ Air Quality ETL — AQICN → PostgreSQL (Airflow)

A small, daily ETL pipeline orchestrated with Apache Airflow:

1. **Extract** — pulls real-time air quality data for Bengaluru from the [AQICN API](https://aqicn.org/api/)
2. **Transform** — cleans, validates, and rounds the readings
3. **Load** — upserts the cleaned data into a PostgreSQL table

## Repository layout

```
.
├── dags/
│   └── air_quality_pipeline.py   # Airflow DAG: extract >> transform >> load
├── scripts/
│   ├── extract.py                # AQICN API client
│   ├── transform.py              # Cleaning/validation
│   └── load.py                   # Postgres loader
├── data/                         # Raw & cleaned CSVs land here (gitignored)
├── docker-compose.yml            # Airflow (standalone) + local Postgres
├── requirements.txt
└── README.md
```

## Prerequisites

- Docker + Docker Compose
- A free [AQICN API token](https://aqicn.org/data-platform/token/)
- A PostgreSQL instance (the included `docker-compose.yml` spins one up for you)

## Environment variables

Create a `.env` file in the repo root:

```bash
AQICN_API_TOKEN=your_token_here

DB_HOST=postgres
DB_PORT=5432
DB_NAME=air_quality
DB_USER=postgres
DB_PASSWORD=postgres
```

> These names must match exactly — `scripts/load.py` reads `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` (not `POSTGRES_*`).

## Running locally

```bash
docker-compose up -d
```

- Airflow UI: http://localhost:8080 (default login `airflow` / `airflow` for standalone mode — check container logs for the generated password)
- Postgres is available on `localhost:5432` for direct inspection (e.g. via `psql` or a GUI client)
- Enable the `air_quality_pipeline` DAG from the UI; it's scheduled `@daily` with `catchup=False`.

## Running stages manually (without Airflow)

Useful for local development/debugging:

```bash
pip install -r requirements.txt

python -c "
from scripts.extract import extract_aqi_data
import os
df = extract_aqi_data('bengaluru', os.getenv('AQICN_API_TOKEN'))
df.to_csv('data/raw_air_quality.csv', index=False)
"

python -c "
import pandas as pd
from scripts.transform import transform_aqi_data
transform_aqi_data(pd.read_csv('data/raw_air_quality.csv'))
"

python -c "
from scripts.load import load_to_postgres
load_to_postgres('data/cleaned_air_quality.csv')
"
```

> Note: none of the scripts currently expose a CLI (`argparse`). The snippets above call the functions directly — a good next step is adding `if __name__ == '__main__':` CLI entry points to each script.

## Design notes

- Each DAG run captures a single real-time snapshot per city (not historical data) — the AQICN feed endpoint returns current readings only.
- Data is deduplicated at the database layer using a `UNIQUE (city, timestamp)` constraint with `ON CONFLICT DO NOTHING`, so reruns/backfills are safe.
- Data file paths are resolved relative to the repo root (not the process's working directory), so the pipeline behaves the same whether run via Airflow or manually.
- `CITY` is currently hardcoded in the DAG; multi-city support can be added via an Airflow `Param`.

## Troubleshooting

- **API errors**: verify `AQICN_API_TOKEN` is valid and has not hit its rate limit.
- **DB connection errors**: confirm the `postgres` service is up (`docker-compose ps`) and the `DB_*` variables match your `.env`.
- **`FileNotFoundError` on `data/...`**: the DAG creates the `data/` directory automatically on load; if running scripts manually, create it yourself first (`mkdir -p data`).

## Roadmap / possible extensions

- Add pytest coverage for `transform_aqi_data`
- Add CLI entry points (`argparse`) to each script
- Parameterize city list for multi-city ingestion
- Add a Grafana/Streamlit dashboard on top of the Postgres table
- Swap `SequentialExecutor` for `LocalExecutor` with a dedicated Postgres metastore for parallel task execution
