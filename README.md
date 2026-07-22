# 🌫️ Air Quality ETL — AQICN → PostgreSQL (Airflow)

A small, daily ETL pipeline orchestrated with Apache Airflow:

1. **Extract** — pulls real-time air quality data for Bengaluru from the [AQICN API](https://aqicn.org/api/)
2. **Transform** — cleans, validates, and deduplicates the readings
3. **Load** — upserts the cleaned data into a PostgreSQL table

## Repository layout

```
.
├── dags/
│   └── air_quality_pipeline.py   # Airflow DAG: extract >> transform >> load
├── scripts/
│   ├── extract.py                # AQICN API client
│   ├── transform.py              # Cleaning/validation/dedup
│   └── load.py                   # Postgres loader (upsert)
├── data/                         # Raw & cleaned CSVs land here (gitignored)
├── docker-compose.yml            # Airflow (standalone) + local Postgres
├── requirements.txt
└── README.md
```

## Prerequisites

- Docker + Docker Compose
- A free [AQICN API token](https://aqicn.org/data-platform/token/)

The included `docker-compose.yml` now spins up its own local Postgres instance, so no external database is required to try this out.

## Environment variables

Create a `.env` file in the repo root:

```bash
AQICN_API_TOKEN=your_token_here
AQI_CITY=bengaluru

DB_HOST=postgres
DB_PORT=5432
DB_NAME=air_quality
DB_USER=postgres
DB_PASSWORD=postgres
```

> These names must match exactly — `scripts/load.py` reads `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`.

## Running locally

```bash
docker-compose up -d
```

- Airflow UI: http://localhost:8080 (standalone mode prints an auto-generated admin password to the container logs on first run: `docker-compose logs airflow`)
- Enable the `air_quality_pipeline` DAG from the UI; it's scheduled `@daily` with `catchup=False`.

## Running stages manually (without Airflow)

Useful for local development/debugging:

```bash
pip install -r requirements.txt

python -c "
from scripts.extract import extract_aqi_data
import os
df = extract_aqi_data(os.getenv('AQI_CITY', 'bengaluru'), os.getenv('AQICN_API_TOKEN'))
df.to_csv('data/raw_air_quality.csv', index=False)
"

python -c "
import pandas as pd
from scripts.transform import transform_aqi_data
transform_aqi_data(pd.read_csv('data/raw_air_quality.csv'), output_path='data/cleaned_air_quality.csv')
"

python -c "
from scripts.load import load_to_postgres
load_to_postgres('data/cleaned_air_quality.csv')
"
```

> None of the scripts expose a CLI (`argparse`) yet — the snippets above call the functions directly. Adding `if __name__ == '__main__':` entry points to each script is a natural next step.

## Design notes

- Each DAG run captures a single real-time snapshot per city (not historical data) — the AQICN feed endpoint returns current readings only.
- Data is deduplicated both in `transform.py` (drops duplicate `(city, timestamp)` rows in-memory) and at the database layer (`UNIQUE (city, timestamp)` + `ON CONFLICT DO NOTHING`), so reruns/backfills are safe.
- `CITY` is configurable via the `AQI_CITY` environment variable (defaults to `bengaluru`).

## Troubleshooting

- **API errors**: verify `AQICN_API_TOKEN` is valid and you haven't hit its rate limit.
- **DB connection errors**: confirm the `postgres` service is up (`docker-compose ps`) and the `DB_*` variables match your `.env`.
- **`FileNotFoundError` on `data/...`**: the extract/transform tasks now create `data/` automatically if it's missing.

## Roadmap / possible extensions

- Add pytest coverage for `transform_aqi_data`
- Add CLI entry points to each script
- Parameterize a *list* of cities for multi-city ingestion
- Add a Grafana/Streamlit dashboard on top of the Postgres table
- Swap `SequentialExecutor` for `LocalExecutor` with a dedicated Postgres metastore for parallel task execution
