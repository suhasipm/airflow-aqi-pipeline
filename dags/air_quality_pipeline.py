import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")

from scripts.extract import extract_aqi_data
from scripts.transform import transform_aqi_data
from scripts.load import load_to_postgres

API_TOKEN = os.getenv("AQICN_API_TOKEN")
CITY = os.getenv("AQI_CITY", "bengaluru")

if not API_TOKEN:
    raise RuntimeError("Environment variable AQICN_API_TOKEN is not set. Set it to your AQICN API token.")

DATA_DIR = "/opt/airflow/data"
RAW_CSV_PATH = os.path.join(DATA_DIR, "raw_air_quality.csv")
CLEANED_CSV_PATH = os.path.join(DATA_DIR, "cleaned_air_quality.csv")

default_args = {
    "owner": "suhasi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_task(**context):
    os.makedirs(DATA_DIR, exist_ok=True)
    df = extract_aqi_data(CITY, API_TOKEN)
    df.to_csv(RAW_CSV_PATH, index=False)
    return RAW_CSV_PATH


def transform_task(ti):
    raw_path = ti.xcom_pull(task_ids="extract_aqi")
    transform_aqi_data(df=pd.read_csv(raw_path), output_path=CLEANED_CSV_PATH)
    return CLEANED_CSV_PATH


def load_task(ti):
    cleaned_path = ti.xcom_pull(task_ids="transform_aqi")
    load_to_postgres(cleaned_path)


with DAG(
    dag_id="air_quality_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    doc_md="""
    ### Air Quality ETL

    Extracts the current AQI reading for `CITY` (env var `AQI_CITY`, default
    `bengaluru`) from the AQICN API, cleans it, and upserts it into the
    `air_quality_data` Postgres table.

    Runs once daily. Safe to rerun or backfill — loads are deduplicated on
    `(city, timestamp)` via a unique constraint.
    """,
) as dag:

    extract = PythonOperator(
        task_id="extract_aqi",
        python_callable=extract_task,
        doc_md="Fetch the current AQI reading from the AQICN API and write it to a raw CSV.",
    )

    transform = PythonOperator(
        task_id="transform_aqi",
        python_callable=transform_task,
        doc_md="Clean, validate, and deduplicate the raw AQI reading.",
    )

    load = PythonOperator(
        task_id="load_aqi",
        python_callable=load_task,
        doc_md="Upsert the cleaned reading into PostgreSQL.",
    )

    extract >> transform >> load
