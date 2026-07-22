from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sys
import os

sys.path.append("/opt/airflow")

from scripts.extract import extract_aqi_data
from scripts.transform import transform_aqi_data
from scripts.load import load_to_postgres

API_TOKEN = os.getenv("AQICN_API_TOKEN")
CITY = "bengaluru"

if not API_TOKEN:
    raise RuntimeError("Environment variable AQICN_API_TOKEN is not set. Set it to your AQICN API token.")

# Resolve data paths relative to the repo root (parent of this dags/ folder),
# so file locations don't depend on Airflow's working directory at runtime.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

RAW_CSV_PATH = os.path.join(DATA_DIR, "raw_air_quality.csv")
CLEANED_CSV_PATH = os.path.join(DATA_DIR, "cleaned_air_quality.csv")

default_args = {
    'owner': 'suhasi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_task(**context):
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
    ### Air Quality ETL Pipeline

    Daily pipeline that fetches real-time AQI data for Bengaluru from the AQICN API,
    cleans it, and loads it into PostgreSQL. Deduplicated on (city, timestamp).
    """,
) as dag:

    extract = PythonOperator(
        task_id="extract_aqi",
        python_callable=extract_task,
        doc_md="Fetch current AQI reading from the AQICN API and write it to a raw CSV.",
    )

    transform = PythonOperator(
        task_id="transform_aqi",
        python_callable=transform_task,
        doc_md="Clean, validate, and round the raw AQI data; write cleaned CSV.",
    )

    load = PythonOperator(
        task_id="load_aqi",
        python_callable=load_task,
        doc_md="Upsert the cleaned AQI data into the air_quality_data Postgres table.",
    )

    extract >> transform >> load
