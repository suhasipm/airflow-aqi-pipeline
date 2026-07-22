import pandas as pd
from psycopg import connect
from psycopg.rows import dict_row
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Read DB config from environment
db_config = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS air_quality_data (
    city TEXT,
    timestamp TIMESTAMP,
    aqi FLOAT,
    pm25 FLOAT,
    pm10 FLOAT,
    temperature FLOAT,
    humidity FLOAT,
    UNIQUE (city, timestamp)
);
"""

INSERT_QUERY = """
INSERT INTO air_quality_data (city, timestamp, aqi, pm25, pm10, temperature, humidity)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (city, timestamp) DO NOTHING;
"""

def load_to_postgres(csv_path):
    df = pd.read_csv(csv_path)

    conn = connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
        row_factory=dict_row
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_TABLE_QUERY)
            for _, row in df.iterrows():
                cursor.execute(
                    INSERT_QUERY,
                    (
                        row["city"],
                        row["timestamp"],
                        row["aqi"],
                        row["pm25"],
                        row["pm10"],
                        row["temperature"],
                        row["humidity"]
                    )
                )
        conn.commit()
        print("Data loaded successfully into PostgreSQL.")
    finally:
        conn.close()
