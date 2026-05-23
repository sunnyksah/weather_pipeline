from datetime import datetime, timedelta
import requests
import psycopg2
import json
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Connection config ────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

# ── Task 1: Fetch from Open-Meteo ────────────────────────────────────────────
def fetch_weather(**context):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=27.7172&longitude=85.3240"   # Kathmandu
        "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
        "&timezone=Asia%2FKathmandu"
        "&past_days=7"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    context["ti"].xcom_push(key="weather_raw", value=data)
    print(f"Fetched {len(data['daily']['time'])} days of weather data")


# ── Task 2: Load raw rows into Postgres ──────────────────────────────────────
def load_to_postgres(**context):
    data = context["ti"].xcom_pull(key="weather_raw", task_ids="fetch_weather")
    daily = data["daily"]

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;

        CREATE TABLE IF NOT EXISTS raw.weather_daily (
            date              DATE,
            temp_max          NUMERIC(5,2),
            temp_min          NUMERIC(5,2),
            precipitation_mm  NUMERIC(6,2),
            windspeed_max     NUMERIC(6,2),
            loaded_at         TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (date)
        );
    """)

    rows = list(zip(
        daily["time"],
        daily["temperature_2m_max"],
        daily["temperature_2m_min"],
        daily["precipitation_sum"],
        daily["windspeed_10m_max"],
    ))

    cur.executemany("""
        INSERT INTO raw.weather_daily
            (date, temp_max, temp_min, precipitation_mm, windspeed_max)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
            temp_max         = EXCLUDED.temp_max,
            temp_min         = EXCLUDED.temp_min,
            precipitation_mm = EXCLUDED.precipitation_mm,
            windspeed_max    = EXCLUDED.windspeed_max,
            loaded_at        = NOW();
    """, rows)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(rows)} rows into raw.weather_daily")


# ── Task 3: Run dbt transformations ─────────────────────────────────────────
def run_dbt(**context):
    result = subprocess.run(
        ["dbt", "run", "--project-dir", "/opt/airflow/dbt/weather_dbt",
         "--profiles-dir", "/opt/airflow/dbt/weather_dbt"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run failed:\n{result.stderr}")


def run_dbt_tests(**context):
    result = subprocess.run(
        ["dbt", "test", "--project-dir", "/opt/airflow/dbt/weather_dbt",
         "--profiles-dir", "/opt/airflow/dbt/weather_dbt"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt test failed:\n{result.stderr}")


# ── DAG definition ───────────────────────────────────────────────────────────
default_args = {
    "owner": "de-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="Fetch Open-Meteo → Postgres raw → dbt transforms",
    schedule_interval="0 6 * * *",   # Daily at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "dbt"],
) as dag:

    t1 = PythonOperator(task_id="fetch_weather",    python_callable=fetch_weather)
    t2 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    t3 = PythonOperator(task_id="dbt_run",          python_callable=run_dbt)
    t4 = PythonOperator(task_id="dbt_test",         python_callable=run_dbt_tests)

    t1 >> t2 >> t3 >> t4