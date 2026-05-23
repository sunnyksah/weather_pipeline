# Weather Pipeline — Airflow + dbt + PostgreSQL + Docker

An end-to-end data engineering pipeline that fetches real weather data daily, loads it into PostgreSQL, and transforms it through a layered dbt model structure. Built with the exact stack modern DE teams use in production.

---

## Architecture

```
Open-Meteo API
      │
      ▼
Airflow DAG (scheduled daily @ 06:00 UTC)
      │
      ├── fetch_weather        → calls Open-Meteo API, pushes to XCom
      ├── load_to_postgres     → writes raw rows to raw.weather_daily
      ├── dbt_run              → builds staging → intermediate → mart models
      └── dbt_test             → validates data quality
            │
            ▼
      PostgreSQL
      ├── raw.weather_daily         (raw ingestion)
      ├── staging.stg_weather       (cleaned & cast)
      ├── intermediate.int_daily_summary  (business logic)
      └── marts.fct_weather_daily   (final wide table)
```

---

## Tech Stack

| Tool | Role |
|---|---|
| Apache Airflow 2.9.1 | Orchestration & scheduling |
| dbt 1.8.0 | SQL transformations |
| PostgreSQL 16 | Data warehouse |
| Docker + Docker Compose | Containerisation |
| Open-Meteo API | Free weather data source |
| Python 3.12 | Ingestion logic |

---

## Project Structure

```
weather_pipeline/
│
├── Dockerfile                          # Custom Airflow image with dbt
├── docker-compose.yml                  # All services defined here
├── requirements.txt
├── README.md
│
├── airflow/
│   └── dags/
│       └── weather_pipeline.py         # Main DAG — all 4 tasks
│
└── dbt/
    └── weather_dbt/
        ├── dbt_project.yml
        ├── profiles.yml                # DB connection config
        ├── models/
        │   ├── staging/
        │   │   └── stg_weather.sql     # Cast + rename raw columns
        │   ├── intermediate/
        │   │   └── int_daily_summary.sql  # Derived metrics + categories
        │   └── marts/
        │       └── fct_weather_daily.sql  # Final wide table + rolling avgs
        └── tests/
            └── assert_temp_not_null.sql
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- 4GB+ RAM available for Docker
- Ports `8080` and `5432` free on your machine

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/weather_pipeline.git
cd weather_pipeline
```

### 2. Build the custom Docker image

```bash
docker compose build
```

This installs Airflow + dbt + all dependencies into the image. Takes 2–3 minutes the first time.

### 3. Initialise the Airflow database

```bash
docker compose up airflow-init
```

Wait for `Admin user admin created` in the output.

### 4. Start all services

```bash
docker compose up -d
```

### 5. Open the Airflow UI

Go to [http://localhost:8080](http://localhost:8080)

```
Username: admin
Password: admin
```

### 6. Trigger the pipeline

1. Find `weather_pipeline` in the DAGs list
2. Toggle it **on**
3. Click **▶ Trigger DAG**
4. Watch all 4 tasks turn green in the Graph view

---

## Verifying the Data

Connect to PostgreSQL:

```bash
docker exec -it weather_pipeline-postgres-1 psql -U airflow
```

Check the raw layer:

```sql
SELECT * FROM raw.weather_daily ORDER BY date DESC LIMIT 5;
```

Check the final mart:

```sql
SELECT weather_date, temperature_avg_c, precipitation_category, temp_avg_7d
FROM marts.fct_weather_daily
LIMIT 5;
```

---

## dbt Model Layers

### `stg_weather` (view)
Casts raw columns to correct types, renames fields, coalesces nulls.

### `int_daily_summary` (view)
Adds derived business logic:
- `temp_range_c` — daily temperature swing
- `precipitation_category` — dry / light_rain / moderate_rain / heavy_rain
- `wind_category` — calm / moderate / strong

### `fct_weather_daily` (table)
Final wide table ready for analytics:
- All fields from intermediate
- Year, month, day-of-week extracted
- Rolling 7-day average temperature (`temp_avg_7d`)
- Rolling 7-day total precipitation (`precip_sum_7d`)

---

## DAG Schedule

The pipeline runs automatically every day at **06:00 UTC**. To change the schedule, update this line in `airflow/dags/weather_pipeline.py`:

```python
schedule_interval="0 6 * * *"   # cron expression
```

---

## Stopping the Pipeline

```bash
docker compose down
```

To also remove all data volumes:

```bash
docker compose down -v
```

---

## What to Add Next

- [ ] Connect Metabase or Grafana for a live dashboard
- [ ] Add more cities to the API call
- [ ] Add `schema.yml` with dbt `not_null` and `unique` tests
- [ ] Set up Airflow email alerts on task failure
- [ ] Swap PostgreSQL for BigQuery (`dbt-bigquery`)

---

## Data Source

[Open-Meteo](https://open-meteo.com/) — free, open-source weather API. No API key required.

---

## License

MIT
