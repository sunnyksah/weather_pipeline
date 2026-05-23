FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    requests \
    psycopg2-binary==2.9.9 \
    logbook \
    dbt-core==1.8.0 \
    dbt-postgres==1.8.0 \
    dbt-adapters \
    dbt-common \
    agate