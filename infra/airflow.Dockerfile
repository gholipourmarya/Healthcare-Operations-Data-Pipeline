FROM apache/airflow:2.10.5-python3.11

RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.9.10 dbt-postgres==1.9.0

COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/
COPY --chown=airflow:root analytics/ /opt/airflow/analytics/
