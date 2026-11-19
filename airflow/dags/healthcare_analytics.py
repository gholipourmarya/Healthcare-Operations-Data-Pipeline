from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="healthcare_analytics",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["healthcare", "dbt"],
) as dag:
    BashOperator(
        task_id="build_analytics",
        bash_command=(
            "/opt/airflow/dbt-venv/bin/dbt build "
            "--project-dir /opt/airflow/analytics "
            "--profiles-dir /opt/airflow/analytics "
            "--target-path /tmp/healthcare-dbt-target"
        ),
        env={"DBT_LOG_PATH": "/tmp/healthcare-dbt-logs"},
        append_env=True,
        execution_timeout=timedelta(minutes=4),
    )
