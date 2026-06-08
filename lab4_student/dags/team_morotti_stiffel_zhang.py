"""
Lab 4 - Capstone DAG: team_morotti_stiffel_zhang
Retail KPI pipeline: wait → ingest → validate → compute → publish
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

from include.ingest import ingest_day, validate_silver
from include.paths import report_json
from include.team_morotti_stiffel_zhang_spark import run_daily

DEFAULT_ARGS = {
    "owner": "morotti_stiffel_zhang",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="team_morotti_stiffel_zhang",
    description="Capstone retail KPI pipeline",
    start_date=datetime(2026, 6, 1),
    end_date=datetime(2026, 6, 14),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab4", "capstone"],
) as dag:
    # Task 1 — Wait for the vendor CSV to appear
    wait_csv = FileSensor(
        task_id="wait_for_csv",
        filepath="/opt/airflow/data/incoming/transactions_{{ ds }}.csv",
        poke_interval=15,
        timeout=600,
        mode="poke",
    )

    # Task 2 — Ingest CSV → Silver Parquet
    @task
    def ingest(ds: str = None):
        ingest_day(ds)

    # Task 3 — Validate Silver Parquet (fail visibly if bad)
    @task
    def validate(ds: str = None):
        ok = validate_silver(ds)
        if not ok:
            raise ValueError(f"Silver validation failed for ds={ds}")

    # Task 4 — Run Spark: Silver → Gold Parquet + dashboard JSON
    @task
    def compute_kpis(ds: str = None):
        run_daily(ds)

    # Task 5 — Confirm report exists and log its path
    @task
    def publish_report(ds: str = None):
        path = report_json(ds)
        if not path.exists():
            raise FileNotFoundError(f"Dashboard JSON not found: {path}")
        print(f"[publish] Report ready: {path}")

    # Wire the pipeline
    ingest_task = ingest()
    validate_task = validate()
    compute_task = compute_kpis()
    publish_task = publish_report()

    wait_csv >> ingest_task >> validate_task >> compute_task >> publish_task
