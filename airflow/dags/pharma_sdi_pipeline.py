"""
pharma_sdi_pipeline.py  –  Airflow 3.x DAG
Automated DWH + BI Pipeline for Pharmaceutical Cold Supply Chain

Tasks:
  1. load_staging        – CSV → PostgreSQL staging tables
  2. validate_staging    – Row counts & null checks
  3. build_star_schema   – Staging → DuckDB star schema (6 dims + 2 facts)
  4. export_to_postgres  – DuckDB star → PostgreSQL star schema (for Superset)
  5. create_index        – DuckDB performance indexes
  6. run_query_layer     – KPI / analytics queries against the warehouse
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
SCRIPTS = f"{PROJECT}/scripts"

with DAG(
    dag_id="pharma_sdi_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pharma", "sdi", "dwh"],
) as dag:

    load_staging = BashOperator(
        task_id="load_staging",
        bash_command=f"cd {SCRIPTS} && python -I load_staging.py",
    )

    validate_staging = BashOperator(
        task_id="validate_staging",
        bash_command=f"cd {SCRIPTS} && python -I validate_staging.py",
    )

    build_star_schema = BashOperator(
        task_id="build_star_schema",
        bash_command=f"cd {SCRIPTS} && python -I build_star_schema.py",
    )

    export_to_postgres = BashOperator(
        task_id="export_to_postgres",
        bash_command=f"cd {SCRIPTS} && python -I export_to_postgres.py",
    )

    create_index = BashOperator(
        task_id="create_index",
        bash_command=f"cd {SCRIPTS} && python -I create_index.py",
    )

    run_query_layer = BashOperator(
        task_id="run_query_layer",
        bash_command=f"cd {SCRIPTS} && python -I time_queries.py",
    )

    # Pipeline flow
    (
        load_staging
        >> validate_staging
        >> build_star_schema
        >> export_to_postgres
        >> create_index
        >> run_query_layer
    )
