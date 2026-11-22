"""
phase 1:
load_staging -> validate_staging -> build_star_schema -> create_index -> run_query_layer

Every task calls one script from  <project>/scripts  and fails if the script fails, so a bad
step stops everything after it. The scripts are safe to run again (build_star_schema.py only adds
what is new), so re-running the DAG doesnt duplicate data.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

SCRIPTS = "/opt/airflow/project/scripts"
ENV = {
    "WAREHOUSE_DB": f"{SCRIPTS}/warehouse.duckdb",   # all tasks use the same DuckDB file
    "DATA_DIR": "/opt/airflow/project/data",         # the 3 source CSV files
}

with DAG(
    dag_id="pharma_sdi_pipeline",
    description="CSV -> Postgres staging -> validate -> DuckDB star schema -> index -> queries",
    start_date=datetime(2026, 11, 1),
    schedule=None,                 # manual trigger (the refresh batches are triggered by hand)
    catchup=False,
    max_active_runs=1,             # never two runs on the same warehouse file
    default_args={"owner": "sammy", "retries": 0, "execution_timeout": timedelta(minutes=30)},
    params={
        "mode": Param("replace", enum=["replace", "append"],
                      description="replace = reload staging from scratch, append = add a refresh batch"),
        "batch_id": Param("initial", type="string", description="name of this load, e.g. initial, refresh1"),
        "data_dir": Param("", type="string", description="folder with the 3 CSV files (empty = project/data)"),
    },
    tags=["pharma", "dwh", "phase1"],
    doc_md=__doc__,
) as dag:

    load_staging = BashOperator(
        task_id="load_staging",
        bash_command=(f"cd {SCRIPTS} && python load_staging.py --mode {{{{ params.mode }}}} "
                      "--batch-id {{ params.batch_id }}"
                      "{% if params.data_dir %} --data-dir {{ params.data_dir }}{% endif %}"),
        append_env=True, env=ENV,
    )
    validate_staging = BashOperator(
        task_id="validate_staging",
        bash_command=f"cd {SCRIPTS} && python validate_staging.py --batch-id {{{{ params.batch_id }}}}",
        append_env=True, env=ENV,
    )
    build_star_schema = BashOperator(
        task_id="build_star_schema",
        bash_command=f"cd {SCRIPTS} && python fix_gtin.py && python build_star_schema.py",
        append_env=True, env=ENV,
    )
    create_index = BashOperator(
        task_id="create_index",
        bash_command=f"cd {SCRIPTS} && python create_index.py",
        append_env=True, env=ENV,
    )
    run_query_layer = BashOperator(
        task_id="run_query_layer",
        bash_command=f"cd {SCRIPTS} && python time_queries.py",
        append_env=True, env=ENV,
    )

    load_staging >> validate_staging >> build_star_schema >> create_index >> run_query_layer
