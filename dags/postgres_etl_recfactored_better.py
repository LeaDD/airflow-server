from datetime import datetime
import logging

from airflow.decorators import dag, task
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from modules.db import fetch_all, run_sql, upsert_values
from modules import sql


@dag(
    dag_id="postgres_etl_refactored_better",
    start_date=datetime(2025, 9, 2),
    schedule="@once",
    catchup=False,
    tags=["lesson", "postgres", "etl"],
    doc_md="""
    ## Postgres ETL (refactored)
    - Helper logic in `modules/` for clean imports and reuse
    - SQL strings centralized in `modules/sql.py`
    - Pure transform in `modules/transforms.py` for easy unit testing

    **Assumptions**
    - Airflow Connection: `my_postgres` is configured
    - `dags/` volume is mounted into the webserver/scheduler/worker containers
    """
)
def postgres_etl_refactored_better():
    @task
    def ensure_output_table():
        run_sql(sql.CREATE_EXAMPLE_OUTPUT, conn_id="my_postgres")

    @task
    def fetch_data():
        records = fetch_all(sql.SELECT_EXAMPLE_INPUT, conn_id="my_postgres")

        logging.info(f"Fetched {len(records)} rows from example input.")
        if records:
            logging.info(f"First 3 rows: {records[:3]}")

        return records
    
    @task
    def transform(data):
        logging.info("Starting transformation of fetched data.")
        transformed = [(row[0], row[1].upper()) for row in data]
        logging.info(f"Transformed {len(transformed)} rows.")
        if transformed:
            logging.info(f"First 3 transformed rows: {transformed[:3]}")
        return transformed
    
    @task
    def write_data(transformed):
        # transformed is a list of tuples (id, value)
        upsert_values(sql.UPSERT_EXAMPLE_OUTPUT, transformed, conn_id="my_postgres")

    trigger_downstream = TriggerDagRunOperator(
        task_id="trigger_downstream",
        trigger_dag_id="downstream_wait_for_postgres_etl",
        conf={"reason": "upstream done"},
        wait_for_completion=False,
    ) 

    ensure_output_table()
    rows = fetch_data()
    transformed = transform(rows)
    write_data(transformed) >> trigger_downstream

dag_object = postgres_etl_refactored_better()

