from datetime import datetime
import logging

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from modules.db import fetch_all, upsert_values
from modules.airflow_defaults import default_args
from modules.transforms import clean_rows
from modules.sql import UPSERT_EXAMPLE_OUTPUT

@dag(
    dag_id="final_project",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args=default_args(),
    tags=["lesson", "etl", "postgres"],
    doc_md="""
    Final refactored ETL DAG:
    - Reads from Postgres
    - Transforms data via Python module
    - Writes back with UPSERT
    - Includes logging, retries, and clean structure
    """
)
def final_project():
    @task
    def extract():
        sql = "SELECT * FROM public.example_input;"
        rows = fetch_all(sql)
        logging.info("Fetched %d rows of type %s", len(rows), type(rows))
        return rows
    
    @task 
    def transform(rows: list[dict]):
        return clean_rows(rows)
    
    @task
    def load(transformed: list[dict]):
        sql_count = "SELECT COUNT(*) FROM public.example_output;"
        before_count = fetch_all(sql_count, conn_id="my_postgres")[0][0]
        # transformed is a list of tuples (id, value)
        upsert_values(UPSERT_EXAMPLE_OUTPUT, transformed) # type: ignore
        after_count = fetch_all(sql_count, conn_id="my_postgres")[0][0]
        count = after_count - before_count
        logging.info("Wrote %d rows to output table", count)
        return count

    data = extract()
    transformed = transform(data) # type: ignore
    load(transformed) # type: ignore

dag_object = final_project()