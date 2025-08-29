from datetime import datetime
import logging
from typing import List, Tuple

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context  # <-- correct import
from airflow.hooks.postgres_hook import PostgresHook

from modules.airflow_defaults import default_args, POSTGRES_CONN_ID

@dag(
    dag_id="partition_demo_dag",
    start_date=datetime(2025, 8, 1),
    schedule="@daily",           # one run per day (one data interval per run)
    catchup=False,               # flip to True later if you want backfills
    default_args=default_args(),
    tags=["lesson", "intervals", "demo"],
    description="Log data_interval_start/end and do a simple COUNT(*) with a PostgresHook",
)
def partition_demo_dag():
    @task
    def log_interval() -> None:
        """
        Show what 'data interval' this run is responsible for.
        Keeping it simple: plain str() and .date() only.
        """
        ctx = get_current_context()
        start = ctx["data_interval_start"]  # tz-aware datetime
        end   = ctx["data_interval_end"]
        logging.info(f"[partition_demo] data_interval_start = {start}")
        logging.info(f"[partition_demo] data_interval_end   = {end}")
        logging.info(f"[partition_demo] start as date only  = {start.date()}")

    @task
    def count_example_input() -> int:
        """
        Simple COUNT(*) — no templating, no timestamps, no new concepts.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records(
            "SELECT COUNT(*) FROM public.example_input"
        )       
        count = rows[0][0] if rows else 0
        logging.info(f"[partition_demo] example_input row count ] {count}")
        return count
    
    log_interval() >> count_example_input() # type: ignore

dag_object = partition_demo_dag()