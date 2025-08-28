from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from datetime import datetime
from typing import List, Tuple
import logging
from psycopg2.extras import execute_values

from modules.alerts_functions import notify_success
from modules.airflow_defaults import default_args, POSTGRES_CONN_ID

@dag(
    dag_id="postgres_python_notify_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson", "postgres", "notify"],
    default_args=default_args(),
    description="ETL using PostgresHook + Python transform + notify"
)
def postgres_python_notify_dag():
    @task
    def fetch_from_postgres() -> List[Tuple[int, str]]:
        """
        Pull rows from example_input; optionally filter by a Variable.
        Returns a list of (id, value) tuples.
        """
        hook = PostgresHook(POSTGRES_CONN_ID)
        min_id = int(Variable.get("LESSON27_MIN_ID", default_var="4"))
        sql = "SELECT id, value from public.example_input WHERE id >= %s ORDER BY id"
        rows = hook.get_records(sql, parameters=(min_id,))
        if not rows:
            raise ValueError("No rows fetched from example_input (check data or filter).")
        logging.info(f"Retrieved {len(rows)} rows from example_input.")
        return rows
    
    @task
    def transform(rows: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
        """
        Simple transform: uppercase the 'value' column.
        """
        logging.info("Starting transformation of fetched data.")
        transformed = [(row[0], (row[1] or "").upper()) for row in rows]
        logging.info(f"Transformed {len(transformed)} rows.")
        if transformed:
            logging.info(f"First 3 transformed rows: {transformed[:3]}")
        return transformed

    @task
    def upsert_to_postgres(transformed) -> int:
        input_len = len(transformed)
        hook = PostgresHook(POSTGRES_CONN_ID)
        conn = hook.get_conn()
        upsert_sql = """
            INSERT INTO public.example_output (id, value)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value;
        """
        # Using the connection & cursor as context managers:
        # - on success, it COMMITs
        # - on exception, it ROLLBACKs and raises
        with conn, conn.cursor() as cur:
            execute_values(cur, upsert_sql, transformed)
        
        logging.info(f"Upserting {input_len} rows into example_output.")
        return input_len 
    
    @task
    def notify_task(rows_written) -> None:
        # Call helper, pass DAG id + details
        notify_success("postgres_python_notify_dag", extra_msg=f"Rows written: {rows_written}.")

    rows = fetch_from_postgres()
    transformed = transform(rows)
    upserted = upsert_to_postgres(transformed)
    notify_task(upserted)

dag_object = postgres_python_notify_dag()