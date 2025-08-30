from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from datetime import datetime
import logging
from typing import List, Tuple

from modules.airflow_defaults import default_args, POSTGRES_CONN_ID

@dag(
    dag_id="task_group_etl",
    start_date=datetime(2025, 8, 1),
    schedule=None,
    catchup=False,
    default_args=default_args(),
    tags=["lesson", "taskgroup", "postgres"],
    description="Week 4 concepts re-inforcement. Build from memory as much as possible."
)
def task_group_etl():
    with TaskGroup("processing_group") as processing:
        @task
        def fetch_from_postgres() -> List[Tuple[int, str]]: # type: ignore
            """
            Use PostgresHook.get_records (returns List[Tuple]) and RETURN the rows for downstream tasks.
            """
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            sql = "SELECT * FROM public.example_input"
            rows = hook.get_records(sql)
            if rows:
                logging.info(f"Selected {len(rows)} rows from public.example_input")
            else:
                logging.info("Selected 0 rows from public.example_input")
            
            return rows

        @task
        def transform(rows: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
            """
            Uppercase the value column; handle None defensively.
            """
            transformed = [(row[0],( row[1] or "").upper()) for row in rows]
            logging.info(f"{len(transformed)} rows transformed and returned.")
            return transformed
        
        @task
        def write_to_file(transformed: List[Tuple[int, str]]) -> str: # type: ignore
            """
            Write transformed rows to a dated file. Jinja doesn't render inside Python strings,
            so grab ds from runtime context instead of using '/tmp/output_{{ ds }}.txt'.
            """
            ds = get_current_context()["ds"]
            out_path = f"/tmp/output_{ ds }.txt"
            with open(out_path, "w") as f:
                for _id, val in transformed:
                    f.write(f"{_id}\t{val}\n")
            logging.info(f"Wrote {len(transformed)} rows to {out_path}")
            return out_path
        
        rows = fetch_from_postgres()
        transformed = transform(rows)
        write_to_file(transformed)

dag_object = task_group_etl()


                