from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context
from datetime import datetime
import logging
from typing import List, Tuple

from modules.alerts_functions import notify_success 
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
        def fetch_from_postgres() -> List[Tuple[int, str]]:
            """
            Use PostgresHook.get_records (returns List[Tuple]) and RETURN the rows for downstream tasks.
            """
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            sql = "SELECT * FROM public.example_input"
            rows = hook.get_records(sql)
            if not rows:
                logging.warning("No rows found in public.example_input table.")
                return []
            
            logging.info(f"Fetched {len(rows)} rows from public.example_input.")
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
        def write_to_file(transformed: List[Tuple[int, str]]) -> str:
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
        
        @task
        def report(file_path: str, transformed: List[Tuple[int, str]]) -> None:
            """
            Simple logging task to confirm file write.
            """
            if not file_path:
                logging.error("No file path provided to report task.")
                return
            row_count = len(transformed)
            logging.info(f"[REPORT] DAG {get_current_context()['dag'].dag_id} wrote {row_count} rows to {file_path}")
        
        rows = fetch_from_postgres()
        transformed = transform(rows)
        file_path = write_to_file(transformed)
        report(file_path, transformed)

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def notify_completion():
        """
        Call helper, pass dag_id + details
        """
        notify_success("task_group_etl", "All processing tasks completed successfully.")

    processing >> notify_completion()


dag_object = task_group_etl()


                