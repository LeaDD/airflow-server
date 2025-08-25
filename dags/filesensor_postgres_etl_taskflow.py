from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.sensors.base import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import csv, os, logging
from pathlib import Path

POSTGRES_CONN_ID = "my_postgres"

@dag(
    dag_id="filesensor_postgres_etl_taskflow",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson", "etl", "filesensor", "postgres"]
)
def filesensor_postgres_etl_taskflow():

    @task
    def read_variable():
        path = Variable.get("data_dir", default_var="/opt/airflow/dags/data/inbox")
        logging.info(f"Data directory variable collected: {path}")
        return path

    @task.sensor(poke_interval=30, timeout=300, mode="reschedule")
    def wait_for_file(path: str) -> PokeReturnValue:
        # Use the logical date folder to keep this reproducible by run
        ctx = get_current_context()
        ds = ctx["ds"] # e.g. 2025-08-24
        file_path = f"{path}/{ds}/ready.csv"
        exists = os.path.exists(file_path)
        if exists:
            logging.info(f"File found: {file_path}")
            return PokeReturnValue(is_done=True, xcom_value=file_path)
        else:
            logging.info(f"File not yet found: {file_path}")
            return PokeReturnValue(is_done=False)

    @task
    def read_file(xcom_path: str):
        path = Path(xcom_path)
        logging.info(f"Reading file: {path}")
        rows = []
        with path.open("r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            # expecting: id, value
            for rec in r:
                rows.append((int(rec["id"]), rec["value"]))
        logging.info(f"Read {len(rows)} rows, first 3: {rows[:3]}")
        return rows
    
    @task
    def transform(data: list[tuple[int, str]]):
        logging.info("Transform: uppercasing 'value'")
        out = [(rid, val.upper()) for (rid, val) in data]
        logging.info(f"Transformed {len(out)} rows; first 3: {out[:3]}")
        return out

    @task
    def upsert_to_postgres(records: list[tuple[int, str]]):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        # Upsert
        sql = """
            INSERT INTO public.example_output (id, value)
            VALUES (%s, %s)
            ON CONFLICT (id)
            DO UPDATE SET value = EXCLUDED.value;
        """
        for row in records:
            logging.info(f"Upserting row: {row}")
            hook.run(sql, parameters=row)

    path = read_variable()
    found_path = wait_for_file(path)
    
    with TaskGroup("processing_group") as processing:
        rows = read_file(found_path)
        out = transform(rows)
        upsert_to_postgres(out)
    

dag_object = filesensor_postgres_etl_taskflow()

        