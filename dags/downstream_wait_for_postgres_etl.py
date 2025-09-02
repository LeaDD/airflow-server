from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import get_current_context

from modules.airflow_defaults import default_args, POSTGRES_CONN_ID

@dag(
    dag_id="downstream_wait_for_postgres_etl",
    start_date=datetime(2025, 9, 2),
    schedule="@once",
    catchup=False,
    tags=["lesson", "postgres", "etl", "downstream"],
    doc_md="""
    ## Downstream DAG waiting for Postgres ETL
    - Uses `ExternalTaskSensor` to wait for the completion of the `postgres_etl_refactored_better` DAG.
    - Demonstrates inter-DAG dependencies in Airflow.           
    **Assumptions**
    - The `postgres_etl_refactored_better` DAG is defined and runs successfully.
    - Airflow Connection: `my_postgres` is configured.
    - `dags/` volume is mounted into the webserver/scheduler/worker containers
    """,
    default_args=default_args()
)
def downstream_wait_for_postgres_etl():
    @task
    def continue_processing():
        ctx = get_current_context()
        logging.info("Triggered by producer with conf: %s", (ctx["dag_run"].conf or {}))
        logging.info("Continuing downstream processing...")   

    continue_processing()

dag_obj = downstream_wait_for_postgres_etl()

