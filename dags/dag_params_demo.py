from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable, Param
from datetime import datetime

from modules.airflow_defaults import default_args

@dag(
    dag_id="dag_params_demo",
    start_date=datetime(2025, 8, 1),
    schedule=None,
    catchup=False,
    tags=["lesson", "params"],
    description="Demo of DAG and task params usage",
    default_args=default_args(),
    # 👇 Add at least one Param to surface the UI form in 2.9.x
    params = {
        "table_name": Param(
            default=None,
            type="string",
            title="Table Name",
            description="Name of the table to process. If not set, falls back to Variable 'default_table_name'."
        )
    }
)
def dag_params_demo():
    @task
    def get_table_name() -> str:
        """
        Fetch a table name from DAG params, with a default if not provided.
        """
        ctx = get_current_context()

        # 1) UI-provided param (2.9.1 Trigger > Parameters)
        table_name = ctx.get("params", {}).get("table_name")
        if table_name:
            print(f"Using table name from DAG params: {table_name}")
            return table_name
        
        # 2) CLI/API/TriggerDagRunOperator-provided conf
        table_name = (ctx["dag_run"].conf or {}).get("table_name")
        if table_name:
            print(f"Using table name from DAG run conf: {table_name}")
            return table_name
        
        # 3) Varioble fallback
        fallback = Variable.get("default_table_name", default_var="exmple_input")
        print(f"Falling back to Variable for table name: {fallback}")
        return fallback
    
    @task
    def process_table(table_name: str) -> None:
        """
        Dummy processing task that just logs the table name it would process.
        """
        print(f"Processing data from table: {table_name}")

    process_table(get_table_name())

dag_object = dag_params_demo()