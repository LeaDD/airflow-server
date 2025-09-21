from datetime import datetime
from typing import List, Dict, Any
import logging
import json

from airflow.decorators import dag, task
from airflow.models import Variable

from modules.airflow_defaults import default_args

@dag(
    dag_id="dynamic_task_mapping_demo",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    tags=["lesson", "mapping", "fanout-fanin"],
    default_args=default_args(),
    doc_md="""
    Demo of dynamic task mapping:
    - Get a list of table names (from Variable or default)
    - Map a processing task over that list (one TI per table)
    - Aggregate results in a fan-in summarize task
    """
)
def dynamic_task_mapping_demo():
    @task
    def get_table_list() -> List[str]:
        """
        Read table names from an airflow Variable `table_list` (JSON array).
        Falls back to ["example_input", "example_output"] if unset or invalid.
        """
        raw = Variable.get(
            "tables_list",
            default_var='["example_input", "example_output"]'
        )
        try:
            tables = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(tables, list) or not all(isinstance(t, str) for t in tables):
                raise ValueError("table_list must be a JSON array of strings")
        except Exception as e:
            logging.warning(f"Invalid table_list Variable {e}. Using default.")
            tables = ["example_input", "example_output"]

        logging.info(f"Tables: {tables}")
        return tables
    
    @task
    def process_table(table_name: str) -> Dict[str, Any]:
        """
        Replace with real work (e.g. row counts, integrity checks, light transforms)
        """
        logging.info(f"Processing table: {table_name}")
        # --- demo work here (stub) ---
        # e.g., query row count, validate schema, etc
        return {"table": table_name, "status": "ok"}
    
    @task
    def summarize(results: List[Dict[str, Any]]) -> None:
        ok = sum(1 for r in results if r.get("status") == "ok")
        logging.info(f"Processed {len(results)} tables. OK={ok} Details={results}")

    table_list = get_table_list()
    # Fan-out: create one task instance per table at run time
    per_table_results = process_table.expand(table_name=table_list)
    # Fan-in: gather mapped results and summarize
    summarize(per_table_results)

dag_object = dynamic_task_mapping_demo()
            