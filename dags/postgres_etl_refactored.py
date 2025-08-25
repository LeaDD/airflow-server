from airflow.decorators import dag, task
from datetime import datetime
from modules import postgres_etl_functions as etl

@dag(
    dag_id="postgres_etl_refactored",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson", "postgres", "etl"]
)
def postgres_etl_refactored():
    @task
    def ensure_output_table():
        return etl.ensure_output_table()

    @task
    def fetch_data():
        return etl.fetch_data()
    
    @task
    def transform(data):
        return etl.transform(data)
    
    @task
    def write_data(transformed):
        return etl.write_data(transformed)

    ensure_output_table()
    rows = fetch_data()
    transformed = transform(rows)
    write_data(transformed)

dag_object = postgres_etl_refactored()