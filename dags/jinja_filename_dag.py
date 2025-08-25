from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="jinja_filename_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["lesson", "jinja"]
)
def jinja_filename_dag():

    @task
    def print_filename(execution_date: str):
        print(f"Generated filename: /data/input/{execution_date}.csv")

    # Must pass Jinja string when calling
    print_filename(execution_date="{{ ds }}")

dag_object = jinja_filename_dag()
