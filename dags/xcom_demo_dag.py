from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="xcom_demo_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson", "xcom"]
)
def xcom_demo_dag():

    @task
    def generate_filename():
        from datetime import date
        return f"/data/output/{date.today()}.csv"
    
    @task
    def use_filename(filename: str):
        print(f"Received filename from upstream: {filename}")

    @task
    def manual_pull(**kwargs):
        val = kwargs['ti'].xcom_pull(task_ids='generate_filename')
        print(f"Pulled manually: {val}")


    filename = generate_filename()
    use_filename(filename)
    manual_pull()

xcom_demo_dag()

