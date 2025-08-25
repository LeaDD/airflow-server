from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime

@dag(
    dag_id="variable_demo",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson", "variables"]
)
def variable_demo():

    @task
    def read_variable():
        path = Variable.get("data_file_path")
        print(f"Path from variable: {path}")
        return path
    
    read_variable()

dag_instance = variable_demo()
