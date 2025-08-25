from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import random

@dag(
    dag_id="task_group_parallel_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson", "parallel", "task_group"]
)
def task_group_flow():

    @task
    def start():
        print("Running task A")

    @task
    def end():
        print("End of DAG")

    # Define parallel tasks inside a TaskGroup
    with TaskGroup("processing_group") as processing:
        @task()
        def a():
            print("Running task A")

        @task()
        def b():
            print("Running task B")

        @task()
        def c():
            print("Running task C")

        a()
        b()
        c()

    start() >> processing >> end()

dag = task_group_flow()

