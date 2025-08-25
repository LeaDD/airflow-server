from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
import os
import logging

@dag(
    dag_id="midpoint_project",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson", "project"]
)
def sensor_transform_notify():
    """
    Midpoint project: Wait for file → transform contents → notify user.
    Practices sensor usage, XCom passing, and PythonOperator flow.
    """
    
    @task.sensor(poke_interval=15, timeout=300, mode="poke")
    def wait_for_file():
        file_path = "/opt/airflow/input/data_ready.txt"
        file_exists = os.path.exists(file_path)
        return PokeReturnValue(is_done=file_exists, xcom_value=file_path if file_exists else None) 
    
    @task
    def transform_file(input_path: str):
        output_path = "/opt/airflow/output/transformed.txt"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(input_path, "r") as f:
            content = f.read()
        output = content.upper()
        with open(output_path, "w") as f:
            f.write(output)
        print("✅ File transformed and saved.")

    @task
    def notify_user(file_path: str):
        log = logging.getLogger(__name__)
        log.info(f"✅ File transformed {file_path}")
        print(f"Operation complete for {file_path}")

    file_path = wait_for_file()
    transform_file(file_path) >> notify_user(file_path)

dag = sensor_transform_notify() 

