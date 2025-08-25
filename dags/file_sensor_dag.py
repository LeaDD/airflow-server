from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
import os

@dag(
    dag_id="sensor_file",
    start_date = datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson", "sensor"]
)
def sensor_dag():

    @task.sensor(poke_interval=10, timeout=300, mode="poke")
    def wait_for_file():
        file_path = "/opt/airflow/input/data_ready.txt"
        file_exists = os.path.exists(file_path)
        return PokeReturnValue(is_done=file_exists, xcom_value=file_path if file_exists else None)
    
    @task
    def process(xcom_path: str):
        print(f"✅ File {xcom_path} found. Proceeding with processing...")

    process(wait_for_file())

dag = sensor_dag()