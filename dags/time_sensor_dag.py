from airflow.decorators import dag, task
from airflow.sensors.time_sensor import TimeSensor
from datetime import datetime, time, timedelta
import random

@dag(
    dag_id="sensor_time",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2, 
        "retry_delay": timedelta(seconds=10),
        "sla": timedelta(seconds=15)
    },
    tags=["lesson", "sensor", "sla", "retries"]
)
def time_sla_retry_flow():

    # Wait until after 7am UTC
    wait_until_7am = TimeSensor(
        task_id="wait_until_7am",
        target_time=time(hour=0, minute=24), 
        # making reschedule so that the worker does not remain engaged with this task if condition not met
        mode="reschedule",
        poke_interval=60, # How often to wake up and check condition
        timeout=3600 # Max wait time
    )

    @task
    def flaky_task():
        if random.random() < 0.5:
            raise ValueError("❌ Random failure!")
        print("✅ Task succeeded.")

    wait_until_7am >> flaky_task()

dag = time_sla_retry_flow()