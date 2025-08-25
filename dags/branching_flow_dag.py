from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule


@dag(
    dag_id="branching_refresher_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson", "branching", "reps"]
)
def branching_refresher():

    # Decide which path to run based on a run-config value "n"
    @task.branch
    def pick_branch(**context):
        n_val = int(context["dag_run"].conf.get("n", 0))
        return "process_large" if n_val > 100 else "process_small"
    
    @task
    def process_large():
        print("LARGE PATH")
        return "LARGE_DONE"
    
    @task
    def process_small():
        print("small path")
        return "small_done"
    
    # fan-in join: runs if at least one upstream task succeeded (the other will be skipped)
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def join():
        print("Joined after branch chosen. This is to demonstrate that this works as a control flow post branching.")

    # Branch to both; Airflow will run only the task whose ID matches pick_branch's return
    choice = pick_branch()
    large = process_large()
    small = process_small()

    choice >> [small, large] >> join()

dag = branching_refresher()