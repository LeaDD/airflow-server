from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from modules.alerts_functions import on_failure_callback, post_to_discord, post_to_telegram
import logging

DEFAULT_ARGS = {
    "owner": "airflow",
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": on_failure_callback
}

@dag(
    dag_id="alerts_demo_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lesson", "alerts"]
)
def alerts_demo_dag():
    @task
    def start_task():
        logging.info("This task will fail on purpose.")

    @task
    def fail_task():
        # print("Succeed in order to test notify_success")
        raise ValueError("Deliberate failure for alert testing.")
    
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def notify_success():
        """
        Sends a 'pipeline completed' message to Discord/Telegram if configured.
        Uses your existing helper functions (best-effort; won't fail the DAG).
        """
        msg = "✅ alerts_demo_dag completed successfully."
        # Discord
        discord_url = Variable.get("DISCORD_WEBHOOK_URL", default_var=None)
        post_to_discord(msg, discord_url)

        # Telegram (optional)
        tg_token = Variable.get("TELEGRAM_BOT_TOKEN", default_var=None)
        tg_chat  = Variable.get("TELEGRAM_CHAT_ID", default_var=None)
        post_to_telegram(msg, tg_token, tg_chat)
        print(msg)

    # Wire the graph: success notifier runs only if both upstream tasks succeed
    s = start_task()
    f = fail_task()
    s >> f >> notify_success()

dag_object = alerts_demo_dag()