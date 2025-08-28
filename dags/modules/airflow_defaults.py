from datetime import timedelta
from modules.alerts_functions import on_failure_callback

def default_args(
    owner: str = "airflow",
    emails = ("alerts@example.com",),
    retries: int = 1,
    retry_delay_seconds: int = 30,
    failure_cb = on_failure_callback,
):
    return {
        "owner": owner,
        "email": list(emails),
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": retries,
        "retry_delay": timedelta(seconds=retry_delay_seconds),
        "on_failure_callback": failure_cb,
    }

POSTGRES_CONN_ID = "my_postgres"