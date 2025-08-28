from typing import Optional, Any, Dict
import requests
import traceback
import logging
from airflow.models import Variable

log = logging.getLogger("airflow.task")

def post_to_discord(text: str, webhook_url: Optional[str]) -> None:
    """
    Post a simple text message to a Discord channel via webhook.
    If requests or webhook_url is not available, do nothing.
    """
    if not webhook_url or not requests:
        return
    try:
        # Discord expects JSON with a "content" field for simple messages
        requests.post(webhook_url, json={"content": text}, timeout=5)
    except Exception:
        # Never raise from the alert path-avoid masking the original task failure
        pass

def post_to_telegram(text: str, bot_token: Optional[str], chat_id: Optional[str]) -> None:
    """
    Send a message via Telegram bot API to a specific chat_id.
    If requests or credentials aren't available, do nothing.
    """
    if not bot_token or not chat_id or not requests:
        return
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        requests.post(url, data={"chat_id": chat_id, "text": text}, timeout=5)
    except Exception:
        pass

def format_failure_message(ctx: Dict[str, Any]) -> str:
    """
    Produce a compact, informative message for alerting channels.
    Includes DAG/task identifiers, run id, try number, log URL, and a short traceback.
    """
    ti = ctx.get("task_instance")
    dag_id = ctx.get("dag").dag_id if ctx.get("dag") else "unknown_dag"
    task_id = ti.task_id if ti else "unknown_task"
    run_id = ctx.get("run_id", "unknown_run_id")
    try_number = getattr(ti, "try_number", "n/a")
    log_url = getattr(ti, "log_url", "n/a")

    exc = ctx.get("exception")
    exc_str = f"{exc!r}" if exc else "No exception object on context."
    tb = "".join(traceback.format_tb(ctx.get("exception_traceback"))) if ctx.get("exception_traceback") else "No traceback."

    # Trim traceback for chat channels
    tb_snippet = tb[-1500:] if isinstance(tb, str) else str(tb)

    return (
        f"🚨 Airflow Task Failure\n"
        f"DAG: `{dag_id}`\n"
        f"Task: `{task_id}`\n"
        f"Run ID: `{run_id}`\n"
        f"Try #: `{try_number}`\n"
        f"Log: {log_url}\n"
        f"Exception: ```{exc_str}```\n"
        f"Traceback (tail):\n```{tb_snippet}```"
    )

def on_failure_callback(ctx: Dict[str, Any]) -> None:
    """
    DAG-level failure callback: logs rich context and (optionally) posts to Discord/Telegram.
    Fires on task failure (typically after retries complete, depending on your config).
    """
    msg = format_failure_message(ctx)
    log.error(msg)

    # Optional alert channels via Airflow Variables (Admin -> Variables)
    discord_url = Variable.get("DISCORD_WEBHOOK_URL", default_var=None)
    tg_token = Variable.get("TELEGRAM_BOT_TOKEN", default_var=None)
    tg_chat = Variable.get("TELEGRAM_CHAT_ID", default_var=None) 

    post_to_discord(msg, discord_url)
    post_to_telegram(msg, tg_token, tg_chat) 

def notify_success(dag_id: str, extra_msg: Optional[str] = None) -> None:
    """
    Sends a 'pipeline completed' message to Discord/Telegram if configured.
    Uses your existing helper functions (best-effort; won't fail the DAG).
    """
    msg = f"✅ {dag_id} completed successfully."
    if extra_msg:
        msg += f" - {extra_msg}"

    # Discord
    discord_url = Variable.get("DISCORD_WEBHOOK_URL", default_var=None)
    post_to_discord(msg, discord_url)

    # Telegram (optional)
    tg_token = Variable.get("TELEGRAM_BOT_TOKEN", default_var=None)
    tg_chat  = Variable.get("TELEGRAM_CHAT_ID", default_var=None)
    post_to_telegram(msg, tg_token, tg_chat)
    log.info(msg)  
