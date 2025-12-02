from typing import Dict
from airflow.utils.context import Context
from airflow.providers.slack.notifications.slack import SlackNotifier


def persist_ingestion_metadata_before_failure(
    error: Exception, context: Context, metadata: Dict
) -> None:
    info = {
        "task": context["task_instance"].task_id,
        "execution_date": metadata.get("execution_date"),
    }

    for key, value in metadata.items():
        info[key] = value

    extra_info = {k: v for k, v in info.items() if k not in ["task", "execution_date"]}

    details = (
        f"FAILURE ALERT: {info['task']} for CoreTelecoms ingestion for {info['execution_date']} FAILED\n\n"
        f"extra_info: {extra_info}\n\n"
        f"ERROR: {error}"
        f"-----------------------------------\n\n\n"
    )

    notifier = SlackNotifier(slack_conn_id="slack", text=details, channel="dag_alerts")
    notifier.notify(context)
    raise error
