from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.utils.context import Context
from airflow.sdk.definitions.connection import AirflowNotFoundException
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log


def success_notification(context):
    try:
        ti = context["task_instance"]
        metadata = ti.xcom_pull(task_ids=ti.task_id, key="metadata")

        details = (
            f"SUCCESS ALERT: {ti.task_id} for CoreTelecoms for {context['ds']} SUCCEEDED\n\n"
            f"Metrics: {metadata}\n\n"
            f"---------------------------------------------\n\n\n"
        )

        notifier = SlackNotifier(slack_conn_id="slack", text=details, channel="dag_alerts")
        notifier.notify(context)
    except AirflowNotFoundException:
        log.warning("Slack connection not configured, skipping notification")


def failure_notification(context):
    try:
        ti = context["task_instance"]
        metadata = ti.xcom_pull(task_ids=ti.task_id, key="metadata")

        details = (
            f"FAILURE ALERT: {ti.task_id} for CoreTelecoms for {context['ds']} FAILED\n\n"
            f"Details: {metadata}\n\n"
            f"---------------------------------------------\n\n\n"
        )

        notifier = SlackNotifier(slack_conn_id="slack", text=details, channel="dag_alerts")
        notifier.notify(context)
    except AirflowNotFoundException:
        log.warning("Slack connection not configured, skipping notification")


def notify_batch_already_complete(
    context: Context, start_batch: int, batches_to_run: int, task_id: str
):
    try:
        details = (
            f"ALERT: {task_id} for CoreTelecoms for {context['ds']} ALREADY SUCCEEDED\n\n"
            f"{start_batch}/{batches_to_run} batches completed in previous run\n\n"
            f"---------------------------------------------\n\n\n"
        )
        notifier = SlackNotifier(slack_conn_id="slack", text=details, channel="dag_alerts")
        notifier.notify(context)
    except AirflowNotFoundException:
        log.warning("Slack connection not configured, skipping notification")

