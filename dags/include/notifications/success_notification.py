from airflow.providers.slack.notifications.slack import SlackNotifier


def success_notification(context):
    ti = context["task_instance"]
    metadata = ti.xcom_pull(task_ids=ti.task_id)

    details = (
        f"SUCCESS ALERT: {ti.task_id} for CoreTelecoms ingestion for {context['ds']} SUCCEEDED\n\n"
        f"Metrics: {metadata}\n\n"
        f"---------------------------------------------\n\n\n"
    )

    notifier = SlackNotifier(slack_conn_id="slack", text=details, channel="dag_alerts")
    notifier.notify(context)
