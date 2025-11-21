from typing import Dict


def persist_ingestion_metadata_before_failure(error: Exception, context: Dict, metadata: Dict) -> None:
    info = {
        "task": context["task_instance"].task_id,
        "execution_date": metadata.get("execution_date"),
    }

    for key, value in metadata.items():
        info[key] = value

    extra_info = {k: v for k, v in info.items() if k not in ["task", "execution_date"]}

    details = (
        f"{info['task']} for CoreTelecoms ingestion for {info['execution_date']} FAILED\n"
        f"extra_info: {extra_info}\n\n"
        f"ERROR: {error}"
    )

    # notifier = SlackNotifier(
    #     slack_conn_id='slack',
    #     text=details,
    #     channel='CoreTelecoms'
    # )
    # notifier.notify(context)
    raise error


def persist_metadata_before_exit(context: Dict, metadata: Dict) -> None:
    info = {
        "task": context["task_instance"].task_id,
        "execution_date": metadata.get("execution_date"),
    }
    for key, value in metadata.items():
        info[key] = value

    ti = context["task_instance"]
    ti.xcom_push(key=f"{info['task']} results", value=info)

    extra_info = {k: v for k, v in info.items() if k not in ["task", "execution_date"]}

    details = (
        f"{info['task']} for CoreTelecoms ingestion for {info['execution_date']} FAILED\n"
        f"info: {extra_info}\n\n"
    )

    # notifier = SlackNotifier(
    #     slack_conn_id='slack',
    #     text=details,
    #     channel='CoreTelecoms'
    # )
    # notifier.notify(context)
