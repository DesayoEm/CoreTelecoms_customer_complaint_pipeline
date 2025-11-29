from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.variable import Variable

log = LoggingMixin().log


def clear_all_checkpoints(context):
    """
    Clear all checkpoint variables for this DAG run after successful completion.
    Must be the last task in the DAG.
    """
    execution_date = context["ds"]
    dag = context["dag"]

    task_ids = [task.task_id for task in dag.tasks]

    log.info(f"Starting checkpoint cleanup for {execution_date}")
    log.info(f"DAG has {len(task_ids)} tasks")
    log.info(f"-------------")

    cleared_count = 0
    not_found_count = 0

    for task_id in task_ids:
        checkpoint_key = f"{task_id}_{execution_date}"
        try:
            Variable.delete(checkpoint_key)
            log.info(f"Cleared checkpoint: {checkpoint_key}")
            cleared_count += 1
        except KeyError:
            log.debug(f"No checkpoint for: {checkpoint_key}")
            not_found_count += 1
        except Exception as e:
            log.error(f"Failed to clear {checkpoint_key}: {e}")

    log.info(f"total tasks: {len(task_ids)}")
    log.info(f"Cleared: {cleared_count}")
    log.info(f"Not found: {not_found_count}")

    return {
        "execution_date": execution_date,
        "checkpoints_cleared": cleared_count,
        "checkpoints_not_found": not_found_count,
        "total_tasks": len(task_ids),
    }
