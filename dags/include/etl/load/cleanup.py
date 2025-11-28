from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def clear_all_checkpoints(context):
    """
    Clear all checkpoint variables for this DAG run after successful completion.
    Must be the last task in the DAG.
    """
    from airflow.models import Variable

    execution_date = context["ds"]
    all_variables = Variable.get_all()
    checkpoints_to_clear = [
        key for key in all_variables.keys() if key.endswith(f"_{execution_date}")
    ]

    if not checkpoints_to_clear:
        log.info(f"No checkpoints found for execution date: {execution_date}")
        metadata = {
            "execution_date": execution_date,
            "checkpoints_cleared": 0,
        }
        return metadata

    cleared_count = 0
    for checkpoint_key in checkpoints_to_clear:
        try:
            Variable.delete(checkpoint_key)
            log.info(f"Cleared checkpoint: {checkpoint_key}")
            cleared_count += 1
        except KeyError:
            log.warning(f"✗ Checkpoint not found: {checkpoint_key}")

    log.info(f"Cleanup complete: {cleared_count} checkpoints cleared")
    metadata = {"execution_date": execution_date, "checkpoints_cleared": cleared_count}
    return metadata
