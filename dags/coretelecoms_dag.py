from airflow.sdk import dag, task
from pendulum import datetime
from airflow.sdk.definitions.context import get_current_context
from include.etl.extraction.s3_extractor import S3Extractor
from include.etl.extraction.google_extractor import GoogleSheetsExtractor
from include.etl.extraction.sql_extractor import SQLEXtractor
from include.etl.transformation.transformation import Transformer
from include.notifications.notifications import (
    success_notification,
    failure_notification,
)
from include.etl.load.load import Loader
from include.etl.load.ddl_scripts.create_tables import create_all_tables
from include.etl.load.cleanup import clear_all_checkpoints
from include.etl.load.ddl_scripts.truncate_staging import truncate_staging_tables


from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

default_args = {
    # "retries": 2,
    # "retry_delay": 10,
    "on_success_callback": success_notification,
    "on_failure_callback": failure_notification,
}


@dag(
    dag_id="coretelecoms_dag",
    start_date=datetime(2025, 11, 19),
    catchup=False,
    schedule=None,
    default_args=default_args,
    tags=["coretelecoms"],
)
def process_complaint_data():

    @task
    def ingest_customer_data_task():
        context = get_current_context()
        extractor = S3Extractor(context=context)

        return extractor.copy_customers_data()

    @task
    def ingest_agents_data_task():
        context = get_current_context()
        extractor = GoogleSheetsExtractor(context=context)

        return extractor.copy_agents_data()

    @task
    def ingest_call_logs_task():
        context = get_current_context()
        extractor = S3Extractor(context=context)

        return extractor.copy_call_log_data()

    @task
    def ingest_sm_complaints_task():
        context = get_current_context()
        extractor = S3Extractor(context=context)

        return extractor.copy_social_media_complaint_data()

    @task
    def ingest_web_complaints_data_task():
        context = get_current_context()
        extractor = SQLEXtractor(context=context)

        return extractor.copy_web_complaints()

    @task
    def create_all_tables_task():
        return create_all_tables()

    @task
    def truncate_staging_tables_task():
        return truncate_staging_tables()

    @task
    def transform_and_load_customers_task(metadata):
        context = get_current_context()
        transformer = Transformer(context=context)

        return transformer.transform_and_load_entity(
            metadata["destination"], "customers"
        )

    @task
    def transform_and_load_agents_task(metadata):
        context = get_current_context()
        transformer = Transformer(context=context)

        return transformer.transform_and_load_entity(metadata["destination"], "agents")

    @task
    def transform_and_load_call_logs_task(metadata):
        context = get_current_context()
        transformer = Transformer(context=context)

        return transformer.transform_and_load_entity(
            metadata["destination"], "call logs"
        )

    @task
    def transform_and_load_sm_complaints_task(metadata):
        context = get_current_context()
        transformer = Transformer(context=context)

        return transformer.transform_and_load_entity(
            metadata["destination"], "sm complaints"
        )

    @task
    def transform_and_load_web_complaints_task(metadata):
        context = get_current_context()
        transformer = Transformer(context=context)

        return transformer.transform_and_load_entity(
            metadata["destination"], "web complaints"
        )

    @task(trigger_rule="all_success")
    def cleanup_checkpoints_task():
        """Clear all checkpoints after successful DAG run."""
        context = get_current_context()
        return clear_all_checkpoints(context=context)

    @task
    def load_customers_to_wh_task():
        context = get_current_context()
        loader = Loader(context=context)
        return loader.load_tables_to_snowflake("customers")

    @task
    def load_agents_to_wh_task():
        context = get_current_context()
        loader = Loader(context=context)
        return loader.load_tables_to_snowflake(entity_type="agents")


    @task
    def load_call_logs_to_wh_task():
        context = get_current_context()
        loader = Loader(context=context)
        return loader.load_tables_to_snowflake(entity_type="call logs")

    @task
    def load_sm_complaints_to_wh_task():
        context = get_current_context()
        loader = Loader(context=context)
        return loader.load_tables_to_snowflake(entity_type="sm complaints")

    @task
    def load_web_complaints_to_wh_task():
        context = get_current_context()
        loader = Loader(context=context)
        return loader.load_tables_to_snowflake(entity_type="web complaints")


    # raw_customer_data = ingest_customer_data_task()
    # raw_agents_data = ingest_agents_data_task()
    # raw_call_logs = ingest_call_logs_task()
    # raw_sm_complaints = ingest_sm_complaints_task()
    # raw_web_complaints_data = ingest_web_complaints_data_task()
    #
    # tables = create_all_tables_task()
    # clear_staging = truncate_staging_tables_task()
    #
    # transform_and_load_customers = transform_and_load_customers_task(raw_customer_data)
    # transform_and_load_agents = transform_and_load_agents_task(raw_agents_data)
    #
    # tables >> clear_staging >> [transform_and_load_customers, transform_and_load_agents]
    #
    # transform_and_load_call_logs = transform_and_load_call_logs_task(raw_call_logs)
    # transform_and_load_sm_complaints = transform_and_load_sm_complaints_task(
    #     raw_sm_complaints
    # )
    # transform_and_load_web_complaints = transform_and_load_web_complaints_task(
    #     raw_web_complaints_data
    # )
    #
    # downstream_tasks = [
    #     transform_and_load_call_logs,
    #     transform_and_load_sm_complaints,
    #     transform_and_load_web_complaints,
    # ]
    #
    # for upstream in [transform_and_load_customers, transform_and_load_agents]:
    #     for downstream in downstream_tasks:
    #         upstream >> downstream
    #
    # cleanup = cleanup_checkpoints_task()
    # downstream_tasks >> cleanup

    load_customers_to_wh = load_customers_to_wh_task()
    load_agents_to_wh = load_agents_to_wh_task()
    load_call_logs_to_wh = load_call_logs_to_wh_task()
    load_sm_complaints_to_wh = load_sm_complaints_to_wh_task()
    load_web_complaints_to_wh = load_web_complaints_to_wh_task()


process_complaint_data()
