from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.definitions.context import get_current_context
from include.etl.extraction.s3_extractor import S3Extractor
from include.etl.extraction.google_extractor import GoogleSheetsExtractor
from include.config import config


from airflow.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log


@dag(
    dag_id="coretelecoms_dag",
    start_date=datetime(2025, 11, 19),
    catchup=False,
    schedule=None,
    tags=["coretelecoms"]
)


def process_complaint_data():

    @task
    def ingest_customer_data_task():
        context = get_current_context()
        extractor = S3Extractor(context=context)

        return extractor.copy_customers_data()

    @task
    def ingest_call_logs_task():
        context = get_current_context()
        extractor = S3Extractor(context=context)

        return extractor.copy_call_log_data()

    @task
    def ingest_sm_complaint_task():
        context = get_current_context()
        extractor = S3Extractor(context=context)

        return extractor.copy_social_media_complaint_data()

    @task
    def ingest_agents_data_task():
        context = get_current_context()
        extractor = GoogleSheetsExtractor(context=context)

        return extractor.copy_agents_data()
    

    ingest_customer_data = ingest_customer_data_task()
    ingest_call_logs = ingest_call_logs_task()
    ingest_sm_complaint = ingest_sm_complaint_task()
    ingest_agents_data = ingest_agents_data_task()


process_complaint_data()