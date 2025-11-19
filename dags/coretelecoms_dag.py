from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.definitions.context import get_current_context
from include.etl.extraction import Extractor
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
    def extract_customer_data():
        context = get_current_context()
        s3_hook = S3Hook(aws_conn_id="aws_airflow_source_user")

        e = Extractor(context=context, s3_hook=s3_hook)

        return e.extract_customers()


    extract_task = extract_customer_data()


process_complaint_data()