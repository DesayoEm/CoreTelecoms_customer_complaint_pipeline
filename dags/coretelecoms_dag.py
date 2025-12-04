from airflow.sdk import dag, task
from typing import Dict
from airflow.models import Variable
from pendulum import datetime, duration
from airflow.sdk.definitions.context import get_current_context
from include.etl.extraction.s3_extractor import S3Extractor
from include.etl.extraction.google_extractor import GoogleSheetsExtractor
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from include.etl.extraction.sql_extractor import SQLEXtractor
from include.etl.transformation.transformation import Transformer
from include.config import config
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
    "retries": 2,
    "retry_delay": duration(minutes=5),
    "on_success_callback": success_notification,
    "on_failure_callback": failure_notification,
}


@dag(
    dag_id="coretelecoms_dag",
    start_date=datetime(2025, 11, 20),
    end_date=datetime(2025, 11, 23),
    max_active_runs=1,
    catchup=True,
    schedule="@daily",
    default_args=default_args,
    tags=["coretelecoms"],
)
def process_complaint_data():

    @task.branch
    def determine_load_type():
        """Decide whether to run static data loads."""
        is_first_run = Variable.get("coretelecoms_first_run", default_var="true")

        if is_first_run.lower() == "true":
            log.info("First run - agents and customers must load before gate opens")
            return [
                "ingest_customer_data_task",
                "ingest_agents_data_task",
                "static_data_gate",
            ]
        else:
            log.info("Subsequent run - gate opens immediately")
            return ["static_data_gate"]

    @task
    def mark_first_run_complete():
        """
        Mark that first run has completed.
        IF THIS FAILS, CUSTOMERS AND AGENTS WILL LOAD EVERY DAY
        AND REPROCESS ALREADY AVAILABLE DATA
        """
        Variable.set("coretelecoms_first_run", "false")
        log.info(
            "First customer and agents run complete and coretelecoms_first_run set to false"
        )

    # STATIC TASKS - ONLY RUN ON START DATE
    @task(task_id="ingest_customer_data_task")
    def ingest_customer_data_task():
        context = get_current_context()
        extractor = S3Extractor(context=context)
        return extractor.copy_customers_data()

    @task(task_id="ingest_agents_data_task")
    def ingest_agents_data_task():
        context = get_current_context()
        extractor = GoogleSheetsExtractor(context=context)
        return extractor.copy_agents_data()

    @task
    def transform_customers_task(metadata: Dict):
        context = get_current_context()
        transformer = Transformer(context=context)
        return transformer.transform_entity(metadata["destination"], "customers")

    @task
    def transform_agents_task(metadata: Dict):
        context = get_current_context()
        transformer = Transformer(context=context)
        return transformer.transform_entity(metadata["destination"], "agents")

    @task(task_id="static_data_gate", trigger_rule="none_failed_min_one_success")
    def static_data_gate():
        """
        Gate that controls when complaints can be processed.
        On first run: only runs AFTER customers/agents are loaded
        On subsequent runs: runs immediately
        """
        # DUE TO FK CONSTRAINTS, COMPLAINTS MUST NOT LOAD UNTIL THE GATE TASK IS SUCCESSFUL
        # i.e until customers and agents have been processed
        log.info("Static data gate OPEN. Complaints can now be processed")
        return True

    @task
    def load_customers_task():
        context = get_current_context()
        loader = Loader(context=context)
        return loader.load_tables_to_snowflake("customers")

    @task
    def load_agents_task():
        context = get_current_context()
        loader = Loader(context=context)
        return loader.load_tables_to_snowflake(entity_type="agents")

    # DAILY COMPLAINTS RUN DAILY
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
    def truncate_conformance_staging_tables_task():
        return truncate_staging_tables()

    @task
    def transform_call_logs_task(metadata: Dict):
        context = get_current_context()
        transformer = Transformer(context=context)

        return transformer.transform_entity(metadata["destination"], "call logs")

    @task
    def transform_sm_task(metadata: Dict):
        context = get_current_context()
        transformer = Transformer(context=context)

        return transformer.transform_entity(metadata["destination"], "sm complaints")

    @task
    def transform_web_task(metadata: Dict):
        context = get_current_context()
        transformer = Transformer(context=context)

        return transformer.transform_entity(metadata["destination"], "web complaints")


    @task
    def load_call_logs_task():
        context = get_current_context()
        loader = Loader(context=context)
        return loader.load_tables_to_snowflake(entity_type="call logs")

    @task
    def load_sm_complaints_task():
        context = get_current_context()
        loader = Loader(context=context)
        return loader.load_tables_to_snowflake(entity_type="sm complaints")

    @task
    def load_web_complaints_task():
        context = get_current_context()
        loader = Loader(context=context)
        return loader.load_tables_to_snowflake(entity_type="web complaints")

    trigger_dbt = DbtCloudRunJobOperator(
        task_id="run_dbt",
        job_id=config.DBT_JOB_ID,
        dbt_cloud_conn_id="dbt_cloud_default",
        check_interval=10,
        timeout=30000,
    )

    @task(trigger_rule="all_success")
    def cleanup_checkpoints_task():
        """Clear all checkpoints after successful DAG run."""
        context = get_current_context()
        return clear_all_checkpoints(context=context)

    # BRANCH RUNS IN ALL CASES AND DETERMINES DOWNSTREAM PATH
    tables = create_all_tables_task()
    clear_staging = truncate_conformance_staging_tables_task()
    branch = determine_load_type()

    tables >> clear_staging >> branch

    # IF BRANCH determine_load_type DETERMINES THAT STATIC PATH MUST RUN
    raw_customer_data = ingest_customer_data_task()
    raw_agents_data = ingest_agents_data_task()
    mark_complete = mark_first_run_complete()
    open_gate = static_data_gate()

    branch >> [raw_customer_data, raw_agents_data]
    branch >> open_gate

    transform_customers = transform_customers_task(raw_customer_data)
    transform_agents = transform_agents_task(raw_agents_data)

    load_customers_wh = load_customers_task()
    load_agents_wh = load_agents_task()


    [transform_customers, transform_agents] >> load_customers_wh >> load_agents_wh >> mark_complete >> open_gate


    # COMPLAINT INGESTION CAN RUN INDEPENDENTLY
    raw_call_logs = ingest_call_logs_task()
    raw_sm_complaints = ingest_sm_complaints_task()
    raw_web_complaints = ingest_web_complaints_data_task()

    transform_call_logs = transform_call_logs_task(raw_call_logs)
    transform_sm = transform_sm_task(raw_sm_complaints)
    transform_web = transform_web_task(raw_web_complaints)

    # COMPLAINTS CAN ONLY BE TRANSFORMED AFTER GATE OPENS
    open_gate >> [transform_call_logs, transform_sm, transform_web]

    load_call_logs = load_call_logs_task()
    load_sm = load_sm_complaints_task()
    load_web = load_web_complaints_task()

    transform_call_logs >> load_call_logs
    transform_sm >> load_sm
    transform_web >> load_web

    cleanup = cleanup_checkpoints_task()
    [load_call_logs, load_sm, load_web] >> trigger_dbt >> cleanup


process_complaint_data()
