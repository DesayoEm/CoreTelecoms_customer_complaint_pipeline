from typing import Dict, Tuple, Any
import io
import json
from datetime import datetime
import pandas as pd
import hashlib

from include.config import config
from include.etl.transformation.data_cleaning import Cleaner
from include.etl.transformation.data_quality import DataQualityChecker
from include.exceptions.exceptions import DataLoadError

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


class Transformer:
    def __init__(self, context: Dict, s3_dest_hook: S3Hook = None):
        self.context = context
        self.s3_dest_hook = s3_dest_hook or S3Hook(aws_conn_id="aws_airflow_dest_user")
        self.cleaner = Cleaner()
        self.dq_checker = DataQualityChecker(self.cleaner)

    @staticmethod
    def apply_transformation(series, func, parallel_threshold=100000):
        if len(series) > parallel_threshold:
            return series.parallel_apply(func)
        return series.apply(func)

    @staticmethod
    def generate_key(*args) -> str:
        """Generates a deterministic surrogate key from input values."""
        combined = "|".join(str(arg) for arg in args if arg is not None)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reusable column standardization"""
        df.columns = [self.cleaner.standardize_column_name(col) for col in df.columns]
        return df

    def upload_problematic_records_to_s3(
        self, data: pd.DataFrame, source: str, dest_bucket: str, dest_key: str
    ) -> str:
        """converts Dict into Parquet and uploads to S3."""
        buffer = io.BytesIO()
        row_count = len(data)

        data.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
        file_size_bytes = buffer.tell()
        buffer.seek(0)

        dest_key = f"{dest_key}.parquet"
        self.s3_dest_hook.load_file_obj(
            file_obj=buffer, key=dest_key, bucket_name=dest_bucket, replace=True
        )

        location = f"s3://{dest_bucket}/{dest_key}"

        log.info(
            f"Successfully uploaded to {location}"
            f"({row_count} rows, {file_size_bytes / 1024 / 1024:.2f} MB)"
        )

        manifest = {
            "data_file": dest_key,
            "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "metrics": {"row_count": row_count, "file_size_bytes": file_size_bytes},
            "lineage": {
                "data_type": "problematic data",
                "source": source,
                "dag_id": self.context["dag"].dag_id,
                "run_id": self.context["run_id"],
                "execution_date": self.context["ds"],
            },
        }

        manifest_key = dest_key.replace(".parquet", "-manifest.json")
        self.s3_dest_hook.load_string(
            string_data=json.dumps(manifest, indent=2),
            key=manifest_key,
            bucket_name=dest_bucket,
            replace=True,
        )

        log.info(
            f"problematic data manifest saved to s3://{dest_bucket}/{manifest_key}"
        )

        return location

    @staticmethod
    def remove_duplicates(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """Reusable deduplication"""
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            df.drop_duplicates(inplace=True)

        return df, duplicate_count

    @staticmethod
    def add_key_as_first_column(df: pd.DataFrame, key_col: str) -> pd.DataFrame:
        """Move key column to first position"""
        return df[[key_col] + [col for col in df.columns if col != key_col]]

    def transform_and_load_customers(self, customer_data: str) -> Dict[str, Any]:
        try:
            df_customers = pd.read_parquet(customer_data)
        except Exception as e:
            raise DataLoadError(error=e, obj_type="customer data")

        df_customers, duplicate_count = self.remove_duplicates(df_customers)
        df_customers = self.standardize_columns(df_customers)

        df_customers["customer_key"] = self.apply_transformation(
            df_customers["customer_id"], self.generate_key
        )
        df_customers = self.add_key_as_first_column(df_customers, "customer_key")

        # customer_key needs to be in the DataFrame, so customers with bad values are identified
        # downstream after bad values are cleaned/nulled

        problematic_records = self.dq_checker.identify_problematic_customers(
            df_customers
        )

        df_customers["name"] = self.apply_transformation(
            df_customers["name"], self.cleaner.standardize_name
        )
        df_customers["gender"] = self.apply_transformation(
            df_customers["gender"], self.cleaner.validate_gender
        )
        df_customers["email"] = self.apply_transformation(
            df_customers["email"], self.cleaner.clean_email
        )
        df_customers["zip_code"] = self.apply_transformation(
            df_customers["address"], self.cleaner.extract_zip_code
        )
        df_customers["state_code"] = self.apply_transformation(
            df_customers["address"], self.cleaner.extract_state_code
        )
        df_customers["state"] = self.apply_transformation(
            df_customers["state_code"], self.cleaner.generate_state
        )

        problematic_record_location = None
        if not problematic_records.empty:
            problematic_record_location = self.upload_problematic_records_to_s3(
                data=problematic_records,
                source=customer_data,
                dest_bucket=config.BRONZE_BUCKET,
                dest_key=f"{config.PROBLEMATIC_DATA_OBJ_PREFIX}/customers-problematic-{self.context['ds']}",
            )

        metadata = {
            "rows_processed": len(df_customers),
            "duplicates_removed": duplicate_count,
            "problematic_rows_excluded": len(problematic_records),
            "problematic_data_location": (
                problematic_record_location
                if problematic_record_location
                else "No problematic data points"
            ),
        }
        # load df here. too large to return
        # to be implemented later

        return metadata

    def transform_and_load_agents(self, agent_data: str) -> Dict[str, Any]:
        """Transforms and loads agent data. Does not require parallelization as
        agent data is predictably small and static"""
        try:
            df_agents = pd.read_parquet(agent_data)
        except Exception as e:
            raise DataLoadError(error=e, obj_type="agent data")

        df_agents, duplicate_count = self.remove_duplicates(df_agents)
        df_agents = self.standardize_columns(df_agents)

        df_agents["agent_key"] = df_agents["id"].apply(self.generate_key)
        df_agents = self.add_key_as_first_column(df_agents, "agent_key")

        # problematic records are only read after generating key
        problematic_records = self.dq_checker.identify_problematic_agents(df_agents)

        df_agents["name"] = df_agents["name"].apply(self.cleaner.standardize_name)
        df_agents["experience"] = df_agents["experience"].apply(
            self.cleaner.validate_experience_level
        )
        df_agents["state"] = df_agents["state"].apply(self.cleaner.validate_state)

        problematic_record_location = None
        if problematic_records:
            problematic_record_location = self.upload_problematic_records_to_s3(
                data=problematic_records,
                source=agent_data,
                dest_bucket=config.BRONZE_BUCKET,
                dest_key=f"{config.PROBLEMATIC_DATA_OBJ_PREFIX}/agents-problematic-{self.context['ds']}",
            )

        metadata = {
            "rows_processed": len(df_agents),
            "duplicates_removed": duplicate_count,
            "problematic_rows_excluded": len(problematic_records),
            "problematic_data_location": (
                problematic_record_location
                if problematic_record_location
                else "No problematic data points"
            ),
        }
        # load df here. too large to return
        # to be implemented later

        return metadata

    def transform_and_load_web_complaints(
        self, web_complaints_data: str
    ) -> Dict[str, Any]:
        try:
            df_complaints = pd.read_parquet(web_complaints_data)
        except Exception as e:
            raise DataLoadError(error=e, obj_type="web complaint data")

        df_complaints, duplicate_count = self.remove_duplicates(df_complaints)
        df_complaints = self.standardize_columns(df_complaints)

        df_complaints["web_complaint_key"] = (
            df_complaints["request_id"].astype(str)
            + "|"
            + df_complaints["resolution_status"].astype(str)
        ).apply(lambda x: hashlib.md5(x.encode()).hexdigest()[:16])

        df_complaints = self.add_key_as_first_column(df_complaints, "web_complaint_key")

        # problematic records are only read after generating key
        problematic_records = self.dq_checker.identify_problematic_web_complaints(
            df_complaints
        )

        df_complaints["complaint_category"] = self.apply_transformation(
            df_complaints["complaint_category"],
            self.cleaner.validate_complaint_category,
        )
        df_complaints["resolution_status"] = self.apply_transformation(
            df_complaints["resolution_status"], self.cleaner.validate_resolution_status
        )

        problematic_record_location = None
        if not problematic_records.empty:
            problematic_record_location = self.upload_problematic_records_to_s3(
                data=problematic_records,
                source=web_complaints_data,
                dest_bucket=config.BRONZE_BUCKET,
                dest_key=f"{config.PROBLEMATIC_DATA_OBJ_PREFIX}/web-complaints-problematic-{self.context['ds']}",
            )

        metadata = {
            "rows_processed": len(df_complaints),
            "duplicates_removed": duplicate_count,
            "problematic_rows_excluded": len(problematic_records),
            "problematic_data_location": (
                problematic_record_location
                if problematic_record_location
                else "No problematic data points"
            ),
        }
        # load df here. too large to return
        # to be implemented later

        return metadata

    def transform_and_load_sm_complaints(
        self, sm_complaints_data: str
    ) -> Dict[str, Any]:
        try:
            df_complaints = pd.read_parquet(sm_complaints_data)
        except Exception as e:
            raise DataLoadError(error=e, obj_type="Social media complaint data")

        df_complaints, duplicate_count = self.remove_duplicates(df_complaints)
        df_complaints = self.standardize_columns(df_complaints)

        df_complaints["sm_complaint_key"] = (
            df_complaints["request_id"].astype(str)
            + "|"
            + df_complaints["resolution_status"].astype(str)
        ).apply(lambda x: hashlib.md5(x.encode()).hexdigest()[:16])

        df_complaints = self.add_key_as_first_column(df_complaints, "sm_complaint_key")

        # problematic records are only read after generating key
        problematic_records = self.dq_checker.identify_problematic_sm_complaints(
            df_complaints
        )

        df_complaints["complaint_category"] = self.apply_transformation(
            df_complaints["complaint_category"],
            self.cleaner.validate_complaint_category,
        )
        df_complaints["resolution_status"] = self.apply_transformation(
            df_complaints["resolution_status"], self.cleaner.validate_resolution_status
        )
        df_complaints["media_channel"] = self.apply_transformation(
            df_complaints["media_channel"], self.cleaner.validate_media_channel
        )

        problematic_record_location = None
        if not problematic_records.empty:
            problematic_record_location = self.upload_problematic_records_to_s3(
                data=problematic_records,
                source=sm_complaints_data,
                dest_bucket=config.BRONZE_BUCKET,
                dest_key=f"{config.PROBLEMATIC_DATA_OBJ_PREFIX}/sm-complaints-problematic-{self.context['ds']}",
            )

        metadata = {
            "rows_processed": len(df_complaints),
            "duplicates_removed": duplicate_count,
            "problematic_rows_excluded": len(problematic_records),
            "problematic_data_location": (
                problematic_record_location
                if problematic_record_location
                else "No problematic data points"
            ),
        }
        # load df here. too large to return
        # to be implemented later

        return metadata

    def transform_and_load_call_logs(self, call_logs: str) -> Dict[str, Any]:
        try:
            df_call_logs = pd.read_parquet(call_logs)
        except Exception as e:
            raise DataLoadError(error=e, obj_type="Call logs")

        df_call_logs = df_call_logs.drop(columns=["unnamed_0"])
        df_call_logs, duplicate_count = self.remove_duplicates(df_call_logs)
        df_call_logs = self.standardize_columns(df_call_logs)

        df_call_logs["call_log_key"] = (
            df_call_logs["request_id"].astype(str)
            + "|"
            + df_call_logs["resolution_status"].astype(str)
        ).apply(lambda x: hashlib.md5(x.encode()).hexdigest()[:16])

        # problematic records are only read after generating key
        problematic_records = self.dq_checker.identify_problematic_call_logs(
            df_call_logs
        )

        df_call_logs["complaint_category"] = self.apply_transformation(
            df_call_logs["complaint_category"], self.cleaner.validate_complaint_category
        )
        df_call_logs["resolution_status"] = self.apply_transformation(
            df_call_logs["resolution_status"], self.cleaner.validate_resolution_status
        )

        problematic_record_location = None
        if not problematic_records.empty:
            problematic_record_location = self.upload_problematic_records_to_s3(
                data=problematic_records,
                source=call_logs,
                dest_bucket=config.BRONZE_BUCKET,
                dest_key=f"{config.PROBLEMATIC_DATA_OBJ_PREFIX}/call-logs-problematic-{self.context['ds']}",
            )

        metadata = {
            "rows_processed": len(df_call_logs),
            "duplicates_removed": duplicate_count,
            "problematic_rows_excluded": len(problematic_records),
            "problematic_data_location": (
                problematic_record_location
                if problematic_record_location
                else "No problematic data points"
            ),
        }
        # load df here. too large to return
        # to be implemented later

        return metadata
