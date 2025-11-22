from typing import Dict, Tuple, Any
import io
import json
from datetime import datetime
import pandas as pd
import hashlib

from include.config import config
from include.etl.transformation.data_cleaning import Cleaner
from include.exceptions.exceptions import DataLoadError

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


class Transformer:
    def __init__(self, context: Dict, s3_dest_hook: S3Hook = None):
        self.context = context
        self.s3_dest_hook = s3_dest_hook or S3Hook(aws_conn_id="aws_airflow_dest_user")

        self.cleaner = Cleaner()

    @staticmethod
    def generate_key(*args) -> str:
        """Generate a deterministic surrogate key from input values."""
        combined = "|".join(str(arg) for arg in args if arg is not None)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reusable column standardization"""
        df.columns = [self.cleaner.standardize_column_name(col) for col in df.columns]
        return df

    def upload_problematic_data_to_s3(
        self, data: Dict, source: str, dest_bucket: str, dest_key: str
    ) -> str:
        """Converts Dict into Parquet and uploads to S3."""
        buffer = io.BytesIO()
        df = pd.DataFrame(data)
        row_count = len(df)

        df.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
        file_size_bytes = buffer.tell()
        buffer.seek(0)

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

        manifest_key = dest_key.replace(".parquet", "_manifest.json")
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

    def clean_and_load_customers(self, customer_data: str) -> Dict[str, Any]:
        try:
            df_customers = pd.read_parquet(customer_data)
        except Exception as e:
            raise DataLoadError(error=e, obj_type="customer data")

        df_customers, duplicate_count = self.remove_duplicates(df_customers)
        df_customers = self.standardize_columns(df_customers)

        df_customers["customer_key"] = df_customers["customer_id"].apply(
            self.generate_key
        )
        df_customers = self.add_key_as_first_column(df_customers, "customer_key")

        df_customers["name"] = df_customers["name"].apply(self.cleaner.standardize_name)
        df_customers["gender"] = df_customers["gender"].apply(
            self.cleaner.validate_gender
        )
        df_customers["email"] = df_customers["email"].apply(self.cleaner.clean_email)
        df_customers["zip_code"] = df_customers["address"].apply(
            self.cleaner.extract_zip_code
        )
        df_customers["state_code"] = df_customers["address"].apply(
            self.cleaner.extract_state_code
        )
        df_customers["state"] = df_customers["state_code"].apply(
            self.cleaner.extract_state
        )

        problematic_data = self.cleaner.problematic_data

        location = None
        if len(problematic_data) > 0:
            location = self.upload_problematic_data_to_s3(
                data=problematic_data,
                source=customer_data,
                dest_bucket=config.BRONZE_BUCKET,
                dest_key=f"{config.PROBLEMATIC_DATA_OBJ_PREFIX}/customers-problematic-{self.context['ds']}",
            )

        metadata = {
            "rows_processed": len(df_customers),
            "duplicates_removed": duplicate_count,
            "null_emails": df_customers["email"].isna().sum(),
            "problematic_data_points": problematic_data,
            "problematic_data_location": (
                location if location else "No problematic data points"
            ),
        }
        # load df here. too large to return
        # to be implemented later

        return metadata

    def clean_agents(self, agent_data: str) -> pd.DataFrame:
        df_agents = pd.read_parquet(agent_data)

        clean_col_names = []
        for col in df_agents.columns:
            clean_col_name = self.cleaner.standardize_column_name(col)
            clean_col_names.append(clean_col_name)
        df_agents.columns = clean_col_names

        if df_agents.duplicated().sum() > 0:
            df_agents.drop_duplicates(inplace=True)

        df_agents["agent_key"] = df_agents["id"].apply(self.generate_key)
        df_agents["name"] = df_agents["name"].apply(self.cleaner.standardize_name)
        df_agents["experience"] = df_agents["experience"].apply(
            self.cleaner.validate_experience_level
        )
        df_agents["state"] = df_agents["state"].apply(self.cleaner.validate_state)

        df_agents = df_agents[
            ["agent_key"] + [col for col in df_agents.columns if col != "agent_key"]
        ]
        return df_agents

    def clean_web_complaints(self, web_complaints_data: str) -> pd.DataFrame:
        df_complaints = pd.read_parquet(web_complaints_data)

        clean_col_names = []
        for col in df_complaints.columns:
            clean_col_name = self.cleaner.standardize_column_name(col)
            clean_col_names.append(clean_col_name)
        df_complaints.columns = clean_col_names

        if df_complaints.duplicated().sum() > 0:
            df_complaints.drop_duplicates(inplace=True)

        df_complaints.drop(columns=["column1"], inplace=True)
        df_complaints["web_complaint_key"] = (
            df_complaints["request_id"].astype(str)
            + "|"
            + df_complaints["resolution_status"].astype(str)
        ).apply(lambda x: hashlib.md5(x.encode()).hexdigest()[:16])

        df_complaints["complaint_category"] = df_complaints["complaint_category"].apply(
            self.cleaner.validate_complaint_category
        )
        df_complaints["resolution_status"] = df_complaints["resolution_status"].apply(
            self.cleaner.validate_resolution_status
        )

        df_complaints = df_complaints[
            ["web_complaint_key"]
            + [col for col in df_complaints.columns if col != "web_complaint_key"]
        ]

        return df_complaints

    def clean_sm_complaints(self, sm_complaints_data: str) -> pd.DataFrame:
        df_complaints = pd.read_parquet(sm_complaints_data)

        clean_col_names = []
        for col in df_complaints.columns:
            clean_col_name = self.cleaner.standardize_column_name(col)
            clean_col_names.append(clean_col_name)
        df_complaints.columns = clean_col_names

        if df_complaints.duplicated().sum() > 0:
            df_complaints.drop_duplicates(inplace=True)

        df_complaints["sm_complaint_key"] = df_complaints.apply(
            lambda row: self.generate_key(
                row["complaint_id"], row["resolution_status"]
            ),
            axis=1,
        )
        df_complaints["complaint_category"] = df_complaints["complaint_category"].apply(
            self.cleaner.validate_complaint_category
        )
        df_complaints["resolution_status"] = df_complaints["resolution_status"].apply(
            self.cleaner.validate_resolution_status
        )

        df_complaints = df_complaints[
            ["sm_complaint_key"]
            + [col for col in df_complaints.columns if col != "sm_complaint_key"]
        ]

        return df_complaints

    def clean_call_logs(self, call_logs: str) -> pd.DataFrame:
        df_logs = pd.read_parquet(call_logs)

        clean_col_names = []
        for col in df_logs.columns:
            clean_col_name = self.cleaner.standardize_column_name(col)
            clean_col_names.append(clean_col_name)
        df_logs.columns = clean_col_names

        if df_logs.duplicated().sum() > 0:
            df_logs.drop_duplicates(inplace=True)

        df_logs = df_logs.drop(columns=["unnamed_0"])
        df_logs["call_log_key"] = df_logs.apply(
            lambda row: self.generate_key(row["call_id"], row["resolution_status"]),
            axis=1,
        )
        df_logs["complaint_category"] = df_logs["complaint_category"].apply(
            self.cleaner.validate_complaint_category
        )
        df_logs["resolution_status"] = df_logs["resolution_status"].apply(
            self.cleaner.validate_resolution_status
        )

        df_logs = df_logs[
            ["call_log_key"] + [col for col in df_logs.columns if col != "call_log_key"]
        ]

        return df_logs
