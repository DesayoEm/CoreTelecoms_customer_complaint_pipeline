from typing import Dict, Tuple, Any
import io
import json
from datetime import datetime
import pandas as pd
import numpy as np
import hashlib
from multiprocessing.dummy import Pool as ThreadPool

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
        self.loader = None  # to be implemented
        self.problematic_records: pd.DataFrame = pd.DataFrame()
        self.batch_size: int = 100000
        self.duplicate_count: int = 0

    @staticmethod
    def parallel_apply_threaded(series, func, workers=8):
        """
        Apply function in parallel using ThreadPool.ThreadPool used instead of ProcessPoolExecutor because Airflow workers
        are daemonic processes and cannot spawn child processes.
        Although ThreadPool is limited by Python's GIL for CPU-bound operations, it still provides some parallelism for pandas operations that release the GIL.
        """
        with ThreadPool(workers) as p:
            chunks = np.array_split(series, workers)
            results = p.map(lambda s: s.apply(func), chunks)
        return pd.concat(results)

    def apply_transformation(self, series, func, parallel_threshold=100000):
        if len(series) > parallel_threshold:
            return self.parallel_apply_threaded(series, func)
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

    def transform_and_load_entity(self, entity_data_location: str, entity_type: str):
        """Transform and load at once or in batches"""
        try:
            entity_df = pd.read_parquet(entity_data_location)
        except Exception as e:
            raise DataLoadError(error=e, entity_type=entity_type)

        total_rows = len(entity_df)
        if total_rows > self.batch_size:
            self.transform_and_load_batched(
                entity_data_location, entity_type, entity_df, total_rows
            )
        else:
            self.transform_and_load_all(entity_data_location, entity_type, entity_df)

    def transform_and_load_all(
        self, location: str, entity_type: str, entity_df: pd.DataFrame
    ) -> Dict[str, Any]:
        from dags.include.etl.transformation.entity_class_config import (
            ENTITY_CLASS_CONFIG,
        )  # scoped import to avoid circular imports

        method_name = ENTITY_CLASS_CONFIG.get(entity_type.lower())
        if not method_name:
            raise ValueError(f"Unknown entity type: {entity_type}")

        transformer_method = getattr(self, method_name)
        self.duplicate_count, self.problematic_records = transformer_method(entity_df)

        problematic_record_location = None
        if not self.problematic_records.empty:
            problematic_record_location = self.upload_problematic_records_to_s3(
                data=self.problematic_records,
                source=location,
                dest_bucket=config.BRONZE_BUCKET,
                dest_key=f"{config.PROBLEMATIC_DATA_OBJ_PREFIX}/{entity_type}-problematic-{self.context['ds']}",
            )

        metadata = {
            "rows_processed": len(entity_df),
            "duplicates_removed": self.duplicate_count,
            "problematic_rows_excluded": len(self.problematic_records),
            "problematic_data_location": (
                problematic_record_location
                if problematic_record_location
                else "No problematic data points"
            ),
        }
        return metadata

    def transform_and_load_batched(
        self, location: str, entity_type: str, entity_df: pd.DataFrame, total_rows: int
    ):
        from dags.include.etl.transformation.entity_class_config import (
            ENTITY_CLASS_CONFIG,
        )

        method_name = ENTITY_CLASS_CONFIG.get(entity_type.lower())
        if not method_name:
            raise ValueError(f"Unknown entity type: {entity_type}")

        transformer_method = getattr(self, method_name)

        start_batch = self.loader.load_checkpoint().get("last_completed_batch", 0)
        for batch_num in range(start_batch, (total_rows // self.batch_size) + 1):
            start_idx = batch_num * self.batch_size
            end_idx = min(start_idx + self.batch_size, total_rows)

            current_batch = entity_df.iloc[start_idx:end_idx].copy()
            batch_duplicates, batch_problems = transformer_method(current_batch)

            # duplicate count and problematic data are accumulated across batch iterations
            self.duplicate_count += batch_duplicates
            self.problematic_records = pd.concat(
                [self.problematic_records, batch_problems], ignore_index=True
            )

            log.info(
                f"Completed batch {batch_num + 1}, processed {end_idx}/{total_rows} rows"
            )

            # self.loader.save_checkpoint('customers', {
            #     'last_completed_batch': batch_num,
            #     'rows_processed': end_idx,
            #     'timestamp': datetime.now().isoformat()
            # })

        problematic_record_location = None
        if (
            not self.problematic_records.empty
        ):  # now a class attr as data needs to be saved across iterations
            problematic_record_location = self.upload_problematic_records_to_s3(
                data=self.problematic_records,
                source=location,
                dest_bucket=config.BRONZE_BUCKET,
                dest_key=f"{config.PROBLEMATIC_DATA_OBJ_PREFIX}/{entity_type}-problematic-{self.context['ds']}",
            )

        metadata = {
            "rows_processed": total_rows,
            "duplicates_removed": self.duplicate_count,
            "problematic_rows_excluded": len(self.problematic_records),
            "problematic_data_location": (
                problematic_record_location
                if problematic_record_location
                else "No problematic data points"
            ),
        }

        return metadata

    def transform_and_load_customer_data(self, customers_df: pd.DataFrame) -> Tuple:

        customers_df, self.duplicate_count = self.remove_duplicates(customers_df)
        customer_df = self.standardize_columns(customers_df)

        customers_df["customer_key"] = self.apply_transformation(
            customers_df["customer_id"], self.generate_key
        )
        customers_df = self.add_key_as_first_column(customer_df, "customer_key")

        # customer_key needs to be in the DataFrame, so customers with bad values are identified
        # downstream after bad values are cleaned/nulled

        self.problematic_records = self.dq_checker.identify_problematic_customers(
            customers_df
        )

        customers_df["name"] = self.apply_transformation(
            customers_df["name"], self.cleaner.standardize_name
        )
        customers_df["gender"] = self.apply_transformation(
            customers_df["gender"], self.cleaner.validate_gender
        )
        customers_df["email"] = self.apply_transformation(
            customers_df["email"], self.cleaner.clean_email
        )
        customers_df["zip_code"] = self.apply_transformation(
            customers_df["address"], self.cleaner.extract_zip_code
        )
        customers_df["state_code"] = self.apply_transformation(
            customers_df["address"], self.cleaner.extract_state_code
        )
        customers_df["state"] = self.apply_transformation(
            customers_df["state_code"], self.cleaner.generate_state
        )

        # self.loader.load df here

        return self.duplicate_count, self.problematic_records

    def transform_and_load_agents_data(self, df_agents: pd.DataFrame) -> Tuple:
        """Transforms and loads agent data. Does not require parallelization as
        agent data is predictably small and static"""

        df_agents, self.duplicate_count = self.remove_duplicates(df_agents)
        df_agents = self.standardize_columns(df_agents)

        df_agents["agent_key"] = df_agents["id"].apply(self.generate_key)
        df_agents = self.add_key_as_first_column(df_agents, "agent_key")

        # problematic records are only read after generating key
        self.problematic_records = self.dq_checker.identify_problematic_agents(
            df_agents
        )

        df_agents["name"] = df_agents["name"].apply(self.cleaner.standardize_name)
        df_agents["experience"] = df_agents["experience"].apply(
            self.cleaner.validate_experience_level
        )
        df_agents["state"] = df_agents["state"].apply(self.cleaner.validate_state)

        # self.loader.load df here
        return self.duplicate_count, self.problematic_records

    def transform_and_load_web_complaints_data(
        self, df_complaints: pd.DataFrame
    ) -> Tuple:

        df_complaints, self.duplicate_count = self.remove_duplicates(df_complaints)
        df_complaints = self.standardize_columns(df_complaints)

        df_complaints["web_complaint_key"] = (
            df_complaints["request_id"].astype(str)
            + "|"
            + df_complaints["resolution_status"].astype(str)
        ).apply(lambda x: hashlib.md5(x.encode()).hexdigest()[:16])

        df_complaints = self.add_key_as_first_column(df_complaints, "web_complaint_key")

        # problematic records are only read after generating key
        self.problematic_records = self.dq_checker.identify_problematic_web_complaints(
            df_complaints
        )

        df_complaints["complaint_category"] = self.apply_transformation(
            df_complaints["complaint_category"],
            self.cleaner.validate_complaint_category,
        )
        df_complaints["resolution_status"] = self.apply_transformation(
            df_complaints["resolution_status"], self.cleaner.validate_resolution_status
        )

        # self.loader.load df here
        return self.duplicate_count, self.problematic_records

    def transform_and_load_sm_complaints_data(
        self, df_complaints: pd.DataFrame
    ) -> Tuple:

        df_complaints, self.duplicate_count = self.remove_duplicates(df_complaints)
        df_complaints = self.standardize_columns(df_complaints)

        df_complaints["sm_complaint_key"] = (
            df_complaints["complaint_id"].astype(str)
            + "|"
            + df_complaints["resolution_status"].astype(str)
        ).apply(lambda x: hashlib.md5(x.encode()).hexdigest()[:16])

        df_complaints = self.add_key_as_first_column(df_complaints, "sm_complaint_key")

        # problematic records are only read after generating key
        self.problematic_records = self.dq_checker.identify_problematic_sm_complaints(
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

        # self.loader.load df here
        return self.duplicate_count, self.problematic_records

    def transform_and_load_call_logs_data(self, df_call_logs: pd.DataFrame) -> Tuple:

        df_call_logs = df_call_logs.drop(columns=["Unnamed: 0"])

        df_call_logs, self.duplicate_count = self.remove_duplicates(df_call_logs)
        df_call_logs = self.standardize_columns(df_call_logs)

        df_call_logs["call_log_key"] = (
            df_call_logs["call_id"].astype(str)
            + "|"
            + df_call_logs["resolution_status"].astype(str)
        ).apply(lambda x: hashlib.md5(x.encode()).hexdigest()[:16])

        # problematic records are only read after generating key
        self.problematic_records = self.dq_checker.identify_problematic_call_logs(
            df_call_logs
        )

        df_call_logs["complaint_category"] = self.apply_transformation(
            df_call_logs["complaint_category"], self.cleaner.validate_complaint_category
        )
        df_call_logs["resolution_status"] = self.apply_transformation(
            df_call_logs["resolution_status"], self.cleaner.validate_resolution_status
        )

        # self.loader.load df here
        return self.duplicate_count, self.problematic_records
