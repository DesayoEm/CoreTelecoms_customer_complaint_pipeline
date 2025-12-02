from typing import Dict
import io
import json
from datetime import datetime
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.context import Context


from include.config import config
from include.notifications.failure_middleware import (
    persist_ingestion_metadata_before_failure,
)
from include.exceptions.exceptions import (
    UnSupportedFileFormatError,
    EmptyDataFrameError,
)

from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


class S3Extractor:
    def __init__(
        self, context: Context, s3_src_hook: S3Hook = None, s3_dest_hook: S3Hook = None
    ):
        self.context = context
        self.execution_date = context.get("ds")
        self.s3_src_hook = s3_src_hook or S3Hook(aws_conn_id="aws_airflow_src_user")
        self.s3_dest_hook = s3_dest_hook or S3Hook(aws_conn_id="aws_airflow_dest_user")

    def copy_data(
        self, src_key: str, dest_prefix: str, obj_prefix: str, obj_type: str
    ) -> Dict[str, any]:
        """Copies data from source S3 to destination S3."""

        metadata = {"execution_date": self.execution_date}
        src_bucket = config.SRC_BUCKET_NAME

        try:
            log.info(f"Reading from s3://{src_bucket}/{src_key}")

            file_content = self.s3_src_hook.read_key(
                key=src_key, bucket_name=src_bucket
            )

            dest_bucket = config.BRONZE_BUCKET

            full_dest_key = f"{dest_prefix}/{obj_prefix}-{self.execution_date}.parquet"

            conversion_result = self.convert_and_upload_to_s3(
                data=file_content,
                src_key=src_key,
                dest_bucket=dest_bucket,
                dest_key=full_dest_key,
            )

            metadata = {**metadata, **conversion_result}
            log.info(f"Successfully copied {obj_type}: {metadata['row_count']} rows")
            return metadata

        except Exception as e:
            log.error(f"Failed to copy {obj_type}: {str(e)}")
            persist_ingestion_metadata_before_failure(e, self.context, metadata)

    def convert_and_upload_to_s3(
        self, data: str | bytes, src_key: str, dest_bucket: str, dest_key: str
    ) -> Dict[str, any]:
        """Converts source files to Parquet and uploads to S3 using destination hook."""

        format_map = {
            ".csv": (
                pd.read_csv,
                lambda d: io.StringIO(d) if isinstance(d, str) else io.BytesIO(d),
            ),
            ".json": (
                pd.read_json,
                lambda d: io.StringIO(d) if isinstance(d, str) else io.BytesIO(d),
            ),
            ".xlsx": (
                pd.read_excel,
                lambda d: io.BytesIO(d) if isinstance(d, bytes) else d,
            ),
            ".xls": (
                pd.read_excel,
                lambda d: io.BytesIO(d) if isinstance(d, bytes) else d,
            ),
        }

        file_ext = "." + src_key.rsplit(".", 1)[-1].lower()

        if file_ext == ".parquet":
            log.info(f"File {src_key} is already parquet, copying as-is")
            bytes_data = data if isinstance(data, bytes) else data.encode()

            self.s3_dest_hook.load_bytes(
                bytes_data=bytes_data,
                key=dest_key,
                bucket_name=dest_bucket,
                replace=True,
            )

            log.info(f"Successfully saved to s3://{dest_bucket}/{dest_key}")
            metadata = {
                "execution_date": self.execution_date,
                "dest_bucket": dest_bucket,
                "dest_key": dest_key,
                "file_size_bytes": len(bytes_data),
                "format": "parquet",
            }
            ti = self.context["task_instance"]
            ti.xcom_push(key="metadata", value=metadata)
            return metadata

        if file_ext not in format_map:
            raise UnSupportedFileFormatError(
                f"File format '{file_ext}' is not supported. "
                f"Supported formats: {', '.join(format_map.keys())}, .parquet"
            )

        parser_func, io_wrapper = format_map[file_ext]
        df = parser_func(io_wrapper(data))

        if df.empty:
            raise EmptyDataFrameError(src_key)

        row_count = len(df)
        log.info(f"Parsed {row_count} rows from {src_key}")

        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
        file_size_bytes = buffer.tell()
        buffer.seek(0)

        self.s3_dest_hook.load_file_obj(
            file_obj=buffer, key=dest_key, bucket_name=dest_bucket, replace=True
        )

        log.info(
            f"Successfully uploaded to s3://{dest_bucket}/{dest_key} "
            f"({row_count} rows, {file_size_bytes / 1024 / 1024:.2f} MB)"
        )

        manifest = {
            "data_file": dest_key,
            "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "metrics": {"row_count": row_count, "file_size_bytes": file_size_bytes},
            "lineage": {
                "source_bucket": config.SRC_BUCKET_NAME,
                "source_key": src_key,
                "dag_id": self.context["dag"].dag_id,
                "run_id": self.context["run_id"],
                "actual_date": datetime.today().strftime("%Y-%m-%d"),
                "execution_date": self.execution_date,
            },
        }

        manifest_key = dest_key.replace(".parquet", "_manifest.json")

        self.s3_dest_hook.load_string(
            string_data=json.dumps(manifest, indent=2),
            key=manifest_key,
            bucket_name=dest_bucket,
            replace=True,
        )

        log.info(f"Metadata saved to s3://{dest_bucket}/{manifest_key}")

        metadata = {
            "src_key": src_key if src_key else "Unknown",
            "destination": (
                f"s3://{dest_bucket}/{dest_key}"
                if dest_bucket and dest_key
                else "Unknown"
            ),
            "row_count": row_count,
            "file_size_bytes": file_size_bytes,
            "format": "parquet",
        }
        ti = self.context["task_instance"]
        ti.xcom_push(key="metadata", value=metadata)
        return metadata

    def copy_customers_data(self) -> Dict[str, any]:
        """Copies customer data from source S3 to destination S3."""
        return self.copy_data(
            src_key=config.SRC_CUSTOMERS_OBJ_KEY,
            dest_prefix=config.CUSTOMER_DATA_STAGING_DEST,
            obj_prefix=config.CUSTOMER_DATA_OBJ_PREFIX,
            obj_type="customers data",
        )

    def copy_call_log_data(self) -> Dict[str, any]:
        """Copies call logs from source S3 to destination S3."""
        return self.copy_data(
            src_key=f"call logs/call_logs_day_{self.execution_date}.csv",
            dest_prefix=config.CALL_LOGS_STAGING_DEST,
            obj_prefix=config.CALL_LOGS_OBJ_PREFIX,
            obj_type="call logs",
        )

    def copy_social_media_complaint_data(self) -> Dict[str, any]:
        """Copies social media complaints from source S3 to destination S3."""
        return self.copy_data(
            src_key=f"social_medias/media_complaint_day_{self.execution_date}.json",
            dest_prefix=config.SM_COMPLAINTS_STAGING_DEST,
            obj_prefix=config.SM_COMPLAINTS_OBJ_PREFIX,
            obj_type="social media complaints",
        )
