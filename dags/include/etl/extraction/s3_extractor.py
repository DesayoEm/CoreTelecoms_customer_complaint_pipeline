from typing import Dict
import io
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.config import config
from include.exceptions.exceptions import (
    UnSupportedFileFormatError, EmptyDataFrameError, DataIngestionError, DataQualityWarning
)
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


class S3Extractor:
    def __init__(
            self,
            context: Dict,
            s3_src_hook: S3Hook = None,
            s3_dest_hook: S3Hook = None
    ):
        self.context = context
        self.s3_src_hook = s3_src_hook or S3Hook(aws_conn_id="aws_airflow_src_user")
        self.s3_dest_hook = s3_dest_hook or S3Hook(aws_conn_id="aws_airflow_dest_user")

    def copy_customers_data(self) -> Dict[str, any]:
        """Copies customer data from source S3 to destination S3."""
        try:

            src_bucket = config.SRC_BUCKET_NAME
            src_key = config.SRC_CUSTOMERS_OBJ_KEY

            log.info(f"Reading from s3://{src_bucket}/{src_key}")


            file_content = self.s3_src_hook.read_key(
                key=src_key,
                bucket_name=src_bucket
            )

            current_execution_date = self.context.get('ds')
            dest_bucket = config.CUSTOMER_DATA_STAGING_BUCKET
            dest_key = f"customer-data/customers_dataset-{current_execution_date}.parquet"

            metadata = self.convert_and_upload_to_parquet(
                data=file_content,
                src_key=src_key,
                dest_bucket=dest_bucket,
                dest_key=dest_key
            )

            log.info(f"Successfully copied customers data: {metadata['row_count']} rows")
            return metadata

        except DataQualityWarning as e:
            log.warning(f"Data quality issue with customers data: {str(e)}")
            raise
        except Exception as e:
            log.error(f"Error ingesting customers data: {str(e)}")
            raise DataIngestionError(f"Failed to copy customers data: {str(e)}") from e


    def convert_and_upload_to_parquet(
            self,
            data: str | bytes,
            src_key: str,
            dest_bucket: str,
            dest_key: str
    ) -> Dict[str, any]:
        """Converts source files to Parquet and uploads to S3 using destination hook."""

        format_map = {
            '.csv': (pd.read_csv, lambda d: io.StringIO(d) if isinstance(d, str) else io.BytesIO(d)),
            '.json': (pd.read_json, lambda d: io.StringIO(d) if isinstance(d, str) else io.BytesIO(d)),
            '.xlsx': (pd.read_excel, lambda d: io.BytesIO(d) if isinstance(d, bytes) else d),
            '.xls': (pd.read_excel, lambda d: io.BytesIO(d) if isinstance(d, bytes) else d),
        }

        file_ext = '.' + src_key.rsplit('.', 1)[-1].lower()

        if file_ext == '.parquet':
            log.info(f"File {src_key} is already parquet, copying as-is")
            bytes_data = data if isinstance(data, bytes) else data.encode()

            self.s3_dest_hook.load_bytes(
                bytes_data=bytes_data,
                key=dest_key,
                bucket_name=dest_bucket,
                replace=True
            )

            log.info(f"Successfully saved to s3://{dest_bucket}/{dest_key}")
            return {
                "dest_bucket": dest_bucket,
                "dest_key": dest_key,
                "file_size_bytes": len(bytes_data),
                "format": "parquet"
            }


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
        df.to_parquet(buffer, engine='pyarrow', index=False, compression='snappy')
        file_size_bytes = buffer.tell()
        buffer.seek(0)

        self.s3_dest_hook.load_file_obj(
            file_obj=buffer,
            key=dest_key,
            bucket_name=dest_bucket,
            replace=True
        )

        log.info(
            f"Successfully uploaded to s3://{dest_bucket}/{dest_key} "
            f"({row_count} rows, {file_size_bytes / 1024 / 1024:.2f} MB)"
        )

        return {
            "src_key": src_key,
            "destination": f"{dest_bucket}/{dest_key}",
            "row_count": row_count,
            "file_size_bytes": file_size_bytes,
            "format": "parquet"
        }