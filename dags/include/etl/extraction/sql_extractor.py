import pandas as pd
from sqlalchemy import create_engine
from typing import Dict
import io

import json
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from include.config import config
from include.notifications.middleware import persist_ingestion_metadata_before_failure, persist_metadata_before_exit
from include.exceptions.exceptions import (
    EmptyDataFrameError,
    SQLReadError,
)
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log


class SQLEXtractor:
    def __init__(self, context: Dict, s3_dest_hook: S3Hook = None):
        self.context = context
        self.s3_dest_hook = s3_dest_hook or S3Hook(aws_conn_id="aws_airflow_dest_user")

    def upload_dataframe_to_s3(
        self, table_name: str, df: pd.DataFrame, dest_bucket: str, dest_key: str
    ) -> Dict[str, any]:
        """Converts DataFrame to Parquet and uploads to S3 with metadata."""

        if df.empty:
            raise EmptyDataFrameError(table_name)

        row_count = len(df)
        log.info(f"Processing {row_count} rows from {table_name}")

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
            "metrics": {
                "row_count": row_count,
                "file_size_bytes": file_size_bytes,
                "columns": list(df.columns),
            },
            "lineage": {
                "source_type": "SQL",
                "source_name": table_name,
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

        log.info(f"Metadata saved to s3://{dest_bucket}/{manifest_key}")

        metadata = {
            "source_name": table_name if table_name else "Unknown",
            "dest_bucket": dest_bucket if dest_bucket else "Unknown",
            "dest_key": dest_key if dest_key else "Unknown",
            "manifest_key": manifest_key if manifest_key else "Unknown",
            "row_count": row_count ,
            "file_size_bytes": file_size_bytes,
        }

        return metadata

    def copy_web_complaints(self):
        metadata = {
            "execution_date": self.context.get("ds")
        }
        try:
            engine = create_engine(f"{config.SRC_DB_CONN_STRING}")

            table_name = "Web_form_request_2025_11_20"
            query = f"SELECT * FROM {config.SRC_DB_SCHEMA}.{table_name}"

            with engine.connect() as conn:
                df = pd.read_sql(sql=query, con=conn.connection)

            log.info(f"Extracted {len(df)} web complaints from table {table_name}")

            current_execution_date = self.context.get("ds")
            dest_key = f"{config.WEB_COMPLAINTS_STAGING_DEST}/web_complaints_{current_execution_date}.parquet"

            conversion_result =  self.upload_dataframe_to_s3(
                df=df,
                table_name=table_name,
                dest_bucket=config.BRONZE_BUCKET,
                dest_key=dest_key,
            )
            metadata = {**metadata, **conversion_result}

        except Exception as e:
            log.error(f"Error ingesting web complaints: {str(e)}")
            persist_ingestion_metadata_before_failure(e, self.context, metadata)


