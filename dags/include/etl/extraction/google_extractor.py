from typing import Dict
import io
import boto3
import json
from datetime import datetime
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from include.config import config
from include.exceptions.exceptions import (
    UnSupportedFileFormatError,
    EmptyDataFrameError,
    DataIngestionError,
    GoogleCredentialsError,
    GoogleSheetReadError,
)
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


class GoogleSheetsExtractor:
    def __init__(self, context: Dict, s3_dest_hook: S3Hook = None):
        self.context = context
        self.s3_dest_hook = s3_dest_hook or S3Hook(aws_conn_id="aws_airflow_dest_user")

        service_account_info = self.get_google_credentials()

        scope = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]

        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            service_account_info, scopes=scope
        )
        self.google_client = gspread.authorize(creds)

    @staticmethod
    def get_google_credentials() -> dict:
        """Retrieve Google account credentials from aws secrets manager."""
        try:
            session = boto3.session.Session()
            client = session.client(
                service_name="secretsmanager", region_name=config.AWS_REGION
            )

            response = client.get_secret_value(SecretId="google_cloud_cred")
            return json.loads(response["SecretString"])

        except Exception as e:
            log.error(
                f"Failed to retrieve Google credentials from Secrets Manager: {str(e)}"
            )
            raise GoogleCredentialsError(details=str(e))

    def upload_dataframe_to_s3(
        self, df: pd.DataFrame, source_name: str, dest_bucket: str, dest_key: str
    ) -> Dict[str, any]:
        """Converts DataFrame to Parquet and uploads to S3 with metadata."""

        if df.empty:
            raise EmptyDataFrameError(source_name)

        row_count = len(df)
        log.info(f"Processing {row_count} rows from {source_name}")

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
                "source_type": "google_sheets",
                "source_name": source_name,
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

        return {
            "source_name": source_name,
            "dest_bucket": dest_bucket,
            "dest_key": dest_key,
            "manifest_key": manifest_key,
            "row_count": row_count,
            "file_size_bytes": file_size_bytes,
            "format": "parquet",
        }

    def copy_agents_data(self) -> Dict[str, any]:
        """Extracts agent data from Google Sheets and uploads to S3."""
        try:
            sheet_id = config.GOOGLE_SHEET_ID
            log.info(f"Reading agents data from Google Sheet: {sheet_id}")

            sheet = self.google_client.open_by_key(sheet_id).sheet1
            data = sheet.get_all_records()
            df = pd.DataFrame(data)

            log.info(f"Extracted {len(df)} agent records from Google Sheets")

            current_execution_date = self.context.get("ds")
            dest_bucket = config.BRONZE_BUCKET
            dest_key = f"{config.AGENT_DATA_STAGING_DEST}/agents_{current_execution_date}.parquet"

            metadata = self.upload_dataframe_to_s3(
                df=df,
                source_name=f"google_sheets:{sheet_id}",
                dest_bucket=dest_bucket,
                dest_key=dest_key,
            )

            log.info(f"Successfully copied agents data: {metadata['row_count']} rows")
            return metadata

        except gspread.exceptions.SpreadsheetNotFound:
            log.error(f"Google Sheet not found: {config.GOOGLE_SHEET_ID}")
            raise GoogleSheetReadError(
                f"Google Sheet not found: {config.GOOGLE_SHEET_ID}"
            )

        except gspread.exceptions.APIError as e:
            log.error(f"Google Sheets API error: {str(e)}")
            raise GoogleSheetReadError(f"Google Sheets API error: {str(e)}") from e

        except Exception as e:
            log.error(f"Error ingesting agents data: {str(e)}")
            raise GoogleSheetReadError(f"Failed to copy agents data: {str(e)}") from e
