from dataclasses import dataclass
from typing import Dict, Tuple
import io
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


from include.config import config
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


@dataclass
class LoadState:
    """Checkpoint state for resumable loading."""
    last_completed_batch: int = 0
    rows_loaded: int = 0


class StateLoader:
    """Loads checkpoint state from Airflow XCom."""

    @staticmethod
    def load_from_context(context: Dict) -> LoadState:
        """Load checkpoint on retry, otherwise start fresh."""
        ti = context.get('task_instance')

        if not ti or ti.try_number == 1:
            log.info("Starting fresh (first attempt)")
            return LoadState()

        checkpoint = ti.xcom_pull(task_ids=ti.task_id, key='checkpoint')

        if checkpoint:
            log.info(f"Resuming from batch {checkpoint['last_completed_batch']}")
            return LoadState(**checkpoint)

        log.info("No checkpoint found, starting fresh")
        return LoadState()


class Loader:
    """Handles loading transformed data to destination with checkpoint support."""

    def __init__(self, context: Dict, s3_dest_hook=None):
        self.context = context
        self.conn_string = config.DEST_DB_CONN_STRING
        self.s3_dest_hook = s3_dest_hook or S3Hook(aws_conn_id="aws_airflow_dest_user")

        state = StateLoader.load_from_context(context)
        self.last_completed_batch = state.last_completed_batch
        self.rows_loaded = state.rows_loaded

    def upload_to_s3(self, data: pd.DataFrame, source: str, dest_bucket: str, dest_key: str) -> Tuple:
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

        return location, file_size_bytes, source, row_count


    def load_data(self, data: pd.DataFrame, entity_type: str):
        """Load DataFrame to destination database."""
        engine = None
        try:
            engine = create_engine(self.conn_string)
            table_name = self._get_table_name(entity_type)

            data.to_sql(
                table_name,
                engine,
                if_exists="replace",
                index=False,
                method='multi',
                chunksize=10000
            )

            self.rows_loaded += len(data)
            log.info(f"Loaded {len(data)} rows to {table_name}")

        except Exception as e:
            log.error(f"Load failed: {e}")
            raise
        finally:
            if engine:
                engine.dispose()


    def save_checkpoint(self, batch_num: int):
        """Save checkpoint to XCom for retry recovery."""
        ti = self.context.get('task_instance')
        if not ti:
            return

        checkpoint = {
            'last_completed_batch': batch_num,
            'rows_loaded': self.rows_loaded,
            'timestamp': datetime.now().isoformat()
        }

        ti.xcom_push(key='checkpoint', value=checkpoint)
        log.info(f"Checkpoint saved: batch {batch_num}")


    def _get_table_name(self, entity_type: str) -> str:
        """Generate table name from entity type and execution date."""
        date_suffix = self.context['ds'].replace('-', '_')
        return f"{entity_type}_{date_suffix}"

    def upload_problematic_records(self, data: pd.DataFrame, entity_type: str) -> str:
        """Upload problematic records to S3 with manifest."""
        if data.empty:
            return None

        dest_key = f"{config.PROBLEMATIC_DATA_PREFIX}/{entity_type}_{self.context['ds']}"

        location, file_size, _, row_count = self.upload_to_s3(
            data=data,
            dest_bucket=config.BRONZE_BUCKET,
            dest_key=dest_key
        )


        manifest = {
            "data_file": dest_key,
            "created_at": datetime.now().isoformat(),
            "metrics": {"row_count": row_count, "file_size_bytes": file_size},
            "lineage": {
                "dag_id": self.context["dag"].dag_id,
                "run_id": self.context["run_id"],
                "execution_date": self.context["ds"]
            }
        }

        manifest_key = f"{dest_key}_manifest.json"
        self.s3_dest_hook.load_string(
            string_data=json.dumps(manifest, indent=2),
            key=manifest_key,
            bucket_name=config.BRONZE_BUCKET
        )

        log.info(f"Uploaded {row_count} problematic records to {location}")
        return location