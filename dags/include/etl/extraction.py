from typing import Dict
import io
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from include.config import config


from airflow.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log


class Extractor:
    def __init__(
        self,
        context: Dict,
        timeout: int = 30,
        max_retries: int = 3
    ):
        self.context = context
        self.timeout = timeout
        self.max_retries = max_retries


    def extract_s3(self, s3_hook):
        pass

    def extract_google_sheets(self):
        pass

    def extract_sql(self):
        pass

    def extract_customers(self, s3_hook):
        pass

    @staticmethod
    def save_response(data: Dict, bucket_name: str, object_name: str, s3_hook):
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        key = f"{bucket_name}/{object_name}.parquet"

        s3_hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=key,
            bucket_name=bucket_name,
            replace=True
        )

        log.info(
            f"Successfully saved page {object_name} at s3://{key}"
        )
