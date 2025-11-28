from typing import Dict
import json
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from include.config import config
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


class LoadStateHandler:
    """Loads checkpoint state from Airflow XCom."""

    def __init__(self, context: Dict):
        self.context = context

    def determine_starting_point(self) -> int:
        """
        Load checkpoint from Airflow Variable on task retry.
        Returns last completed batch number, or 0 if no checkpoint exists.
        """
        ti = self.context.get("task_instance")
        if ti.try_number == 1:  # don't access checkpoint on first run
            log.info("First run. Starting fresh load")
            return 0

        current_execution_date = self.context.get("ds")
        checkpoint_key = f"{ti.task_id}_{current_execution_date}"
        try:
            checkpoint_json = Variable.get(checkpoint_key)
            checkpoint = json.loads(checkpoint_json)
            batch_num = checkpoint.get("last_completed_batch", 0)
            rows = checkpoint.get("rows_loaded", "unknown")

            log.info(
                f"Checkpoint loaded - Key: {checkpoint_key}, Batch: {batch_num}, Rows: {rows}"
            )
            log.info(f"  Resuming from batch {batch_num + 1}")
            return batch_num

        except KeyError:
            log.info(f"No checkpoint found for key: {checkpoint_key}")
            log.info(f"  Starting fresh from batch 0")
            return 0

        except json.JSONDecodeError as e:
            log.error(
                f"Failed to parse checkpoint JSON: {e}\n"
                f"JSON DATA\n\n"
                f"{checkpoint_json}"
            )

            log.info(f"Starting fresh from batch 0")
            return 0

    def save_checkpoint(self, batch_num: int, rows_loaded: int) -> None:
        """
        Save checkpoint to Airflow Variable for retry recovery.
        Overwrites previous run for same task_id + execution_date.
        """
        ti = self.context.get("task_instance")
        current_execution_date = self.context.get("ds")
        checkpoint_key = f"{ti.task_id}_{current_execution_date}"
        checkpoint_value = {
            "last_completed_batch": batch_num,
            "rows_loaded": rows_loaded,
            "date": current_execution_date,
        }
        Variable.set(checkpoint_key, json.dumps(checkpoint_value))
        log.info(
            f"Checkpoint saved - Key: {checkpoint_key}, Batch: {batch_num}, Rows: {rows_loaded}"
        )

    def clear_checkpoint(self, context: Dict):
        """
        Manually clear checkpoint
        """
        ti = context["task_instance"]
        checkpoint_key = f"{ti.task_id}_{context['ds']}"

        try:
            Variable.delete(checkpoint_key)
            log.info(f"✓ Checkpoint cleared - Key: {checkpoint_key}")
        except KeyError:
            log.info(f"○ No checkpoint to clear for key: {checkpoint_key}")


class Loader:
    """Handles loading transformed data to destination with checkpoint support."""

    def __init__(self, context: Dict, s3_dest_hook=None):
        self.context = context
        self.engine = create_engine(
            config.SILVER_DB_CONN_STRING,
            isolation_level="AUTOCOMMIT",
            pool_pre_ping=True,
        )
        self.s3_dest_hook = s3_dest_hook or S3Hook(aws_conn_id="aws_airflow_dest_user")
        self.rows_loaded: int = 0

    def load_data(self, data: pd.DataFrame, entity_type: str):
        table = self.get_table_name(entity_type)
        staging_table = f"staging_{table}"
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {staging_table}"))
                log.info(f"Cleared {staging_table}")

                data.to_sql(
                    staging_table,
                    conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=10000,
                )
                if "complaints" in entity_type:
                    self.load_complaints_with_fk_validation(conn, staging_table, table)
                elif "customer" in entity_type:
                    self.load_customers(conn, staging_table, table)
                elif "agents" in entity_type:
                    self.load_agents(conn, staging_table, table)
                else:
                    raise ValueError(f"Unknown entity type: {entity_type}")
        finally:
            self.engine.dispose()

    def load_complaints_with_fk_validation(self, conn, staging_table, target_table):
        result = conn.execute(
            text(
                f"""
            INSERT INTO {target_table}
                SELECT * FROM {staging_table}
                ON CONFLICT (complaint_id)
                    DO UPDATE SET
                        resolution_status = EXCLUDED.resolution_status,
                        resolution_date = EXCLUDED.resolution_date,
                        last_updated_at = CURRENT_TIMESTAMP
                    """
            )
        )

        fk_violations = conn.execute(
            text(
                f"""
            SELECT s.*
            FROM {staging_table} s
            LEFT JOIN conformed_customers c ON s.customer_id = c.customer_id
            LEFT JOIN conformed_agents a ON s.agent_id = a.agent_id
            WHERE c.customer_id IS NULL OR a.agent_id IS NULL
            """
            )
        ).fetchall()

        if fk_violations:
            self.save_to_quarantine(fk_violations, target_table, "fk_violation")
            log.warning(f"Quarantined {len(fk_violations)} records with FK violations")

        rows_inserted = conn.execute(
            text(
                f"""
            INSERT INTO {target_table}
            SELECT s.*
            FROM {staging_table} s
            INNER JOIN conformed_customers c ON s.customer_id = c.customer_id
            INNER JOIN conformed_agents a ON s.agent_id = a.agent_id
            ON CONFLICT (complaint_id) DO UPDATE SET
                resolution_status = EXCLUDED.resolution_status,
                resolution_date = EXCLUDED.resolution_date,
                updated_at = CURRENT_TIMESTAMP
        """
            )
        ).rowcount
        rows_affected = result.rowcount

        self.rows_loaded += rows_affected
        log.info(f"Loaded {rows_inserted} valid records to {target_table}")

    def load_customers(self, conn, staging_table, target_table):
        result = conn.execute(
            text(
                f"""
                INSERT INTO {target_table}
                SELECT * FROM {staging_table}
                ON CONFLICT (customer_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    gender = EXCLUDED.gender,
                    signup_date = EXCLUDED.signup_date,
                    email = EXCLUDED.email,
                    address = EXCLUDED.address,
                    zip_code = EXCLUDED.zip_code,
                    state_code = EXCLUDED.state_code,
                    state = EXCLUDED.state,
                    last_updated_at = CURRENT_TIMESTAMP
                """
            )
        )
        rows_affected = result.rowcount
        self.rows_loaded += rows_affected
        log.info(f"Loaded/updated {rows_affected} rows to {target_table}")

    def load_agents(self, conn, staging_table, target_table):
        result = conn.execute(
            text(
                f"""
                INSERT INTO {target_table}
                SELECT * FROM {staging_table}
                ON CONFLICT (id)
                DO UPDATE SET
                    experience = EXCLUDED.experience,
                    last_updated_at = CURRENT_TIMESTAMP
                """
            )
        )
        rows_affected = result.rowcount
        self.rows_loaded += rows_affected
        log.info(f"Loaded/updated {rows_affected} rows to {target_table}")

    def save_to_quarantine(self, records, table_name, issue_type):
        with self.engine.begin() as conn:
            for record in records:
                conn.execute(
                    text(
                        """
                    INSERT INTO data_quality_quarantine (table_name, issue_type, record_data)
                    VALUES (:table_name, :issue_type, :record_data)
                """
                    ),
                    {
                        "table_name": table_name,
                        "issue_type": issue_type,
                        "record_data": json.dumps(dict(record)),
                    },
                )

    @staticmethod
    def get_table_name(entity_type: str) -> str:
        """Generate table name from entity type"""
        entity_type = entity_type.replace(" ", "_")
        return f"conformed_{entity_type}"

    def create_load_manifest(self, entity_type: str, table_name: str) -> str:

        manifest = {
            "entity": entity_type,
            "table": table_name,
            "created_at": datetime.now().isoformat(),
            "metrics": {"row_count": self.rows_loaded},
            "lineage": {
                "dag_id": self.context["dag"].dag_id,
                "run_id": self.context["run_id"],
                "execution_date": self.context["ds"],
            },
        }

        manifest_key = f"load-manifests/{entity_type}/{entity_type}-{self.context['ds']}-manifest.json"
        self.s3_dest_hook.load_string(
            string_data=json.dumps(manifest, indent=2),
            key=manifest_key,
            bucket_name=config.BRONZE_BUCKET,
            replace=True,
        )

        return manifest_key
