import pandas as pd
import pyarrow.parquet as pq
import hashlib
from include.etl.transformation.enums import (
    STATE_CODES,
    STATES,
    RESOLUTION_STATUS,
    COMPLAINT_CATEGORIES,
)
from include.etl.transformation.data_cleaning import Cleaner


class Transformer:
    def __init__(self):
        self.cleaner = Cleaner()

    @staticmethod
    def generate_key(*args) -> str:
        """Generate a deterministic surrogate key from input values."""
        combined = "|".join(str(arg) for arg in args if arg is not None)
        return hashlib.md5(combined.encode()).hexdigest()[:16]

    def clean_customers(self, customer_data: str) -> pd.DataFrame:
        df_customers = pd.read_parquet(customer_data)

        clean_col_names = []
        for col in df_customers.columns:
            clean_col_name = self.cleaner.standardize_column_name(col)
            clean_col_names.append(clean_col_name)
        df_customers.columns = clean_col_names

        if df_customers.duplicated().sum() > 0:
            df_customers.drop_duplicates(inplace=True)

        df_customers["customer_key"] = df_customers["customer_id"].apply(
            self.generate_key
        )

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

        df_customers = df_customers[
            ["customer_key"]
            + [col for col in df_customers.columns if col != "customer_key"]
        ]

        return df_customers

    def clean_agents(self, agent_data: str) -> pd.DataFrame:
        df_agents = pd.read_parquet(agent_data)

        clean_col_names = []
        for col in df_agents.columns:
            clean_col_name = self.cleaner.standardize_column_name(col)
            clean_col_names.append(clean_col_name)
        df_agents.columns = clean_col_names

        if df_agents.duplicated().sum() > 0:
            df_agents.drop_duplicates(inplace=True)

        df_agents["agent_key"] = df_agents["id"].apply(self.cleaner.generate_key)
        df_agents["name"] = df_agents["name"].apply(self.cleaner.standardize_name)
        df_agents["experience"] = df_agents["experience"].apply(
            self.cleaner.standardize_experience
        )
        df_agents["state"] = df_agents["state"].apply(self.cleaner.standardize_state)

        df_agents = df_agents[
            ["agent_key"] + [col for col in df_agents.columns if col != "agent_key"]
        ]
        return df_agents

