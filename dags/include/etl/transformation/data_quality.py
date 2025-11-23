from typing import List, Dict, Tuple
import pandas as pd
from datetime import datetime
from dags.include.etl.transformation.data_cleaning import Cleaner
from dags.include.etl.transformation.enums import (
    GENDER,
    EXPERIENCE_LEVELS,
    STATES,
    RESOLUTION_STATUS,
    COMPLAINT_CATEGORIES,
    MEDIA_CHANNELS,
)


class DataQualityChecker:
    """Identifies data quality issues in DataFrames before transformation"""

    def __init__(self, cleaner: Cleaner):
        self.cleaner = cleaner

    @staticmethod
    def build_problematic_fields(row, mask_field_pairs: List[Tuple]) -> List[Dict]:
        """
        Generic helper to identify which fields failed validation.
        Args:
            row: DataFrame row being processed
            mask_field_pairs: List of tuples (mask, field_name, source_column)
        Returns:
            List of dicts with field name and original value
        """

        fields = []
        idx = row.name

        for mask, field_name, source_column in mask_field_pairs:
            if mask.loc[idx]:
                fields.append(
                    {"field": field_name, "original_value": row[source_column]}
                )

        return fields

    def identify_problematic_records(
        self, df: pd.DataFrame, mask_field_pairs: List[Tuple]
    ) -> pd.DataFrame:
        """
        Generic method to identify problematic records given validation masks.
        Args:
            df: Source DataFrame
            mask_field_pairs: List of (mask, field_name, source_column) tuples
        Returns:
            DataFrame with problematic records and problematic_fields column
        """

        combined_mask = mask_field_pairs[0][0]
        for mask, _, _ in mask_field_pairs[1:]:
            combined_mask = combined_mask | mask

        if not combined_mask.any():
            return pd.DataFrame(
                columns=list(df.columns) + ["problematic_fields", "identified_at"]
            )

        problematic_df = df[combined_mask].copy()

        problematic_df["problematic_fields"] = problematic_df.apply(
            lambda row: self.build_problematic_fields(row, mask_field_pairs), axis=1
        )
        problematic_df["identified_at"] = datetime.now().isoformat()

        return problematic_df

    def identify_problematic_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identify customer records with validation failures"""

        invalid_gender_mask = df["gender"].notna() & ~df["gender"].str.lower().isin(
            GENDER
        )

        email_series = df["email"].apply(
            lambda x: self.cleaner.clean_email(x) if pd.notna(x) else x
        )
        invalid_email_mask = df["email"].notna() & email_series.isna()

        state_code_series = df["address"].apply(
            lambda x: self.cleaner.extract_state_code(x) if pd.notna(x) else x
        )
        missing_state_mask = df["address"].notna() & state_code_series.isna()

        mask_field_pairs = [
            (invalid_gender_mask, "gender", "gender"),
            (invalid_email_mask, "email", "email"),
            (missing_state_mask, "state_code", "address"),
        ]

        return self.identify_problematic_records(df, mask_field_pairs)

    def identify_problematic_agents(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identify agent records with validation failures"""

        invalid_experience_mask = df["experience"].notna() & ~df[
            "experience"
        ].str.lower().isin(EXPERIENCE_LEVELS)

        invalid_state_mask = df["state"].notna() & ~df["state"].str.lower().isin(STATES)

        mask_field_pairs = [
            (invalid_experience_mask, "experience", "experience"),
            (invalid_state_mask, "state", "state"),
        ]

        return self.identify_problematic_records(df, mask_field_pairs)

    def identify_problematic_web_complaints(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identify web complaint records with validation failures"""

        invalid_category_mask = df["complaint_category"].notna() & ~df[
            "complaint_category"
        ].str.lower().isin(COMPLAINT_CATEGORIES)

        invalid_status_mask = df["resolution_status"].notna() & ~df[
            "resolution_status"
        ].str.lower().isin(RESOLUTION_STATUS)

        mask_field_pairs = [
            (invalid_category_mask, "complaint_category", "complaint_category"),
            (invalid_status_mask, "resolution_status", "resolution_status"),
        ]

        return self.identify_problematic_records(df, mask_field_pairs)

    def identify_problematic_sm_complaints(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identify social media complaint records with validation failures"""

        invalid_category_mask = df["complaint_category"].notna() & ~df[
            "complaint_category"
        ].str.lower().isin(COMPLAINT_CATEGORIES)

        invalid_status_mask = df["resolution_status"].notna() & ~df[
            "resolution_status"
        ].str.lower().isin(RESOLUTION_STATUS)

        invalid_channel_mask = df["media_channel"].notna() & ~df[
            "media_channel"
        ].str.lower().isin(MEDIA_CHANNELS)

        mask_field_pairs = [
            (invalid_category_mask, "complaint_category", "complaint_category"),
            (invalid_status_mask, "resolution_status", "resolution_status"),
            (invalid_channel_mask, "media_channel", "media_channel"),
        ]

        return self.identify_problematic_records(df, mask_field_pairs)

    def identify_problematic_call_logs(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identify call log records with validation failures"""

        invalid_category_mask = df["complaint_category"].notna() & ~df[
            "complaint_category"
        ].str.lower().isin(COMPLAINT_CATEGORIES)

        invalid_status_mask = df["resolution_status"].notna() & ~df[
            "resolution_status"
        ].str.lower().isin(RESOLUTION_STATUS)

        mask_field_pairs = [
            (invalid_category_mask, "complaint_category", "complaint_category"),
            (invalid_status_mask, "resolution_status", "resolution_status"),
        ]

        return self.identify_problematic_records(df, mask_field_pairs)
