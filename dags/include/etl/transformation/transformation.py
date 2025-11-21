import pandas as pd
import re

# import json
# from typing import Dict, List, Any, Hashable
# from datetime import datetime
import hashlib
from include.etl.transformation.states_config import (
    STATES,
    RESOLUTION_STATUS,
    COMPLAINT_CATEGORIES,
)


class Transformer:
    def __init__(self):
        pass

    @staticmethod
    def generate_key(*args) -> str:
        """Generate a deterministic surrogate key from input values."""
        combined = "|".join(str(arg) for arg in args if arg is not None)
        return hashlib.md5(combined.encode()).hexdigest()[:16]

    @staticmethod
    def standardize_column_name(col: str) -> str:
        col = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", col)
        col = col.lower()
        col = re.sub(r"[^a-z0-9]+", "_", col)
        col = col.strip("_")

        return col

    @staticmethod
    def standardize_name(name: str) -> str:
        return name.strip().title()

    @staticmethod
    def clean_email(email: str) -> str | None:
        if pd.isna(email):
            return None

        email = email.lower().strip()

        email = re.sub(r"@\d+@", "@", email)

        email = re.sub(r"[^a-z0-9._-]+@", "@", email)

        email = email.replace(".om", ".com")
        email = email.replace("gmial", "gmail")

        if "@" in email and "." in email.split("@")[-1]:
            return email
        return None  # flag

    @staticmethod
    def extract_state_code(address: str) -> str | None:
        if pd.isna(address):
            return None

        parts = [p.strip() for p in address.split(" ")]
        if len(parts) >= 2:
            state_code = parts[-2]

            if state_code in STATES and len(state_code) == 2:
                return state_code
            else:
                pass  # log data quality

        return None

    @staticmethod
    def extract_zip_code(address: str) -> str | None:
        if pd.isna(address):
            return None

        parts = [p.strip() for p in address.split(" ")]
        if len(parts) >= 2:
            zip_code = parts[-1]

            if len(zip_code) == 5:
                return zip_code
            else:
                pass  # log data quality

        return None
