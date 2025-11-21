import pandas as pd
import re
from include.etl.transformation.enums import (
    STATE_CODES,
    STATES,
    RESOLUTION_STATUS,
    COMPLAINT_CATEGORIES,
    GENDER,
    EXPERIENCE_LEVELS,
)
from include.exceptions.exceptions import NullAfterCleanError


class Cleaner:
    def __init__(self):
        self.problematic_data = []

    @staticmethod
    def standardize_column_name(col: str) -> str:
        MANUAL_CORRECTIONS = {
            'custome_r_i_d': 'customer_id',
            'complaint_catego_ry': 'complaint_category',
            'webformgenerationdate': 'web_form_generation_date',
            'resolutionstatus': 'resolution_status'
        }
        if col in MANUAL_CORRECTIONS:
            col = MANUAL_CORRECTIONS.get(col)
        else:
            col = col.strip().lower().replace(" ", "_")
            col = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", col)
            col = re.sub(r"[^a-z0-9]+", "_", col)
            col = col.strip("_")

        return col


    @staticmethod
    def standardize_name(name: str) -> str:
        return name.strip().title()

    def standardize_state(self, state: str) -> str | None:
        if state.lower() not in STATES:
            return None

        return state.title()

    def standardize_experience(self, experience: str) -> str | None:
        if experience.lower() not in EXPERIENCE_LEVELS:
            return None

        return experience.title()

    def validate_gender(self, gender: str) -> str | None:
        if gender.lower() not in GENDER:
            return None

        return gender.upper()

    @staticmethod
    def clean_email(email: str) -> str | None:
        if pd.isna(email):
            return None

        email = email.lower().strip()

        email = re.sub(r"@\d+@", "@", email)

        email = re.sub(r"[^a-z0-9._-]+@", "@", email)

        email = email.replace(".om", ".com")
        email = email.replace("gmial", "gmail")
        email = email.replace("hotmai", "hotmail")
        email = email.replace("hotmaill", "hotmail")

        if "@" in email and "." in email.split("@")[-1]:
            return email
        return None  # flag

    @staticmethod
    def extract_state_code(address: str) -> str | None:
        if pd.isna(address):
            return None

        parts = [p.strip() for p in address.split(" ")]
        if len(parts) >= 2:
            state_code = parts[-2].upper()

            if state_code in STATE_CODES and len(state_code) in range(2, 4):
                return state_code
            else:
                pass  # log data quality

        return None

    @staticmethod
    def extract_state(state_code: str) -> str | None:
        if pd.isna(state_code):
            return None
        return STATE_CODES.get(state_code)

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
