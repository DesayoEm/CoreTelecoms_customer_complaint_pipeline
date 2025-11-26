import pandas as pd
import re
from include.etl.transformation.config.enums import (
    STATE_CODES,
    STATES,
    RESOLUTION_STATUS,
    COMPLAINT_CATEGORIES,
    GENDER,
    EXPERIENCE_LEVELS,
    MEDIA_CHANNELS,
)


class Cleaner:
    def __init__(self):
        pass

    @staticmethod
    def standardize_column_name(col: str) -> str:
        MANUAL_CORRECTIONS = {
            "custome_r_i_d": "customer_id",
            "complaint_catego_ry": "complaint_category",
            "COMPLAINT_catego ry": "complaint_category",
            "webformgenerationdate": "web_form_generation_date",
            "resolutionstatus": "resolution_status",
            "customeR iD": "customer_id",
            "iD": "id",
            "DATE of biRTH": "date_of_birth",
            "NamE": "name",
        }

        if col in MANUAL_CORRECTIONS:
            return MANUAL_CORRECTIONS[col]
        col = col.strip()
        col = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", col)
        col = col.lower()
        col = re.sub(r"[^a-z0-9]+", "_", col)
        col = col.strip("_")

        return col

    @staticmethod
    def clean_email(email: str) -> str | None:
        if pd.isna(email):
            return None

        email = email.strip().lower()

        if email.count("@") > 1:
            parts = [p for p in email.split("@") if p]
            if len(parts) < 2:
                return None
            local = parts[-2]
            domain = parts[-1]
            email = f"{local}@{domain}"

        # common domain typos
        corrections = {
            ".om": ".com",
            "gmial": "gmail",
            "hotmaill.com": "hotmail.com",
            "hotmai.com": "hotmail.com",
        }
        for wrong, right in corrections.items():
            email = email.replace(wrong, right)

        email = re.sub(r"[^a-z0-9._-]+(?=@)", "", email)

        if email.count("@") != 1:
            return None

        local, domain = email.split("@", 1)

        if not local:
            return None
        if "." not in domain:
            return None

        return email

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
                return None

    @staticmethod
    def extract_date_from_timestamp(timestamp_str: str) -> str | None:
        return timestamp_str.split()[0]

    @staticmethod
    def generate_state(state_code: str) -> str | None:
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

            if zip_code.isdigit() and len(zip_code) == 5:
                return zip_code
            else:
                return None

    @staticmethod
    def standardize_name(name: str) -> str | None:
        if pd.isna(name):
            return None
        return name.strip().title()

    @staticmethod
    def validate_state(state: str) -> str | None:
        if pd.isna(state):
            return None
        if state.lower() not in STATES:
            return None

        return state.title()

    @staticmethod
    def validate_experience_level(experience: str) -> str | None:
        if pd.isna(experience):
            return None
        if experience.lower() not in EXPERIENCE_LEVELS:
            return None

        return experience.title()

    @staticmethod
    def validate_complaint_category(category: str) -> str | None:
        if pd.isna(category):
            return None
        if category.lower() not in COMPLAINT_CATEGORIES:
            return None

        return category.title()

    @staticmethod
    def validate_resolution_status(status: str) -> str | None:
        if pd.isna(status):
            return None
        if status.lower() not in RESOLUTION_STATUS:
            return None

        return status.title()

    @staticmethod
    def validate_media_channel(channel: str) -> str | None:
        if pd.isna(channel):
            return None
        if channel.lower() not in MEDIA_CHANNELS:
            return None

        return channel.upper()

    @staticmethod
    def validate_gender(gender: str) -> str | None:
        if pd.isna(gender):
            return None
        if gender.lower() not in GENDER:
            return None
        return gender.upper()
