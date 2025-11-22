from typing import Dict, List
import pandas as pd
import re
from include.etl.transformation.enums import (
    STATE_CODES,
    STATES,
    RESOLUTION_STATUS,
    COMPLAINT_CATEGORIES,
    GENDER,
    EXPERIENCE_LEVELS,
    MEDIA_CHANNELS,
)
from include.exceptions.exceptions import NullAfterCleanError


class Cleaner:
    def __init__(self, problematic_data: List[Dict]):
        self.problematic_data = problematic_data

    @staticmethod
    def standardize_column_name(col: str) -> str:
        MANUAL_CORRECTIONS = {
            "custome_r_i_d": "customer_id",
            "complaint_catego_ry": "complaint_category",
            "webformgenerationdate": "web_form_generation_date",
            "resolutionstatus": "resolution_status",
            "MediaComplaintGenerationDate": "media_complaint_generation_date",
            "COMPLAINT_catego ry": "complaint_category",
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

    def validate_state(self, state: str) -> str | None:
        if state.lower() not in STATES:
            self.problematic_data.append(
                {"field": "state", "value": state, "reason": "invalid_state"}
            )
            return None

        return state.title()

    def validate_experience_level(self, experience: str) -> str | None:
        if experience.lower() not in EXPERIENCE_LEVELS:
            self.problematic_data.append(
                {
                    "field": "experience",
                    "value": experience,
                    "reason": "invalid experience level",
                }
            )
            return None

        return experience.title()

    def validate_complaint_category(self, category: str) -> str | None:
        if category.lower() not in COMPLAINT_CATEGORIES:
            self.problematic_data.append(
                {
                    "field": "complaint_category",
                    "value": category,
                    "reason": "invalid complaint category",
                }
            )
            return None

        return category.title()

    def validate_resolution_status(self, status: str) -> str | None:
        if status.lower() not in RESOLUTION_STATUS:
            self.problematic_data.append(
                {
                    "field": "resolution_status",
                    "value": status,
                    "reason": "invalid resolution status",
                }
            )
            return None

        return status.title()

    def validate_media_channel(self, channel: str) -> str | None:
        if channel.lower() not in MEDIA_CHANNELS:
            self.problematic_data.append(
                {
                    "field": "social_media_channel",
                    "value": channel,
                    "reason": "invalid social media channel",
                }
            )
            return None

        return channel.upper()

    def validate_gender(self, gender: str) -> str | None:
        if gender.lower() not in GENDER:
            self.problematic_data.append(
                {"field": "gender", "value": gender, "reason": "invalid gender"}
            )
            return None

        return gender.upper()

    def clean_email(self, email: str) -> str | None:
        if pd.isna(email):
            return None

        email_copy = email  # original value preserved for data quality logging

        email = email.lower().strip()
        email = re.sub(r"@\d+@", "@", email)
        email = re.sub(r"[^a-z0-9._-]+@", "@", email)

        email = email.replace(".om", ".com")
        email = email.replace("gmial", "gmail")
        email = email.replace("hotmai", "hotmail")
        email = email.replace("hotmaill", "hotmail")

        if "@" in email and "." in email.split("@")[-1]:
            return email

        self.problematic_data.append(
            {"field": "email", "value": email_copy, "reason": "invalid email address"}
        )
        return None

    def extract_state_code(self, address: str) -> str | None:
        if pd.isna(address):
            return None

        parts = [p.strip() for p in address.split(" ")]
        if len(parts) >= 2:
            state_code = parts[-2].upper()

            if state_code in STATE_CODES and len(state_code) in range(2, 4):
                return state_code
            else:
                self.problematic_data.append(
                    {
                        "field": "address",
                        "value": address,
                        "reason": "could not parse state code from address",
                    }
                )

                return None

    @staticmethod
    def extract_state(state_code: str) -> str | None:
        if pd.isna(state_code):
            return None
        return STATE_CODES.get(state_code)

    def extract_zip_code(self, address: str) -> str | None:
        if pd.isna(address):
            return None

        parts = [p.strip() for p in address.split(" ")]
        if len(parts) >= 2:
            zip_code = parts[-1]

            if len(zip_code) == 5:
                return zip_code
            else:
                self.problematic_data.append(
                    {
                        "field": "address",
                        "value": address,
                        "reason": "could not parse zip code from address",
                    }
                )

        return None
