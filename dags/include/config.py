from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    # db
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_CONN_STR: str

    # google
    GOOGLE_SHEET_ID: str
    GOOGLE_SERVICE_ACCOUNT_PATH: str

    # aws
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str

    SRC_ACCESS_KEY_ID: str
    SRC_SECRET_ACCESS_KEY: str
    SRC_REGION: str
    SRC_DB_CONN_STRING: str
    SRC_DB_SCHEMA: str

    # bucket keys
    BRONZE_BUCKET: str
    CUSTOMER_DATA_STAGING_DEST: str
    AGENT_DATA_STAGING_DEST: str
    CALL_LOGS_STAGING_DEST: str
    SM_COMPLAINTS_STAGING_DEST: str
    WEB_COMPLAINTS_STAGING_DEST: str

    SRC_BUCKET_NAME: str
    SRC_CUSTOMERS_OBJ_KEY: str
    SRC_SM_COMPLAINTS_OBJ_KEY: str
    SRC_CALL_LOGS_OBJ_KEY: str

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )


config = Settings()
