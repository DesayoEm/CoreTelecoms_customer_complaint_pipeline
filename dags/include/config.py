from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
load_dotenv()

class Settings(BaseSettings):
    # db
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_CONN_STR: str

    #aws
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str

    SRC_ACCESS_KEY_ID: str
    SRC_SECRET_ACCESS_KEY: str
    SRC_REGION: str

    #bucket keys
    CUSTOMER_DATA_STAGING_BUCKET: str
    AGENT_DATA_STAGING_BUCKET: str
    CALL_LOGS_STAGING_BUCKET: str
    SM_COMPLAINTS_STAGING_BUCKET: str
    WEB_COMPLAINTS_STAGING_BUCKET: str

    SRC_BUCKET_NAME: str
    SRC_CUSTOMERS_OBJ_KEY: str
    SRC_SM_COMPLAINTS_OBJ_KEY: str
    SRC_CALL_LOGS_OBJ_KEY: str



    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

config = Settings()
