from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
load_dotenv()

class Settings(BaseSettings):
    BASE_URL: str
    FIRST_PAGE_URL: str

    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str

    SRC_ACCESS_KEY_ID: str
    SRC_SECRET_ACCESS_KEY: str
    SRC_REGION: str


    CUSTOMER_DATA_STAGING_BUCKET: str
    AGENT_DATA_STAGING_BUCKET: str
    CALL_LOGS_STAGING_BUCKET: str
    SM_COMPLAINTS_STAGING_BUCKET: str
    WEB_COMPLAINTS_STAGING_BUCKET: str

    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_CONN_STR: str




    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

config = Settings()
