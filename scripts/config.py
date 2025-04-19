from typing import Literal, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

class Config(BaseSettings):
    """Configuration settings for the application"""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # Environment settings
    ENVIRONMENT: Literal['local', 'test', 'prod'] = 'local'
    
    # Database settings
    DATABASE_NAME: Optional[str] = None
    TABLE_NAME: Optional[str] = None
    MOTHERDUCK_TOKEN: Optional[str] = None

    # API Credentials
    STREAMS_CHARTS_CLIENT_ID: str
    STREAMS_CHARTS_TOKEN: str

    @property
    def database_name(self) -> str:
        if self.ENVIRONMENT in ('local', 'test'):
            return "test_db"
        if self.DATABASE_NAME is None:
            raise ValueError("DATABASE_NAME must be set in prod environment")
        return self.DATABASE_NAME

    @property
    def table_name(self) -> str:
        if self.ENVIRONMENT in ('local', 'test'):
            return "test_table"
        if self.TABLE_NAME is None:
            raise ValueError("TABLE_NAME must be set in prod environment")
        return self.TABLE_NAME