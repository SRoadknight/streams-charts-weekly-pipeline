from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict

class Config(BaseSettings):
    """Configuration settings for the application"""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # Environment settings
    ENVIRONMENT: Literal['local', 'prod'] = 'local'
    DESTINATION: Literal['md', 'local'] = 'local' if ENVIRONMENT == 'local' else 'md'
    
    # Database settings
    DATABASE_NAME: str
    TABLE_NAME: str
    MOTHERDUCK_TOKEN: str

    # API Credentials
    STREAMS_CHARTS_CLIENT_ID: str
    STREAMS_CHARTS_TOKEN: str

    @property
    def database_name(self) -> str:
        """Get appropriate database name based on environment"""
        if self.ENVIRONMENT == 'local':
            return f"{self.DATABASE_NAME}_test"
        return self.DATABASE_NAME

    @property
    def table_name(self) -> str:
        """Get appropriate table name based on environment"""
        if self.ENVIRONMENT == 'local':
            return f"{self.TABLE_NAME}_test"
        return self.TABLE_NAME