from typing import Literal
import boto3
import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class CustomBaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

class Config(CustomBaseSettings):
    ENVIRONMENT: Literal['dev', 'prod'] = 'dev'

    STREAMS_CHARTS_CLIENT_ID: str
    STREAMS_CHARTS_TOKEN: str

    AWS_PROFILE: str | None = None
    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None
    AWS_REGION: str 

    @property
    def glue_database(self) -> str:
        return f"weekly_stream_data_{self.ENVIRONMENT}"
    
    @property
    def glue_table(self) -> str:
        return "weekly_data"
    
    @property
    def s3_bucket(self) -> str:
        return f"streamers-data-lake-{self.ENVIRONMENT}"
    
    @property
    def s3_prefix(self) -> str:
        return "weekly_streamers_data"
    
    @property
    def aws_profile_available(self) -> bool:
        return self.AWS_PROFILE and self.AWS_PROFILE in boto3.Session().available_profiles  
    
    def get_aws_session(self) -> boto3.Session:
        return boto3.Session(
            aws_access_key_id=self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            region_name=self.AWS_REGION
        )

settings = Config()