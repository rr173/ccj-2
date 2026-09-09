from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = Field(
        default="postgresql+psycopg://event:event@localhost:5432/events"
    )
    db_pool_size: int = 10
    db_max_overflow: int = 5

    # Worker settings
    worker_concurrency: int = 4
    poll_interval_seconds: float = 0.5
    http_timeout_seconds: float = 10.0
    claim_lease_seconds: float = 60.0
    lease_heartbeat_seconds: float = 15.0
    retry_backoff_base_seconds: float = 2.0
    retry_backoff_max_seconds: float = 3600.0
    failure_threshold: int = 5
    quarantine_seconds: int = 900
    max_response_body_bytes: int = 2048


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
