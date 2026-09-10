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
    # A copy that fails this many times *consecutively* (a 2xx resets the
    # streak, and a manual revive starts a fresh one) stops being retried and
    # is parked in the dead-letter area as 'dead_lettered'. It never goes out
    # again by itself and no longer blocks later copies of the same address.
    max_delivery_attempts: int = 10
    # How many times an unreconciled copy (timed out / failure receipt) may be
    # sent back out via requeue before "the receipt never matches" is treated
    # as terminal: the next timeout/failure receipt parks it in the dead-letter
    # area. A manual revive resets the cycle budget.
    max_requeue_cycles: int = 3

    # Receipt reconciliation settings
    receipt_timeout_seconds: float = 300.0
    reconcile_sweep_interval_seconds: float = 5.0
    receipt_delivery_grace_seconds: float = 2.0

    # Destination activation handshake settings. A newly registered or
    # re-located destination starts unconfirmed; the confirmer probes it with
    # a one-time challenge that must be echoed back correctly before the round
    # deadline. Probes use their own exponential backoff inside a round; an
    # unanswered round expires and a fresh challenge round begins.
    confirm_timeout_seconds: float = 300.0
    confirm_backoff_base_seconds: float = 2.0
    confirm_backoff_max_seconds: float = 60.0
    confirm_poll_interval_seconds: float = 1.0
    # Set false for extra worker replicas so only one process sends probes.
    confirmation_enabled: bool = True

    # Inbound source authentication settings. Every event POST must carry a
    # source id, a send timestamp and an HMAC signature made with that
    # source's secret.
    # How old (or how far in the future) the signed send timestamp is allowed
    # to be. Older events look like replays; future-dated ones look like a
    # sender clock that is wrong (or an attempted replay window abuse).
    ingest_max_age_seconds: float = 300.0
    ingest_max_future_skew_seconds: float = 60.0


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
