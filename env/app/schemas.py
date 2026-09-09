from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl


class DestinationIn(BaseModel):
    url: HttpUrl


class DestinationOut(BaseModel):
    id: UUID
    url: str
    status: str
    failure_count: int
    recoverable_at: datetime | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


class EventIn(BaseModel):
    destination_url: HttpUrl
    dedupe_key: str = Field(..., min_length=1, max_length=256)
    payload: dict[str, Any]


class EventOut(BaseModel):
    id: UUID
    destination_id: UUID
    dedupe_key: str
    payload: dict[str, Any]
    destination_seq: int
    status: str
    attempts: int
    next_attempt_at: datetime
    last_error: str | None = None
    created_at: datetime
    updated_at: datetime
    delivered_at: datetime | None = None
    duplicate: bool = False


class DeliveryAttemptOut(BaseModel):
    id: int
    event_id: UUID
    destination_id: UUID
    attempt_no: int
    started_at: datetime
    finished_at: datetime
    success: bool
    status_code: int | None = None
    response_excerpt: str | None = None
    error: str | None = None
    lost_lease: bool = False

    model_config = {"from_attributes": True}


class EventTraceOut(BaseModel):
    event: EventOut
    attempts: list[DeliveryAttemptOut]


class RecoveryOut(BaseModel):
    destination: DestinationOut
    recovered: bool
    pending_events_reset: int | None = None
