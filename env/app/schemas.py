from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl, field_validator

MAX_EVENT_TYPE_LENGTH = 128
MAX_EVENT_TYPES_PER_DESTINATION = 100


def _normalize_event_type(value: str) -> str:
    event_type = value.strip()
    if not event_type:
        raise ValueError("event_type must be a non-empty string")
    if len(event_type) > MAX_EVENT_TYPE_LENGTH:
        raise ValueError(
            f"event_type must be at most {MAX_EVENT_TYPE_LENGTH} characters"
        )
    return event_type


class DestinationIn(BaseModel):
    url: HttpUrl
    # None means "keep the current subscriptions" when re-registering an
    # existing URL; a list (even empty) replaces the whole set.
    event_types: list[str] | None = None

    @field_validator("event_types")
    @classmethod
    def normalize_event_types(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        normalized: list[str] = []
        for item in value:
            event_type = _normalize_event_type(item)
            if event_type not in normalized:
                normalized.append(event_type)
        if len(normalized) > MAX_EVENT_TYPES_PER_DESTINATION:
            raise ValueError(
                f"a destination can subscribe to at most "
                f"{MAX_EVENT_TYPES_PER_DESTINATION} event types"
            )
        return normalized


class DestinationOut(BaseModel):
    id: UUID
    url: str
    status: str
    failure_count: int
    event_types: list[str] = []
    recoverable_at: datetime | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


class EventIn(BaseModel):
    event_type: str
    dedupe_key: str = Field(..., min_length=1, max_length=256)
    payload: dict[str, Any]

    @field_validator("event_type")
    @classmethod
    def normalize_event_type(cls, value: str) -> str:
        return _normalize_event_type(value)


class EventOut(BaseModel):
    id: UUID
    event_type: str
    dedupe_key: str
    payload: dict[str, Any]
    # unrouted: no destination subscribed to this type at ingest time;
    # pending: at least one delivery is still undelivered;
    # delivered: every delivery created for this event succeeded.
    status: str
    delivery_count: int
    delivered_count: int
    # How many fanned-out copies have been acknowledged by a matching receipt.
    acknowledged_count: int = 0
    created_at: datetime
    duplicate: bool = False


class DeliveryOut(BaseModel):
    id: UUID
    event_id: UUID | None = None
    destination_id: UUID
    destination_url: str | None = None
    destination_seq: int
    status: str
    attempts: int
    next_attempt_at: datetime
    last_error: str | None = None
    created_at: datetime
    updated_at: datetime
    delivered_at: datetime | None = None
    # Reconciliation lifecycle of this copy:
    # none -> awaiting -> acknowledged | receipt_failed | timed_out
    reconcile_state: str = "none"
    reconcile_deadline: datetime | None = None
    reconciled_at: datetime | None = None
    receipt_result: str | None = None
    requeue_count: int = 0

    model_config = {"from_attributes": True}


class DeliveryAttemptOut(BaseModel):
    id: int
    delivery_id: UUID | None = None
    event_id: UUID | None = None
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


class ReceiptIn(BaseModel):
    # The receiver echoes back the destination_id and dedupe_key it was given
    # in the delivery, plus whether its own processing succeeded.
    destination_id: UUID
    dedupe_key: str = Field(..., min_length=1, max_length=256)
    result: Literal["success", "failure"]


class ReceiptOut(BaseModel):
    id: UUID
    destination_id: UUID
    dedupe_key: str
    result: str
    delivery_id: UUID | None = None
    # applied: matched a delivery still inside its reconciliation window;
    # duplicate: the delivery was already reconciled, counted exactly once;
    # late: arrived after the reconcile deadline, recorded but NOT applied;
    # orphan: no delivery exists for (destination_id, dedupe_key);
    # premature: the delivery has not completed transport yet.
    disposition: str
    received_at: datetime

    model_config = {"from_attributes": True}


class ReconciliationSummaryOut(BaseModel):
    awaiting: int = 0
    acknowledged: int = 0
    receipt_failed: int = 0
    timed_out: int = 0


class RequeueOut(BaseModel):
    delivery_id: UUID
    destination_id: UUID
    destination_seq: int
    requeued: bool


class BulkRequeueOut(BaseModel):
    destination_id: UUID
    requeued_count: int


class EventTraceOut(BaseModel):
    event: EventOut
    deliveries: list[DeliveryOut]
    attempts: list[DeliveryAttemptOut]
    receipts: list[ReceiptOut] = []


class RecoveryOut(BaseModel):
    destination: DestinationOut
    recovered: bool
    pending_deliveries_reset: int | None = None
