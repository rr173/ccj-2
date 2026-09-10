from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl, field_validator

MAX_EVENT_TYPE_LENGTH = 128
MAX_EVENT_TYPES_PER_DESTINATION = 100


def _normalize_optional_datetime(value: datetime | None) -> datetime | None:
    """Naive timestamps are interpreted as UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


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
    # Earliest time the event may be sent out. None means "as soon as its
    # per-destination queue position is reached".
    not_before: datetime | None = None

    @field_validator("event_type")
    @classmethod
    def normalize_event_type(cls, value: str) -> str:
        return _normalize_event_type(value)

    @field_validator("not_before")
    @classmethod
    def normalize_not_before(cls, value: datetime | None) -> datetime | None:
        return _normalize_optional_datetime(value)


class EventRescheduleIn(BaseModel):
    # New earliest send time. Null clears the schedule, so the event goes out
    # as soon as its per-destination queue position allows.
    not_before: datetime | None = None

    @field_validator("not_before")
    @classmethod
    def normalize_not_before(cls, value: datetime | None) -> datetime | None:
        return _normalize_optional_datetime(value)


class EventOut(BaseModel):
    id: UUID
    # Registered external source that pushed this event in (null only for
    # events created before inbound source authentication existed).
    source_id: UUID | None = None
    event_type: str
    dedupe_key: str
    payload: dict[str, Any]
    # Transport state:
    # unrouted: no destination subscribed to this type at ingest time;
    # pending: at least one delivery is still undelivered;
    # delivered: every delivery created for this event got a transport 2xx;
    # cancelled: the event was cancelled before anything was sent; its
    # remaining copies will never go out.
    status: str
    # Whole-event receipt state. Only success receipts on every fanned-out copy
    # make the whole event acknowledged:
    # pending: no copy has a receipt outcome yet;
    # partially_acknowledged: at least one copy is acknowledged and another is not;
    # acknowledged: every fanned-out copy is acknowledged.
    reconcile_status: str = "pending"
    delivery_count: int
    delivered_count: int
    # How many fanned-out copies have been acknowledged by a matching success receipt.
    acknowledged_count: int = 0
    # Copies still awaiting, transport-failed/pending, receipt-failed or timed out.
    unacknowledged_count: int = 0
    # Scheduled send gate copied onto every fanned-out copy; null = send as
    # soon as the per-destination queue reaches it.
    not_before: datetime | None = None
    cancelled_at: datetime | None = None
    created_at: datetime
    duplicate: bool = False


class DeliveryOut(BaseModel):
    id: UUID
    event_id: UUID | None = None
    destination_id: UUID
    destination_url: str | None = None
    destination_seq: int
    # pending: queued (possibly waiting for not_before); in_flight: being
    # delivered right now; delivered: transport 2xx; cancelled: the event was
    # cancelled before this copy went out — terminal, it will never be sent.
    status: str
    attempts: int
    next_attempt_at: datetime
    # Earliest time this copy may be sent; null = no schedule gate.
    not_before: datetime | None = None
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


class EventBulkRequeueOut(BaseModel):
    event_id: UUID
    requeued_count: int
    deliveries: list[RequeueOut]


class EventTraceOut(BaseModel):
    event: EventOut
    deliveries: list[DeliveryOut]
    attempts: list[DeliveryAttemptOut]
    receipts: list[ReceiptOut] = []


class RecoveryOut(BaseModel):
    destination: DestinationOut
    recovered: bool
    pending_deliveries_reset: int | None = None


# --- Inbound event sources ---------------------------------------------------

MAX_SOURCE_NAME_LENGTH = 128


class SourceIn(BaseModel):
    # A human-readable, unique name for the external system pushing events in.
    name: str = Field(..., min_length=1, max_length=MAX_SOURCE_NAME_LENGTH)


class SourceOut(BaseModel):
    """Registered source metadata. The secret is never returned here."""

    id: UUID
    name: str
    status: str  # active | disabled
    disabled_at: datetime | None = None
    key_rotated_at: datetime
    created_at: datetime

    model_config = {"from_attributes": True}


class SourceCreatedOut(SourceOut):
    # The secret is shown exactly once — on registration or key rotation.
    # It cannot be retrieved afterwards.
    secret: str


class SourceRotatedOut(BaseModel):
    source: SourceCreatedOut
    # Old signatures stop matching immediately after rotation.
    previous_key_rotated_at: datetime


class IngestionAttemptOut(BaseModel):
    id: int
    source_id: UUID | None = None
    source_name: str | None = None
    event_id: UUID | None = None
    dedupe_key: str | None = None
    event_type: str | None = None
    signed_at: datetime | None = None
    # accepted: stored and fanned out to current subscribers;
    # unrouted: stored but no destination subscribes to the type;
    # duplicate: same dedupe_key seen again, no new event was created;
    # source_unknown / source_disabled / bad_signature / stale_timestamp /
    # future_timestamp / invalid_timestamp / invalid_body: rejected at entry.
    disposition: str
    reason: str | None = None
    remote_addr: str | None = None
    received_at: datetime

    model_config = {"from_attributes": True}
