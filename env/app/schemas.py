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
    # Observe-only ("shadow"): the destination still gets a copy of every
    # subscribed event with its own delivery/retry/dead-letter lifecycle, but
    # its receipts never count toward the whole event's acknowledgement and
    # event-level requeue never selects its copies. None on re-registration
    # keeps the current flag; a brand-new destination defaults to for-real.
    observe_only: bool | None = None

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


class DestinationPatchIn(BaseModel):
    # Re-locating a destination (new url) re-arms the activation handshake:
    # it becomes unconfirmed immediately, queued copies aimed at the old
    # location are superseded (never sent, not backfilled later) and copies
    # already sent or in flight are left alone. None keeps the current url.
    url: HttpUrl | None = None
    # As on registration: None leaves subscriptions untouched, a list
    # (including the empty list) replaces the whole set.
    event_types: list[str] | None = None
    # None leaves the observe-only flag untouched; true/false retoggles it.
    # Toggling only changes copies fanned out afterwards — existing copies
    # keep the flag snapshot taken when they were created.
    observe_only: bool | None = None

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


class DestinationPauseIn(BaseModel):
    # Operator-marked "not receiving" window for this address. paused_from
    # defaults to "right now" (database clock) when omitted; paused_until
    # omitted means "not receiving until explicitly resumed". At least one of
    # the two must be provided — clearing the window is POST .../resume.
    paused_from: datetime | None = None
    paused_until: datetime | None = None

    @field_validator("paused_from", "paused_until")
    @classmethod
    def normalize_window(cls, value: datetime | None) -> datetime | None:
        return _normalize_optional_datetime(value)


class DestinationOut(BaseModel):
    id: UUID
    url: str
    status: str
    failure_count: int
    event_types: list[str] = []
    recoverable_at: datetime | None = None
    created_at: datetime
    # Activation handshake:
    # pending: not confirmed yet — events are ingested but nothing is fanned
    #          out to this destination (old events are never backfilled);
    # confirmed: the last challenge round was answered correctly in time.
    confirmation_state: str = "pending"
    # When the current unconfirmed round expires; null once confirmed.
    challenge_expires_at: datetime | None = None
    confirmed_at: datetime | None = None
    # Bumped whenever confirmation is re-armed (registration / URL change).
    confirmation_generation: int = 1
    confirmation_round: int = 1
    next_probe_at: datetime | None = None
    # True: a shadow that receives copies but whose receipts never decide the
    # whole event's acknowledgement. False (default): a for-real subscriber.
    observe_only: bool = False
    # Operator-marked "not receiving" window ([paused_from, paused_until);
    # both null = no window, paused_until null = "until explicitly resumed".
    # While `paused` is true the worker never claims this destination's
    # copies: they wait in their original queue positions, nothing is
    # attempted (so nothing counts toward failure isolation) and no reconcile
    # countdown runs for them. Other destinations are unaffected.
    paused_from: datetime | None = None
    paused_until: datetime | None = None
    paused: bool = False

    model_config = {"from_attributes": True}


class DestinationResumeOut(BaseModel):
    destination: DestinationOut
    # False when the address had no window to clear (idempotent resume).
    resumed: bool


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
    # Whole-event receipt state. Decided by for-real copies only:
    # pending: no for-real copy has a success receipt yet;
    # partially_acknowledged: at least one for-real copy is acknowledged but
    #   the event's requirement is not yet met;
    # acknowledged: the required number of for-real copies
    #   (required_ack_count below) carried matching success receipts — once
    #   reached, later timeouts/failure receipts/dead-letters of the remaining
    #   copies can never move it back.
    reconcile_status: str = "pending"
    # Type-wide acknowledgement threshold ("认完门槛") configured right now,
    # if any. Null means the default rule (every for-real copy). Observe-only
    # ("shadow") copies never count.
    ack_threshold: int | None = None
    # This event's own snapshot: how many for-real success receipts are enough
    # for it (the configured threshold capped at the for-real subscribers
    # present at ingest time; otherwise every fanned-out for-real copy). A zero
    # snapshot (unrouted / shadow-only / pending-confirmation) never reads as
    # acknowledged.
    required_ack_count: int = 0
    # True once acknowledged_count reached required_ack_count; the standing is
    # monotonic afterwards.
    acknowledged_quorum: bool = False
    delivery_count: int
    delivered_count: int
    # How many fanned-out copies have been acknowledged by a matching success receipt.
    acknowledged_count: int = 0
    # Copies still awaiting, transport-failed/pending, receipt-failed or timed out.
    unacknowledged_count: int = 0
    # Observe-only ("shadow") copies are reported separately: they still go
    # out and reconcile on their own, but their receipts/timeouts never change
    # reconcile_status or the for-real counts above, and event-level requeue
    # never selects them.
    shadow_delivery_count: int = 0
    shadow_delivered_count: int = 0
    shadow_acknowledged_count: int = 0
    shadow_unacknowledged_count: int = 0
    # Extra per-state shadow counters, kept separate from the for-real ones.
    shadow_pending_count: int = 0
    shadow_superseded_count: int = 0
    shadow_dead_lettered_count: int = 0
    # Scheduled send gate copied onto every fanned-out copy; null = send as
    # soon as the per-destination queue reaches it.
    not_before: datetime | None = None
    cancelled_at: datetime | None = None
    created_at: datetime
    duplicate: bool = False
    # Fanned-out copies abandoned because their destination changed location
    # before they could be sent. They never went out and are not retried or
    # backfilled; they are excluded from delivery_count above.
    superseded_count: int = 0
    # Copies parked in the dead-letter area (transport failures exhausted, or
    # receipts kept not matching after redelivery). They never go out on their
    # own and are not acked; a manual revive puts them back in queue.
    dead_lettered_count: int = 0


class DeliveryOut(BaseModel):
    id: UUID
    event_id: UUID | None = None
    destination_id: UUID
    destination_url: str | None = None
    destination_seq: int
    # Idempotency key / business key of the copy (matches the event's
    # dedupe_key); included so a dead-letter row identifies "which copy"
    # without a second lookup.
    dedupe_key: str
    event_type: str | None = None
    # pending: queued (possibly waiting for not_before); in_flight: being
    # delivered right now; delivered: transport 2xx; cancelled: the event was
    # cancelled before this copy went out — terminal, it will never be sent;
    # superseded: the destination changed location before this copy was sent
    # — terminal, it never went out and is not retried or backfilled;
    # dead_lettered: repeated transport failures (or repeatedly unmatched
    # receipts after redelivery) gave up on this one copy — terminal until a
    # manual revive puts it back at its original queue position.
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
    # Consecutive transport failures for this copy (reset by a 2xx or a
    # manual revive). When it reaches the limit the copy is dead-lettered.
    consecutive_failures: int = 0
    # Dead-letter area:
    # null while the copy is live; when parked, one of
    # delivery_attempts_exhausted / receipt_timeout_exhausted /
    # receipt_failure_exhausted. dead_lettered_at says when it was parked.
    dead_letter_reason: str | None = None
    dead_lettered_at: datetime | None = None
    # Destination activation generation this copy was fanned out under.
    # A URL change bumps the destination generation; older copies then fail
    # the claim gate and queued ones become superseded.
    confirmation_generation: int = 1
    # True: this copy belongs to an observe-only ("shadow") subscriber. It is
    # delivered and reconciled on its own, but its receipt outcome never
    # changes the whole event's reconcile_status or its for-real counts.
    observe_only: bool = False

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


# --- Dead-letter area --------------------------------------------------------


class DeadLetterSummaryOut(BaseModel):
    total: int = 0
    delivery_attempts_exhausted: int = 0
    receipt_timeout_exhausted: int = 0
    receipt_failure_exhausted: int = 0


class DeadLetterReviveOut(BaseModel):
    delivery_id: UUID
    destination_id: UUID
    destination_seq: int
    # Previous dead-letter reason, kept in the response so the operator sees
    # why the copy had been parked.
    dead_letter_reason: str
    revived: bool


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
    # True when the event has already reached its acknowledgement requirement
    # (per-type threshold or all for-real copies): the whole-event list is
    # deliberately not pulled and requeued_count stays 0.
    already_acknowledged: bool = False


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
    # accepted: stored and fanned out to current confirmed subscribers;
    # unrouted: stored but no destination subscribes to the type;
    # pending_confirmation: stored, but every subscriber was still unconfirmed
    #   at ingest time — no copies created, nothing sent;
    # duplicate: same dedupe_key seen again, no new event was created;
    # source_unknown / source_disabled / bad_signature / stale_timestamp /
    # future_timestamp / invalid_timestamp / invalid_body: rejected at entry.
    disposition: str
    reason: str | None = None
    remote_addr: str | None = None
    received_at: datetime

    model_config = {"from_attributes": True}


# --- Destination activation handshake ---------------------------------------

class ConfirmationIn(BaseModel):
    # Echo of the challenge token delivered in the handshake probe. It may
    # also arrive as the X-Confirmation-Challenge header (used by receivers
    # that answer by HTTP 2xx on the probe itself).
    challenge: str = Field(..., min_length=1, max_length=256)


class ConfirmationOut(BaseModel):
    destination_id: UUID
    confirmation_state: str  # confirmed | pending
    confirmed_at: datetime | None = None
    confirmation_round: int
    # What this call did:
    # confirmed: challenge matched before the deadline, destination is live;
    # already_confirmed: the destination had finished an earlier round;
    # invalid: challenge did not match this round;
    # expired: the round deadline passed; a fresh round was issued.
    disposition: str
    challenge_expires_at: datetime | None = None


class ReissueChallengeOut(BaseModel):
    destination_id: UUID
    confirmation_state: str
    confirmation_round: int
    challenge_expires_at: datetime
    reissued: bool


class ConfirmationAttemptOut(BaseModel):
    id: int
    destination_id: UUID
    confirmation_round: int
    # challenge: a probe sent to the destination's URL;
    # echo: an answer received at the confirmation endpoint;
    # expired: a round ended without a correct answer.
    kind: str
    # sent (probe dispatched) | confirmed (probe echoed 2xx or echo matched) |
    # failed (transport/non-2xx probe) | invalid (wrong echo) | expired.
    result: str
    status_code: int | None = None
    response_excerpt: str | None = None
    error: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


# --- Per-event-type acknowledgement threshold ("认完门槛") -----------------


class AckThresholdIn(BaseModel):
    # Number of FOR-REAL destinations whose matching success receipts are
    # enough to mark an event of this type acknowledged as a whole.
    # Observe-only ("shadow") copies never count. The effective requirement
    # for an event is this value capped at the number of for-real confirmed
    # subscribers present when the event is ingested, snapshotted onto the
    # event. Must be >= 1; only the type-wide threshold can be changed later,
    # never the snapshot of an already-accepted event.
    #
    # The field is required (an empty body is a 422, not an accidental clear)
    # but may be explicitly null: {"ack_threshold": null} on PUT clears a
    # configured threshold and restores the default all-for-real rule, same as
    # DELETE on the resource.
    ack_threshold: int | None = Field(..., ge=1, le=2_147_483_647)


class AckThresholdOut(BaseModel):
    event_type: str
    # Configured threshold; null on a response to a clear request means the
    # type now uses the default all-for-real rule.
    ack_threshold: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    # False when this response reports that no threshold is configured (after a
    # clear, or from the upsert endpoint when the row was deleted).
    configured: bool = True

    model_config = {"from_attributes": True}
