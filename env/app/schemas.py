from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl, field_validator

MAX_EVENT_TYPE_LENGTH = 128
MAX_EVENT_TYPES_PER_DESTINATION = 100

# A subscription condition ("订阅条件") is an arbitrary JSON object whose
# shape is validated by app.subfilters.validate_filter_spec at write time;
# Pydantic only enforces "a JSON object keyed by event type, each value an
# object". NULL / an absent field means "leave conditions unchanged" on an
# edit; per-key null on PATCH clears just that type's condition.
SubscriptionFilters = dict[str, dict[str, Any]]


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
    # Optional subscription conditions keyed by event type. On registration
    # filters only make sense together with event_types (a condition for a
    # type the address does not subscribe to is refused, 422). A type absent
    # from the map has no condition and receives every event of that type.
    filters: SubscriptionFilters | None = None

    @field_validator("filters")
    @classmethod
    def filters_must_be_objects(cls, value):
        if value is None:
            return None
        for event_type, spec in value.items():
            if not isinstance(event_type, str) or not isinstance(spec, dict):
                raise ValueError("filters must map event types to condition objects")
        return value

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
    # Replace the condition of each listed subscribed type: a condition
    # object sets it, null clears that type's condition (the address then
    # receives every event of that type). Types not named in the map keep
    # their current condition. None leaves all conditions untouched. Keys
    # must name types the address subscribes to after this edit (422
    # otherwise), and edits to conditions only affect later events.
    filters: dict[str, dict[str, Any] | None] | None = None

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
    # Current subscription conditions keyed by event type. Only types with a
    # condition appear here; an absent type means the address receives every
    # event of that type. Conditions are evaluated at fan-out time, so a
    # change here only ever affects later events.
    filters: dict[str, dict[str, Any]] = {}

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
    # Latest time each copy is allowed to complete transport. Null means no
    # such promise; copies still queued when it passes become terminal
    # deadline_expired. Delivered copies are never recalled.
    deliver_by: datetime | None = None
    # Only meaningful when the event type is gated (preview-consent). This is
    # the short text the preview is allowed to show; the real content stays in
    # payload and only goes out with the body after this address nods. Sending
    # it for a non-gated type is refused (422) rather than silently dropped.
    preview_payload: dict[str, Any] | None = None

    @field_validator("event_type")
    @classmethod
    def normalize_event_type(cls, value: str) -> str:
        return _normalize_event_type(value)

    @field_validator("not_before")
    @classmethod
    def normalize_not_before(cls, value: datetime | None) -> datetime | None:
        return _normalize_optional_datetime(value)

    @field_validator("deliver_by")
    @classmethod
    def normalize_deliver_by(cls, value: datetime | None) -> datetime | None:
        return _normalize_optional_datetime(value)


class EventRescheduleIn(BaseModel):
    # New earliest send time. Null clears the schedule, so the event goes out
    # as soon as its per-destination queue position allows.
    not_before: datetime | None = None

    @field_validator("not_before")
    @classmethod
    def normalize_not_before(cls, value: datetime | None) -> datetime | None:
        return _normalize_optional_datetime(value)


class EventDeliverByIn(BaseModel):
    # New latest-delivery promise. Null clears it for copies that are still
    # undelivered, so those copies go out normally.
    deliver_by: datetime | None = None

    @field_validator("deliver_by")
    @classmethod
    def normalize_deliver_by(cls, value: datetime | None) -> datetime | None:
        return _normalize_optional_datetime(value)


class EventDeliverByOut(BaseModel):
    event_id: UUID
    # Current latest-delivery promise on the event; null means the cutoff was
    # cleared and remaining undelivered copies proceed normally.
    deliver_by: datetime | None = None
    # Copies the new value was snapshotted onto. Delivered/terminal copies are
    # never rewritten and therefore are not counted here.
    updated_count: int
    updated_real_count: int = 0
    updated_shadow_count: int = 0


class CorrectionIn(BaseModel):
    # A correction ("补一笔") submitted against an already-accepted event. It
    # is an additional entry of its own — the original event's copies that
    # already went out are never recalled or rewritten. The correction is
    # fanned out only to the destinations the original was really delivered
    # to; destinations that never got the original do not get the correction.
    #
    # dedupe_key is the correction's own idempotency key (globally unique,
    # shared with the event key namespace): re-sending the same correction is
    # applied exactly once and returns the original correction as a
    # duplicate. payload is the corrected business payload.
    dedupe_key: str = Field(..., min_length=1, max_length=256)
    payload: dict[str, Any]


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
    # remaining copies will never go out;
    # failed: every live copy stopped and at least one is a correction copy
    # whose send attempt failed terminally (only correction events can reach
    # this state — ordinary copies retry or dead-letter instead);
    # deadline_expired: no copy is still queued and at least one for-real copy
    # was closed because its deliver_by cutoff passed without transport.
    status: str
    # Set only on correction events: the original event this one corrects.
    # A correction is an additional entry fanned out to the destinations the
    # original was really delivered to; the original event and its copies are
    # never rewritten. Null on ordinary events.
    corrects_event_id: UUID | None = None
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
    # Latest transport time chosen at submission (or changed later before
    # delivery). Null means "no cutoff".
    deliver_by: datetime | None = None
    cancelled_at: datetime | None = None
    # Preview-consent gate ("预告 + 点头才给正文"): true when this event's type
    # is gated. A gated event fans out as a preview/body pair per subscriber;
    # a body waits for that destination's own nod before its deadline.
    preview_gated: bool = False
    preview_payload: dict[str, Any] | None = None
    # Gated for-real bodies waiting on, released by, or terminally closed by
    # their destination's preview decision. release_closed counts the bodies
    # that never went out (denied / expired / voided); they are not pending and
    # never delivered.
    bodies_waiting_count: int = 0
    bodies_released_count: int = 0
    bodies_denied_count: int = 0
    bodies_expired_count: int = 0
    bodies_voided_count: int = 0
    # The same counters for observe-only ("shadow") subscribers, kept separate.
    shadow_bodies_waiting_count: int = 0
    shadow_bodies_released_count: int = 0
    shadow_bodies_closed_count: int = 0
    # Confirmed subscribers whose own subscription condition withheld this
    # exact body: no delivery was created for them, so they are neither
    # "unrouted" (nobody subscribed) nor pending confirmation. Kept separate
    # for for-real and observe-only subscribers; each judgement is also in the
    # event's filter_evaluations (trace) with its condition snapshot.
    filtered_out_count: int = 0
    shadow_filtered_out_count: int = 0
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
    # Undelivered for-real bodies closed at the deliver-by cutoff. They never
    # went out and are deliberately neither "delivered" nor "unrouted".
    deadline_expired_count: int = 0
    # The same cutoff outcomes for observe-only subscribers.
    shadow_deadline_expired_count: int = 0
    # Correction copies whose send attempt failed terminally (corrections are
    # not retried in place and never count toward the address's failure
    # tally). Always zero on ordinary events — their copies never take this
    # state.
    failed_count: int = 0
    shadow_failed_count: int = 0
    # Relay chain ("接力") stations that were never reached because an earlier
    # station stopped (failure receipt / reconcile timeout / dead-letter /
    # supersede). These copies never went out and are excluded from
    # delivery_count, like superseded copies.
    relay_skipped_count: int = 0
    # Present and populated only for events that walked a relay chain; null on
    # every ordinary event. Shows the snapshotted chain version, each station
    # in order with its copy's state, which stations acknowledged, the current
    # ("walking") station, and where/why the run stopped if it did.
    relay: "RelayRunOut | None" = None


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
    # manual revive puts it back at its original queue position;
    # failed: a correction copy whose send attempt failed — terminal for that
    # copy; corrections are not retried in place, the failure is not charged
    # to the address's consecutive-failure tally (no isolation) and the copy
    # no longer blocks later copies of its destination.
    # deadline_expired: this copy was still undelivered when its deliver_by
    # cutoff passed; terminal, it never went out and is not delivered.
    status: str
    attempts: int
    next_attempt_at: datetime
    # Earliest time this copy may be sent; null = no schedule gate.
    not_before: datetime | None = None
    # Latest time this copy is allowed to complete transport; null = no cutoff.
    deliver_by: datetime | None = None
    # Set when this copy was closed because deliver_by passed without
    # transport completion. Such a copy is terminal and was never delivered.
    deliver_by_expired_at: datetime | None = None
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
    # Snapshot of the subscription condition this copy was fanned out under.
    # Null means the subscription carried no condition (every body matches);
    # a non-null condition held for this body — a non-matching body creates no
    # copy at all (its judgement is in the event's filter_evaluations).
    filter_spec: dict[str, Any] | None = None
    # Preview-consent gate ("预告 + 点头才给正文"):
    # preview: the notice queued ahead of the body for a gated event (no real
    #          payload, no receipt reconciliation);
    # body:    an ordinary copy, or the content copy of a gated event.
    phase: str = "body"
    # Gated body only:
    # held            — waiting for the preview to land and this address to nod;
    # released        — this address nodded in time, the body may go out;
    # release_denied  — this address answered "no"; terminal, never sent;
    # release_expired — no nod before the deadline; terminal, never sent;
    # release_voided  — voided manually or because its preview never made it.
    # Null on ordinary copies and on previews.
    release_state: str | None = None
    preview_delivery_id: UUID | None = None
    body_delivery_id: UUID | None = None
    # From when the address may decide until when (written when the preview
    # really completes transport; null while the notice is still queued).
    consent_deadline: datetime | None = None
    consent_timeout_seconds: int | None = None
    released_at: datetime | None = None
    voided_at: datetime | None = None
    # manual | preview_dead_lettered | preview_superseded (release_voided).
    void_reason: str | None = None
    # Relay chain ("接力") snapshot: set together on station copies, null on
    # ordinary copies. relay_station_no is the 1-based position in the chain
    # version this event fanned out under. relay_skip_* are set only on
    # terminal relay_skipped rows (never reached because an earlier station
    # stopped).
    relay_chain_id: UUID | None = None
    relay_station_no: int | None = None
    relay_skipped_at: datetime | None = None
    relay_skip_reason: str | None = None
    relay_stopped_by_delivery_id: UUID | None = None

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
    # Corrections submitted against this event. Each is its own event with
    # its own copies and reconciliation (trace it via its own
    # /v1/events/{id}/trace); the original event above is never rewritten by
    # them. Empty when nothing was ever corrected — in particular for events
    # that never went out anywhere.
    corrections: list[EventOut] = []
    # One row per condition-carrying confirmed subscriber judged at fan-out.
    # The per-address routing list below additionally covers unconditional and
    # not-yet-confirmed subscribers, so every receiving address's pass/withhold
    # outcome is visible in one place without cross-referencing tables.
    filter_evaluations: list["SubscriptionFilterEvaluationOut"] = []
    # One row per relevant address (it has a copy, a condition judgement, a
    # current subscription, or is otherwise connected to this event type):
    # pass / withheld / unconfirmed / unsubscribed, all in one list.
    routing: list["EventRoutingOut"] = []


class SubscriptionFilterEvaluationOut(BaseModel):
    id: int
    event_id: UUID
    destination_id: UUID
    event_type: str
    # True: this address's own condition held for this body, a delivery was
    # created; false: it did not, no delivery was created and nothing was sent
    # to this address (it must never read as delivered).
    matched: bool
    # Exact condition snapshot that was judged, so the decision is auditable
    # even after the subscription condition is later replaced.
    filter_spec: dict[str, Any]
    observe_only: bool = False
    created_at: datetime

    model_config = {"from_attributes": True}


# Per-destination routing outcome for ONE event, collected from everything the
# system knew about the address at fan-out: its current subscription (and
# condition snapshot), whether this exact body passed that address's OWN
# condition, and the body copy it ended up with (null when no copy was made).
# This is the row-per-address answer to "did each receiving address pass its
# condition" — it must never collapse a withheld address into "nobody
# subscribed" and must stay queryable even when part of the surrounding data
# is missing.
class EventRoutingOut(BaseModel):
    destination_id: UUID
    destination_url: str | None = None
    observe_only: bool = False
    # Subscribed to the event's type at query time.
    subscribed: bool
    # The address's CURRENT condition for the type (null = no condition, the
    # address receives every event of the type). For a judgement taken at
    # fan-out time see `matched` / `filter_spec` (the snapshot then judged).
    filter_spec: dict[str, Any] | None = None
    # confirmed: handshake done and the address was an eligible subscriber at
    #            fan-out, so its condition WAS evaluated:
    #              no_condition  — it subscribes without a condition, receives;
    #              matched       — its own condition held, a copy was created;
    #              filtered      — its own condition withheld the body, no copy;
    # unconfirmed: it subscribed but had not completed the handshake then — no
    #              copy and no condition evaluation (pending_confirmation);
    # unsubscribed: it is no longer subscribed to the type (no copy).
    outcome: str
    # The fan-out verdict for this address when it carried a condition at
    # ingest/correction time; null for unconditional / unconfirmed / no-longer-
    # subscribed addresses.
    matched: bool | None = None
    # Condition snapshot that was actually judged (null when none was judged).
    evaluated_filter_spec: dict[str, Any] | None = None
    # The body copy this address got (the preview of a gated pair is reached
    # via its preview_delivery_id); null when no copy was created.
    body_delivery_id: UUID | None = None
    body_status: str | None = None

    model_config = {"from_attributes": True}


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
    # filtered: stored, subscribers exist and are confirmed, but each one's
    #   own subscription condition withheld the body — no copies created, the
    #   per-address judgements are in subscription_filter_evaluations;
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


# --- Preview-consent gate ("预告 + 点头才给正文") ---------------------------


class PreviewPolicyIn(BaseModel):
    # How long after the preview really reaches the address the address has to
    # nod before the body is voided. Counted from the preview's delivered
    # time, never from ingest/queue time; must be at least 1 second. Omitted
    # falls back to the service default (PREVIEW_CONSENT_TIMEOUT_SECONDS).
    consent_timeout_seconds: int | None = Field(default=None, ge=1, le=2_147_483_647)


class PreviewPolicyOut(BaseModel):
    event_type: str
    consent_timeout_seconds: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    # False when no policy row exists for the type (clear, or a GET miss):
    # the type then behaves like an ordinary type, body sent straight away.
    gated: bool = True

    model_config = {"from_attributes": True}


class ConsentIn(BaseModel):
    # approve ("点头"): release this address's own body when the decision
    # arrives in time; deny ("不要"): close that body for good. Exactly one
    # effective decision per (event, destination); a repeated same answer is a
    # duplicate and an opposite answer a conflict, neither changes anything.
    decision: Literal["approve", "deny"]


class ConsentOut(BaseModel):
    event_id: UUID
    destination_id: UUID
    delivery_id: UUID | None = None
    # released: the nod applied in time, the body is free to go out;
    # denied: "不要" applied — the body never goes out;
    # duplicate: the same answer was already applied once (counted once);
    # conflict: an opposite answer had already been applied;
    # late_ignored: arrived after the deadline / after the body was closed —
    #   recorded but never revives it;
    # preview_not_delivered: the notice has not reached this address yet;
    # orphan: no gated body exists for this (event, destination).
    disposition: str
    release_state: str | None = None


class ReleaseGateOut(DeliveryOut):
    """A gated body with its preview's state for the gate query endpoint."""

    # Preview ("预告") transport state and when it really landed; null until
    # the notice completes transport. The consent window only opens then.
    preview_status: str | None = None
    preview_delivered_at: datetime | None = None
    preview_delivery: UUID | None = None


class ReleaseGateDecisionOut(BaseModel):
    id: int
    destination_id: UUID
    event_id: UUID | None = None
    delivery_id: UUID | None = None
    decision: str
    disposition: str
    reason: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


class BodyVoidOut(BaseModel):
    delivery_id: UUID
    # False when the body was already terminally closed (idempotent void).
    voided: bool
    status: str
    release_state: str


# --- Per-event-type relay chains ("接力") -----------------------------------


class RelayChainIn(BaseModel):
    # Ordered receiving addresses. The event goes to station 1 first; each
    # later station only after the immediately previous station carries a
    # matching success receipt. At least one station is required and the same
    # destination cannot occupy two stations of the same chain. Re-defining
    # the order creates a new chain version: events already on their way keep
    # the stations they set out with, only later accepted events follow the
    # new order.
    destination_ids: list[UUID] = Field(..., min_length=1)

    @field_validator("destination_ids")
    @classmethod
    def no_duplicate_stations(cls, value: list[UUID]) -> list[UUID]:
        if len(set(value)) != len(value):
            raise ValueError(
                "the same destination cannot occupy two stations of one chain"
            )
        return value


class RelayStationOut(BaseModel):
    station_no: int
    destination_id: UUID


class RelayChainOut(BaseModel):
    id: UUID
    event_type: str
    version: int
    active: bool = True
    created_at: datetime
    stations: list[RelayStationOut] = []

    model_config = {"from_attributes": True}


class RelayRunStationOut(BaseModel):
    # One row per station of the chain version this event set out under, in
    # station order. acknowledged = the station's copy matched a success
    # receipt (this is what opens the next station); delivered = transport
    # reached it at least once; reached = the copy is past pure queueing.
    # skip_reason / stopped_by_delivery_id identify why a later station was
    # never sent.
    station_no: int
    destination_id: UUID
    destination_url: str | None = None
    delivery_id: UUID
    status: str
    reconcile_state: str = "none"
    acknowledged: bool = False
    delivered: bool = False
    reached: bool = False
    skip_reason: str | None = None
    skipped_at: datetime | None = None
    stopped_by_delivery_id: UUID | None = None

    model_config = {"from_attributes": True}


class RelayRunOut(BaseModel):
    chained: bool = True
    event_type: str
    chain_id: UUID
    # Version of the chain the event fanned out under (kept even if the type's
    # current chain was later re-defined; the run never changes stations).
    chain_version: int
    # Whether THIS version is still the type's current active chain. False
    # after a later re-definition, while existing runs keep using it.
    chain_active: bool
    station_count: int
    acknowledged_count: int = 0
    delivered_count: int = 0
    skipped_count: int = 0
    # First station that has not acknowledged yet (the station the run is
    # walking at right now); null once the run completed.
    current_station_no: int | None = None
    # True once a station stopped with the run unfinished: later stations are
    # relay_skipped and will never be backfilled.
    stopped: bool = False
    stopped_at_station_no: int | None = None
    # delivery_attempts_exhausted | receipt_timeout_exhausted |
    # receipt_failure_exhausted | receipt_failed | receipt_timeout |
    # superseded
    stop_reason: str | None = None
    completed: bool = False
    stations: list[RelayRunStationOut] = []
