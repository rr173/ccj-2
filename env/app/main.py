import json
import logging
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

logger = logging.getLogger("ingest-api")

from app.confirmation import (
    DESTINATION_CONFIRM_COLUMNS,
    apply_echo,
    arm_round,
)
from app.db import engine, get_db
from app.ingest_auth import (
    ACCEPTED,
    DUPLICATE,
    FILTERED,
    INVALID_BODY,
    PENDING_CONFIRMATION,
    SOURCE_DISABLED,
    UNROUTED,
    AdmissionError,
    authenticate,
    generate_secret,
    log_attempt,
    parse_event_body,
)
from app.models import init_db
from app import release as release_gate
from app.config import settings
from app.receipts import ingest_receipt
from app.subfilters import (
    FilterSpecError,
    evaluate as evaluate_filter,
    validate_filter_spec,
)
from app.schemas import (
    AckThresholdIn,
    AckThresholdOut,
    BodyVoidOut,
    BulkRequeueOut,
    ConfirmationAttemptOut,
    ConfirmationIn,
    ConfirmationOut,
    ConsentIn,
    ConsentOut,
    CorrectionIn,
    DeadLetterReviveOut,
    DeadLetterSummaryOut,
    DeliveryOut,
    DestinationIn,
    DestinationOut,
    DestinationPatchIn,
    DestinationPauseIn,
    DestinationResumeOut,
    EventBulkRequeueOut,
    EventIn,
    EventOut,
    EventRescheduleIn,
    EventTraceOut,
    IngestionAttemptOut,
    PreviewPolicyIn,
    PreviewPolicyOut,
    ReceiptIn,
    ReceiptOut,
    ReconciliationSummaryOut,
    RecoveryOut,
    ReleaseGateDecisionOut,
    ReleaseGateOut,
    ReissueChallengeOut,
    RequeueOut,
    SourceCreatedOut,
    SourceIn,
    SourceOut,
    SourceRotatedOut,
    SubscriptionFilterEvaluationOut,
)
from app.schemas import MAX_EVENT_TYPE_LENGTH


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(engine)
    yield


app = FastAPI(
    title="Event Ingest Service",
    version="2.10.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready(db: Session = Depends(get_db)) -> dict[str, str]:
    db.execute(text("SELECT 1"))
    return {"status": "ready"}


def fetch_event_types(db: Session, destination_id: UUID) -> list[str]:
    rows = db.execute(
        text(
            """
            SELECT event_type
            FROM destination_subscriptions
            WHERE destination_id = CAST(:destination_id AS UUID)
            ORDER BY event_type
            """
        ),
        {"destination_id": destination_id},
    ).all()
    return [row[0] for row in rows]


def fetch_filters(
    db: Session, destination_id: UUID
) -> dict[str, dict[str, Any]]:
    """Current subscription conditions keyed by event type; types with no
    condition are omitted (they receive every event of the type)."""
    rows = db.execute(
        text(
            """
            SELECT event_type, filter_spec
            FROM destination_subscriptions
            WHERE destination_id = CAST(:destination_id AS UUID)
              AND filter_spec IS NOT NULL
            ORDER BY event_type
            """
        ),
        {"destination_id": destination_id},
    ).all()
    return {row[0]: row[1] for row in rows}


def validate_filters(
    filters: dict[str | Any, Any] | None,
) -> dict[str, dict[str, Any] | None]:
    """Validate every condition in an inbound filters map.

    Returns the map with each non-null condition canonicalized. Raises
    HTTPException(422) on any malformed condition.
    """
    if filters is None:
        return {}
    canonical: dict[str, dict[str, Any] | None] = {}
    for event_type, spec in filters.items():
        normalized_type = event_type.strip()
        if (
            not normalized_type
            or len(normalized_type) > MAX_EVENT_TYPE_LENGTH
        ):
            raise HTTPException(
                status_code=422,
                detail=(
                    "filters keys must be non-empty event types of at most "
                    f"{MAX_EVENT_TYPE_LENGTH} characters"
                ),
            )
        if spec is None:
            canonical[normalized_type] = None
            continue
        try:
            canonical[normalized_type] = validate_filter_spec(spec)
        except FilterSpecError as exc:
            raise HTTPException(
                status_code=422,
                detail=f"invalid filter for event type {normalized_type!r}: {exc}",
            )
    return canonical


def assert_filter_keys_subscribed(
    filters: dict[str, dict[str, Any] | None], subscribed_types: set[str]
) -> None:
    """A condition can only live on an actual subscription. Writing a
    condition for a type the address does not subscribe to is a client error
    (it would otherwise be silently inert)."""
    unknown = sorted(set(filters) - subscribed_types)
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=(
                "filters may only name subscribed event types; unknown for "
                f"this destination: {unknown}"
            ),
        )


def replace_subscriptions(
    db: Session,
    destination_id: Any,
    event_types: list[str],
    filters: dict[str, dict[str, Any] | None],
) -> None:
    """Wholesale replacement of a destination's subscription set (POST with
    event_types). Types not in the list lose both subscription and condition;
    a type listed in filters gets that condition, the rest stay unconditional.
    """
    db.execute(
        text(
            """
            DELETE FROM destination_subscriptions
            WHERE destination_id = CAST(:destination_id AS UUID)
            """
        ),
        {"destination_id": str(destination_id)},
    )
    if not event_types:
        return
    db.execute(
        text(
            """
            INSERT INTO destination_subscriptions
                (destination_id, event_type, filter_spec)
            VALUES (
                CAST(:destination_id AS UUID), :event_type,
                CAST(:filter_spec AS JSONB)
            )
            ON CONFLICT DO NOTHING
            """
        ),
        [
            {
                "destination_id": str(destination_id),
                "event_type": event_type,
                "filter_spec": (
                    json.dumps(filters[event_type])
                    if filters.get(event_type) is not None
                    else None
                ),
            }
            for event_type in event_types
        ],
    )


def patch_subscription_filters(
    db: Session,
    destination_id: Any,
    filters: dict[str, dict[str, Any] | None],
) -> None:
    """Patch conditions of already-subscribed types (PATCH). A null value
    clears that type's condition; other types keep theirs. The caller has
    already verified every key is subscribed."""
    if not filters:
        return
    db.execute(
        text(
            """
            UPDATE destination_subscriptions
            SET filter_spec = CAST(:filter_spec AS JSONB)
            WHERE destination_id = CAST(:destination_id AS UUID)
              AND event_type = :event_type
            """
        ),
        [
            {
                "destination_id": str(destination_id),
                "event_type": event_type,
                "filter_spec": (
                    json.dumps(spec) if spec is not None else None
                ),
            }
            for event_type, spec in filters.items()
        ],
    )


def destination_response(db: Session, destination: RowMapping) -> dict:
    result = dict(destination)
    result["event_types"] = fetch_event_types(db, destination["id"])
    result["filters"] = fetch_filters(db, destination["id"])
    return result


def event_status(
    cancelled_at,
    live_count: int,
    delivered_count: int,
    superseded_count: int,
    pending_count: int = 0,
    dead_lettered_count: int = 0,
    shadow_live_count: int = 0,
    shadow_pending_count: int = 0,
    shadow_delivered_count: int = 0,
    shadow_dead_lettered_count: int = 0,
    shadow_superseded_count: int = 0,
    failed_count: int = 0,
    shadow_failed_count: int = 0,
    release_closed_count: int = 0,
    shadow_release_closed_count: int = 0,
    filtered_count: int = 0,
    shadow_filtered_count: int = 0,
) -> str:
    if cancelled_at is not None:
        return "cancelled"
    # Bodies terminally closed by the preview gate ("no", deadline passed with
    # no nod, manual/cascading void) never went out: they are excluded from
    # live_count by the caller. Subtract them from the total elsewhere; here
    # they simply don't count as pending or delivered.
    if live_count == 0:
        # No for-real live copies: nothing subscribed/confirmed for-real, or
        # every for-real copy was abandoned because its destination moved, or
        # every gated body was closed before it could go out. Shadow-only
        # subscribers must never make an event look fully routed, so they do
        # not count toward acknowledgement; but the transport status still says
        # what actually happened to the shadow copies —
        # delivered/pending/dead-lettered — instead of "unrouted".
        if shadow_live_count == 0:
            if superseded_count > 0 or shadow_superseded_count > 0:
                return "superseded"
            if release_closed_count > 0:
                # Every for-real gated body was withheld and closed; nothing
                # was ever sent to any for-real address. This is explicit and
                # never "delivered".
                return "release_closed"
            if filtered_count > 0:
                # Confirmed for-real subscribers exist, but every one of their
                # own subscription conditions withheld this body: no copy was
                # created and nothing was sent. Explicitly "filtered", never
                # "unrouted" (that means nobody subscribes at all).
                return "filtered"
            return "unrouted"
        if shadow_pending_count > 0:
            return "pending"
        if shadow_dead_lettered_count > 0 and shadow_delivered_count == 0:
            return "dead_lettered"
        if shadow_failed_count > 0 and shadow_delivered_count == 0:
            return "failed"
        if shadow_delivered_count >= shadow_live_count:
            return "delivered"
        return "pending"
    # For-real copies decide the transport state. A shadow copy being parked
    # (dead-letter) or still pending can never hold the whole event back here,
    # and a shadow's 2xx can never make the event look delivered either.
    if pending_count > 0:
        return "pending"
    if dead_lettered_count > 0:
        # Every for-real live copy stopped: at least one is parked in the
        # dead-letter area and nothing for-real is queued or in flight. It is
        # not "delivered" — the parked copies never completed — and it only
        # leaves this state when a copy is manually revived (which puts it
        # back to pending).
        return "dead_lettered"
    if failed_count > 0:
        # Every for-real live copy stopped and at least one is a correction
        # copy whose send attempt failed terminally. Only correction events
        # can reach this state: ordinary copies retry or dead-letter instead.
        return "failed"
    if delivered_count >= live_count:
        return "delivered"
    return "pending"


def event_reconcile_status(
    live_count: int,
    acknowledged_count: int,
    required_ack_count: int | None = None,
) -> str:
    # Only for-real copies decide whether the whole event is acknowledged: a
    # shadow's success receipt can never complete it, and a shadow's timeout
    # or failure receipt can never drag an otherwise-acknowledged event back.
    #
    # A per-type acknowledgement threshold ("认完门槛") snapshots how many
    # for-real success receipts are enough: once that many match, the whole
    # event is acknowledged for good. The decision is monotonic — it only
    # looks at the acknowledged count, which never decreases — so timeouts,
    # failure receipts or dead-letter parking of the remaining copies can
    # never move an acknowledged event back to pending/partially acknowledged.
    # No threshold (legacy rows / unconfigured types) keeps the old rule:
    # every live for-real copy must be acknowledged.
    if required_ack_count is None:
        required_ack_count = live_count
    if required_ack_count > 0 and acknowledged_count >= required_ack_count:
        return "acknowledged"
    if acknowledged_count > 0:
        return "partially_acknowledged"
    return "pending"


def event_response(event: RowMapping | dict[str, Any]) -> dict:
    result = dict(event)
    superseded_count = result.get("superseded_count", 0) or 0
    dead_lettered_count = result.get("dead_lettered_count", 0) or 0
    pending_count = result.get("pending_count", 0) or 0
    failed_count = result.get("failed_count", 0) or 0
    bodies_denied_count = result.get("bodies_denied_count", 0) or 0
    bodies_expired_count = result.get("bodies_expired_count", 0) or 0
    bodies_voided_count = result.get("bodies_voided_count", 0) or 0
    release_closed_count = (
        bodies_denied_count + bodies_expired_count + bodies_voided_count
    )
    shadow_superseded_count = result.get("shadow_superseded_count", 0) or 0
    shadow_dead_lettered_count = result.get("shadow_dead_lettered_count", 0) or 0
    shadow_pending_count = result.get("shadow_pending_count", 0) or 0
    shadow_failed_count = result.get("shadow_failed_count", 0) or 0
    shadow_closed_count = result.get("shadow_bodies_closed_count", 0) or 0
    # Subscribers whose own condition withheld this body got no delivery row,
    # so these counts come from subscription_filter_evaluations (joined into
    # the counts SQL), never from deliveries. They must stay visible and never
    # collapse into "unrouted".
    filtered_out_count = result.get("filtered_out_count", 0) or 0
    shadow_filtered_out_count = result.get("shadow_filtered_out_count", 0) or 0
    result["filtered_out_count"] = filtered_out_count
    result["shadow_filtered_out_count"] = shadow_filtered_out_count
    # delivery_count counts live for-real body copies only — superseded copies
    # never went out, and a gated body closed before release (denied / expired
    # / voided) never went out either; neither must be described as still
    # pending or delivered.
    live_count = (
        result["delivery_count"]
        - superseded_count
        - release_closed_count
    )
    result["delivery_count"] = live_count
    result["superseded_count"] = superseded_count
    result["dead_lettered_count"] = dead_lettered_count
    result["pending_count"] = pending_count
    result["failed_count"] = failed_count
    result["bodies_denied_count"] = bodies_denied_count
    result["bodies_expired_count"] = bodies_expired_count
    result["bodies_voided_count"] = bodies_voided_count
    shadow_live_count = (
        (result.get("shadow_delivery_count", 0) or 0)
        - shadow_superseded_count
        - shadow_closed_count
    )
    result["shadow_delivery_count"] = shadow_live_count
    result["shadow_superseded_count"] = shadow_superseded_count
    result["shadow_dead_lettered_count"] = shadow_dead_lettered_count
    result["shadow_pending_count"] = shadow_pending_count
    result["shadow_failed_count"] = shadow_failed_count
    result["status"] = event_status(
        result.get("cancelled_at"),
        live_count,
        result["delivered_count"],
        superseded_count,
        pending_count,
        dead_lettered_count,
        shadow_live_count,
        shadow_pending_count,
        result.get("shadow_delivered_count", 0) or 0,
        shadow_dead_lettered_count,
        shadow_superseded_count,
        failed_count,
        shadow_failed_count,
        release_closed_count,
        shadow_closed_count,
        filtered_out_count,
        shadow_filtered_out_count,
    )
    required_ack_count = result.get("required_ack_count")
    # No configured threshold (legacy rows) means "every live for-real copy".
    # Cap at live_count as well: a for-real copy that never went out because
    # its destination relocated (terminal 'superseded') or its gated body was
    # closed before release (denied/expired/voided) is not among the live
    # copies and so cannot be part of the required number either. A zero
    # effective requirement (unrouted / shadow-only / pending-confirmation /
    # every body closed) still never reads as acknowledged.
    if required_ack_count is None:
        required_ack_count = live_count
    required_ack_count = min(required_ack_count, live_count)
    result["required_ack_count"] = required_ack_count
    result["reconcile_status"] = event_reconcile_status(
        live_count,
        result["acknowledged_count"],
        required_ack_count,
    )
    result["acknowledged_quorum"] = (
        required_ack_count > 0
        and result["acknowledged_count"] >= required_ack_count
    )
    result["unacknowledged_count"] = live_count - result["acknowledged_count"]
    # The type-wide threshold as configured right now (null = no threshold);
    # required_ack_count above is this event's own ingest-time snapshot.
    result["ack_threshold"] = result.get("configured_ack_threshold")
    result.pop("configured_ack_threshold", None)
    shadow_acknowledged = result.get("shadow_acknowledged_count", 0) or 0
    result["shadow_acknowledged_count"] = shadow_acknowledged
    result["shadow_unacknowledged_count"] = shadow_live_count - shadow_acknowledged
    return result


# Full column list for delivery-shaped responses (trace, reconciliation
# listing and dead-letter listing), including the preview-consent gate fields.
DELIVERY_COLUMNS = (
    "d.id, d.event_id, d.destination_id, dest.url AS destination_url, "
    "d.destination_seq, d.dedupe_key, d.event_type, d.status, d.attempts, "
    "d.next_attempt_at, d.not_before, d.last_error, d.created_at, d.updated_at, "
    "d.delivered_at, d.reconcile_state, d.reconcile_deadline, "
    "d.reconciled_at, d.receipt_result, d.requeue_count, "
    "d.consecutive_failures, d.dead_letter_reason, d.dead_lettered_at, "
    "d.confirmation_generation, d.observe_only, d.filter_spec, "
    "d.phase, d.release_state, d.preview_delivery_id, d.body_delivery_id, "
    "d.consent_deadline, d.consent_timeout_seconds, d.released_at, "
    "d.voided_at, d.void_reason"
)


EVENT_WITH_COUNTS_SQL = """
    SELECT e.id, e.source_id, e.event_type, e.dedupe_key, e.payload, e.created_at,
           e.not_before, e.cancelled_at,
           e.required_ack_count,
           e.preview_payload,
           e.corrects_event_id,
           t.ack_threshold AS configured_ack_threshold,
           -- A gated type fans out as a preview/body pair per subscriber.
           -- Only body copies decide the whole event's transport/reconcile
           -- standing; previews (the notices) are reported separately and
           -- never count as a delivered body or toward acknowledgement.
           p.event_type IS NOT NULL AS preview_gated,
           COUNT(d.id) FILTER (WHERE NOT d.observe_only AND d.phase = 'body')::int AS delivery_count,
           COUNT(d.id) FILTER (
               WHERE NOT d.observe_only AND d.phase = 'body' AND d.status = 'delivered'
           )::int AS delivered_count,
           COUNT(d.id) FILTER (
               WHERE NOT d.observe_only AND d.phase = 'body'
                 AND d.status IN ('pending', 'in_flight')
           )::int AS pending_count,
           COUNT(d.id) FILTER (
               WHERE NOT d.observe_only AND d.phase = 'body'
                 AND d.reconcile_state = 'acknowledged'
           )::int AS acknowledged_count,
           COUNT(d.id) FILTER (
               WHERE NOT d.observe_only AND d.phase = 'body'
                 AND d.status = 'superseded'
           )::int AS superseded_count,
           COUNT(d.id) FILTER (
               WHERE NOT d.observe_only AND d.phase = 'body'
                 AND d.status = 'dead_lettered'
           )::int AS dead_lettered_count,
           COUNT(d.id) FILTER (
               WHERE NOT d.observe_only AND d.phase = 'body'
                 AND d.status = 'failed'
           )::int AS failed_count,
           COUNT(d.id) FILTER (WHERE d.observe_only AND d.phase = 'body')::int AS shadow_delivery_count,
           COUNT(d.id) FILTER (
               WHERE d.observe_only AND d.phase = 'body' AND d.status = 'delivered'
           )::int AS shadow_delivered_count,
           COUNT(d.id) FILTER (
               WHERE d.observe_only AND d.phase = 'body'
                 AND d.status IN ('pending', 'in_flight')
           )::int AS shadow_pending_count,
           COUNT(d.id) FILTER (
               WHERE d.observe_only AND d.phase = 'body'
                 AND d.reconcile_state = 'acknowledged'
           )::int AS shadow_acknowledged_count,
           COUNT(d.id) FILTER (
               WHERE d.observe_only AND d.phase = 'body'
                 AND d.status = 'superseded'
           )::int AS shadow_superseded_count,
           COUNT(d.id) FILTER (
               WHERE d.observe_only AND d.phase = 'body'
                 AND d.status = 'dead_lettered'
           )::int AS shadow_dead_lettered_count,
           COUNT(d.id) FILTER (
               WHERE d.observe_only AND d.phase = 'body'
                 AND d.status = 'failed'
           )::int AS shadow_failed_count,
           -- Preview-consent gate body states (for-real). Waiting bodies are
           -- a subset of pending_count above (held + already released but not
           -- yet sent); closed bodies never went out and never get delivered.
           COUNT(d.id) FILTER (
               WHERE NOT d.observe_only AND d.phase = 'body'
                 AND d.release_state = 'held'
           )::int AS bodies_waiting_count,
           COUNT(d.id) FILTER (
               WHERE NOT d.observe_only AND d.phase = 'body'
                 AND d.release_state = 'released'
           )::int AS bodies_released_count,
           COUNT(d.id) FILTER (
               WHERE NOT d.observe_only AND d.phase = 'body'
                 AND d.status = 'release_denied'
           )::int AS bodies_denied_count,
           COUNT(d.id) FILTER (
               WHERE NOT d.observe_only AND d.phase = 'body'
                 AND d.status = 'release_expired'
           )::int AS bodies_expired_count,
           COUNT(d.id) FILTER (
               WHERE NOT d.observe_only AND d.phase = 'body'
                 AND d.status = 'release_voided'
           )::int AS bodies_voided_count,
           -- Shadow subscriber gate states, kept separate.
           COUNT(d.id) FILTER (
               WHERE d.observe_only AND d.phase = 'body'
                 AND d.release_state = 'held'
           )::int AS shadow_bodies_waiting_count,
           COUNT(d.id) FILTER (
               WHERE d.observe_only AND d.phase = 'body'
                 AND d.release_state = 'released'
           )::int AS shadow_bodies_released_count,
           COUNT(d.id) FILTER (
               WHERE d.observe_only AND d.phase = 'body'
                 AND d.status IN ('release_denied', 'release_expired', 'release_voided')
           )::int AS shadow_bodies_closed_count,
           -- Notices themselves: did each preview reach the wire.
           COUNT(d.id) FILTER (
               WHERE NOT d.observe_only AND d.phase = 'preview'
                 AND d.status = 'delivered'
           )::int AS previews_delivered_count,
           COUNT(d.id) FILTER (
               WHERE d.observe_only AND d.phase = 'preview'
                 AND d.status = 'delivered'
           )::int AS shadow_previews_delivered_count
    FROM events e
    LEFT JOIN deliveries d ON d.event_id = e.id
    LEFT JOIN event_type_ack_thresholds t ON t.event_type = e.event_type
    LEFT JOIN event_type_preview_policies p ON p.event_type = e.event_type
    WHERE {where}
    GROUP BY e.id, t.ack_threshold, p.event_type
"""


# Subscription-condition ("订阅条件") withheld counts are deliberately kept
# OUT of EVENT_WITH_COUNTS_SQL above: that SQL backs nearly every event
# response, and joining a newer table into it would make every event query
# fail together if the migration/table is missing. These counts live in their
# own table (subscription_filter_evaluations) and are attached separately,
# best-effort, by fetch_event_counts.
FILTER_COUNTS_BY_IDS_SQL = """
    SELECT event_id,
           COUNT(*) FILTER (WHERE NOT observe_only)::int AS filtered_out_count,
           COUNT(*) FILTER (WHERE observe_only)::int AS shadow_filtered_out_count
    FROM subscription_filter_evaluations
    WHERE NOT matched
      AND event_id = ANY(CAST(:event_ids AS UUID[]))
    GROUP BY event_id
"""


def attach_filter_counts(db: Session, rows: list[dict]) -> None:
    """Best-effort: add withheld counts to already-materialized count dicts.

    Every row must be a plain mutable dict carrying an ``id``. Any failure (a
    missing table/column on a partially upgraded database, a transient DB
    error) is swallowed and both counts stay zero, so attaching the condition
    summary can never take an event query down."""
    if not rows:
        return
    try:
        ids = [str(row["id"]) for row in rows if row.get("id") is not None]
        if not ids:
            return
        counts_rows = db.execute(
            text(FILTER_COUNTS_BY_IDS_SQL), {"event_ids": ids}
        ).all()
        counts = {str(r[0]): (r[1], r[2]) for r in counts_rows}
    except Exception:
        db.rollback()
        logger.warning(
            "subscription-filter counts unavailable; defaulting to zero",
        )
        counts = {}
    for row in rows:
        real_n, shadow_n = counts.get(str(row["id"]), (0, 0))
        row["filtered_out_count"] = real_n
        row["shadow_filtered_out_count"] = shadow_n


def fetch_event_counts(db: Session, where_sql: str, params: dict) -> dict:
    """Run the core counts SQL for one event and attach the condition
    counts, returning a materialized dict ready for event_response."""
    event = db.execute(
        text(EVENT_WITH_COUNTS_SQL.format(where=where_sql)), params
    ).mappings().first()
    if event is None:
        return None
    event = dict(event)
    attach_filter_counts(db, [event])
    return event


@app.post(
    "/v1/destinations",
    response_model=DestinationOut,
    status_code=status.HTTP_201_CREATED,
)
def register_destination(body: DestinationIn, db: Session = Depends(get_db)):
    url = str(body.url)
    existing = db.execute(
        text("SELECT id FROM destinations WHERE url = :url"),
        {"url": url},
    ).mappings().first()
    is_new = existing is None
    if is_new:
        try:
            destination = db.execute(
                text(
                    f"""
                    INSERT INTO destinations (url, observe_only)
                    VALUES (:url, COALESCE(:observe_only, FALSE))
                    RETURNING {DESTINATION_CONFIRM_COLUMNS}
                    """
                ),
                {"url": url, "observe_only": body.observe_only},
            ).mappings().one()
        except IntegrityError:
            # Concurrent registration of the same URL: fall back to the
            # existing row (registration stays idempotent, handshake not re-armed).
            db.rollback()
            existing = db.execute(
                text("SELECT id FROM destinations WHERE url = :url"),
                {"url": url},
            ).mappings().one()
            destination = db.execute(
                text(
                    f"""
                    SELECT {DESTINATION_CONFIRM_COLUMNS}
                    FROM destinations WHERE id = :destination_id
                    """
                ),
                {"destination_id": existing["id"]},
            ).mappings().one()
            is_new = False
    else:
        # Re-registering the same URL is idempotent and never re-arms the
        # handshake; only a URL change (PATCH) does that.
        destination = db.execute(
            text(
                f"""
                SELECT {DESTINATION_CONFIRM_COLUMNS}
                FROM destinations WHERE id = :destination_id
                """
            ),
            {"destination_id": existing["id"]},
        ).mappings().one()

    canonical_filters = validate_filters(body.filters)
    if canonical_filters and body.event_types is None:
        # A condition must accompany the subscription set on registration:
        # without it the address may not subscribe to those types yet (and a
        # silently dropped condition would later route content unexpectedly).
        # Existing subscriptions are changed via PATCH.
        raise HTTPException(
            status_code=422,
            detail=(
                "filters require event_types on registration: name the "
                "subscribed types the conditions apply to"
            ),
        )
    if body.event_types is not None:
        # Whitespace-normalized subscription set; filter keys must name a
        # type the address actually subscribes to after this edit.
        subscribed_types = set(body.event_types)
        assert_filter_keys_subscribed(canonical_filters, subscribed_types)
        replace_subscriptions(
            db, destination["id"], body.event_types, canonical_filters
        )

    if not is_new and body.observe_only is not None:
        # Re-registration can retoggle the shadow flag. Only newly fanned-out
        # copies pick the new value; existing copies keep their snapshot.
        destination = db.execute(
            text(
                f"""
                UPDATE destinations
                SET observe_only = :observe_only
                WHERE id = :destination_id
                RETURNING {DESTINATION_CONFIRM_COLUMNS}
                """
            ),
            {
                "observe_only": body.observe_only,
                "destination_id": destination["id"],
            },
        ).mappings().one()

    # A brand-new destination starts unconfirmed: arm its first challenge
    # round before any event can fan out to it.
    if is_new:
        destination = arm_round(
            db, str(destination["id"]), bump_generation=False
        )

    result = destination_response(db, destination)
    db.commit()
    return result


@app.patch("/v1/destinations/{destination_id}", response_model=DestinationOut)
def update_destination(
    destination_id: UUID,
    body: DestinationPatchIn,
    db: Session = Depends(get_db),
):
    # Re-locating (new url) re-arms the handshake under a new generation:
    # queued copies aimed at the old location become superseded, copies already
    # out or in flight are left alone. Subscription/observe-only edits never
    # re-arm.
    canonical_filters = validate_filters(body.filters)
    if (
        body.url is None
        and body.event_types is None
        and body.observe_only is None
        and not canonical_filters
    ):
        raise HTTPException(
            status_code=422,
            detail="provide a url, event_types, observe_only and/or filters to update",
        )

    destination = db.execute(
        text(
            f"""
            SELECT {DESTINATION_CONFIRM_COLUMNS}
            FROM destinations
            WHERE id = CAST(:destination_id AS UUID)
            FOR UPDATE
            """
        ),
        {"destination_id": destination_id},
    ).mappings().first()
    if destination is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="destination not found")

    new_url = str(body.url) if body.url is not None else None
    relocated = new_url is not None and new_url != destination["url"]
    if relocated:
        clash = db.execute(
            text("SELECT id FROM destinations WHERE url = :url"),
            {"url": new_url},
        ).first()
        if clash is not None:
            db.rollback()
            raise HTTPException(
                status_code=409,
                detail="another destination is already registered with this url",
            )

    if relocated:
        db.execute(
            text(
                """
                UPDATE destinations SET url = :url
                WHERE id = CAST(:destination_id AS UUID)
                """
            ),
            {"url": new_url, "destination_id": str(destination_id)},
        )

    if body.event_types is not None:
        replace_subscriptions(
            db, str(destination_id), body.event_types, canonical_filters
        )
        # With a wholesale subscription replacement, every filter key must
        # name a type the address subscribes to after the replacement; types
        # dropped from the list lose their condition automatically.
        assert_filter_keys_subscribed(canonical_filters, set(body.event_types))
    elif canonical_filters:
        # Subscription set untouched: every patched condition must name an
        # existing subscription, and a null value clears that type's
        # condition. The change only affects later fan-outs — copies already
        # fanned out keep the condition snapshot they were born with.
        current_types = set(fetch_event_types(db, destination_id))
        assert_filter_keys_subscribed(canonical_filters, current_types)
        patch_subscription_filters(db, destination_id, canonical_filters)

    if body.observe_only is not None and body.observe_only != destination["observe_only"]:
        # Toggling shadow mode only affects copies fanned out afterwards;
        # already-created copies keep the flag they were born with.
        destination = db.execute(
            text(
                f"""
                UPDATE destinations
                SET observe_only = :observe_only
                WHERE id = CAST(:destination_id AS UUID)
                RETURNING {DESTINATION_CONFIRM_COLUMNS}
                """
            ),
            {
                "observe_only": body.observe_only,
                "destination_id": str(destination_id),
            },
        ).mappings().one()

    if relocated:
        destination = arm_round(db, str(destination_id), bump_generation=True)

    result = destination_response(db, destination)
    db.commit()
    return result


@app.get("/v1/destinations/{destination_id}", response_model=DestinationOut)
def get_destination(destination_id: UUID, db: Session = Depends(get_db)):
    destination = db.execute(
        text(
            f"""
            SELECT {DESTINATION_CONFIRM_COLUMNS}
            FROM destinations
            WHERE id = CAST(:destination_id AS UUID)
            """
        ),
        {"destination_id": destination_id},
    ).mappings().first()
    if destination is None:
        raise HTTPException(status_code=404, detail="destination not found")
    return destination_response(db, destination)


@app.post(
    "/v1/destinations/{destination_id}/confirm",
    response_model=ConfirmationOut,
)
def confirm_destination(
    destination_id: UUID,
    request: Request,
    body: ConfirmationIn | None = None,
    db: Session = Depends(get_db),
):
    # The echo can come in the JSON body or in a header (a receiver answering
    # the probe callback by proxying the value). Empty/whitespace echoes are a
    # wrong answer, not a server error.
    header_challenge = request.headers.get("X-Confirmation-Challenge", "").strip()
    challenge = body.challenge.strip() if body and body.challenge else header_challenge
    if not challenge:
        raise HTTPException(status_code=400, detail="missing challenge echo")

    outcome = apply_echo(db, str(destination_id), challenge)
    if outcome["destination"] is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="destination not found")
    disposition = outcome["disposition"]
    destination = outcome["destination"]
    if disposition == "invalid":
        db.commit()
        raise HTTPException(
            status_code=400,
            detail={
                "error": "challenge did not match the outstanding round",
                "disposition": "invalid",
            },
        )
    if disposition == "expired":
        db.commit()
        raise HTTPException(
            status_code=410,
            detail={
                "error": "confirmation round expired before the echo arrived; "
                         "a new round has been issued",
                "disposition": "expired",
            },
        )
    db.commit()
    return {
        "destination_id": destination["id"],
        "confirmation_state": destination["confirmation_state"],
        "confirmed_at": destination["confirmed_at"],
        "confirmation_round": destination["confirmation_round"],
        "disposition": disposition,
        "challenge_expires_at": destination["challenge_expires_at"],
    }


@app.post(
    "/v1/destinations/{destination_id}/reissue-challenge",
    response_model=ReissueChallengeOut,
)
def reissue_challenge(destination_id: UUID, db: Session = Depends(get_db)):
    destination = db.execute(
        text(
            f"""
            SELECT {DESTINATION_CONFIRM_COLUMNS}
            FROM destinations
            WHERE id = CAST(:destination_id AS UUID)
            FOR UPDATE
            """
        ),
        {"destination_id": destination_id},
    ).mappings().first()
    if destination is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="destination not found")
    if destination["confirmation_state"] == "confirmed":
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail="destination is already confirmed",
        )
    destination = arm_round(db, str(destination_id), bump_generation=False)
    db.commit()
    return {
        "destination_id": destination["id"],
        "confirmation_state": destination["confirmation_state"],
        "confirmation_round": destination["confirmation_round"],
        "challenge_expires_at": destination["challenge_expires_at"],
        "reissued": True,
    }


@app.get(
    "/v1/destinations/{destination_id}/confirmation-attempts",
    response_model=list[ConfirmationAttemptOut],
)
def list_confirmation_attempts(
    destination_id: UUID,
    kind: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    destination = db.execute(
        text(
            "SELECT 1 FROM destinations WHERE id = CAST(:destination_id AS UUID)"
        ),
        {"destination_id": str(destination_id)},
    ).first()
    if destination is None:
        raise HTTPException(status_code=404, detail="destination not found")
    if kind is not None and kind not in ("challenge", "echo", "expired"):
        raise HTTPException(status_code=422, detail="invalid kind")
    return db.execute(
        text(
            """
            SELECT id, destination_id, confirmation_round, kind, result,
                   status_code, response_excerpt, error, created_at
            FROM confirmation_attempts
            WHERE destination_id = CAST(:destination_id AS UUID)
              AND (CAST(:kind AS TEXT) IS NULL OR kind = CAST(:kind AS TEXT))
            ORDER BY created_at DESC, id DESC
            LIMIT :limit
            """
        ),
        {"destination_id": str(destination_id), "kind": kind, "limit": limit},
    ).mappings().all()


# --- Inbound event sources: registration, rotation, enable/disable ----------

SOURCE_COLUMNS_SQL = (
    "id, name, secret, disabled_at, key_rotated_at, created_at"
)


def source_response(row: RowMapping, *, include_secret: bool) -> dict:
    result = dict(row)
    result["status"] = "disabled" if result.get("disabled_at") else "active"
    if not include_secret:
        result.pop("secret", None)
    return result


@app.post(
    "/v1/sources",
    response_model=SourceCreatedOut,
    status_code=status.HTTP_201_CREATED,
)
def register_source(body: SourceIn, db: Session = Depends(get_db)):
    # The secret is generated server side and returned exactly once; later
    # endpoints never expose it.
    source = db.execute(
        text(
            """
            INSERT INTO event_sources (name, secret)
            VALUES (:name, :secret)
            ON CONFLICT (name) DO NOTHING
            RETURNING """
            + SOURCE_COLUMNS_SQL
        ),
        {"name": body.name, "secret": generate_secret()},
    ).mappings().first()
    if source is None:
        db.rollback()
        existing = db.execute(
            text("SELECT " + SOURCE_COLUMNS_SQL + " FROM event_sources WHERE name = :name"),
            {"name": body.name},
        ).mappings().first()
        raise HTTPException(
            status_code=409,
            detail={
                "error": "source name already registered",
                "source_id": str(existing["id"]),
                "hint": "rotate the secret or disable the source instead",
            },
        )
    result = source_response(source, include_secret=True)
    db.commit()
    return result


def _fetch_source(db: Session, source_id: UUID, *, for_update: bool = False):
    return db.execute(
        text(
            "SELECT "
            + SOURCE_COLUMNS_SQL
            + " FROM event_sources WHERE id = CAST(:source_id AS UUID)"
            + (" FOR UPDATE" if for_update else "")
        ),
        {"source_id": str(source_id)},
    ).mappings().first()


@app.get("/v1/sources", response_model=list[SourceOut])
def list_sources(
    include_disabled: bool = True,
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text(
            f"""
            SELECT {SOURCE_COLUMNS_SQL}
            FROM event_sources
            WHERE (:include_disabled OR disabled_at IS NULL)
            ORDER BY created_at ASC, id ASC
            LIMIT :limit
            """
        ),
        {"include_disabled": include_disabled, "limit": limit},
    ).mappings().all()
    return [source_response(row, include_secret=False) for row in rows]


@app.get("/v1/sources/{source_id}", response_model=SourceOut)
def get_source(source_id: UUID, db: Session = Depends(get_db)):
    source = _fetch_source(db, source_id)
    if source is None:
        raise HTTPException(status_code=404, detail="source not found")
    return source_response(source, include_secret=False)


@app.post("/v1/sources/{source_id}/rotate-key", response_model=SourceRotatedOut)
def rotate_source_key(source_id: UUID, db: Session = Depends(get_db)):
    # Overwrite the only secret the verifier checks. The old secret is not
    # retained, so events still signed with it fail signature verification
    # and are rejected from this same instant on.
    source = _fetch_source(db, source_id, for_update=True)
    if source is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="source not found")
    previous_rotated_at = source["key_rotated_at"]
    rotated = db.execute(
        text(
            """
            UPDATE event_sources
            SET secret = :secret, key_rotated_at = now()
            WHERE id = CAST(:source_id AS UUID)
            RETURNING """
            + SOURCE_COLUMNS_SQL
        ),
        {"secret": generate_secret(), "source_id": str(source_id)},
    ).mappings().one()
    result = source_response(rotated, include_secret=True)
    db.commit()
    return {"source": result, "previous_key_rotated_at": previous_rotated_at}


@app.post("/v1/sources/{source_id}/disable", response_model=SourceOut)
def disable_source(source_id: UUID, db: Session = Depends(get_db)):
    # Admission stops for new events; deliveries of already-accepted events are
    # rows in the queues and are never touched here, so they keep draining.
    source = db.execute(
        text(
            """
            UPDATE event_sources
            SET disabled_at = COALESCE(disabled_at, now())
            WHERE id = CAST(:source_id AS UUID)
            RETURNING """
            + SOURCE_COLUMNS_SQL
        ),
        {"source_id": str(source_id)},
    ).mappings().first()
    if source is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="source not found")
    db.commit()
    return source_response(source, include_secret=False)


@app.post("/v1/sources/{source_id}/enable", response_model=SourceOut)
def enable_source(source_id: UUID, db: Session = Depends(get_db)):
    source = db.execute(
        text(
            """
            UPDATE event_sources
            SET disabled_at = NULL
            WHERE id = CAST(:source_id AS UUID)
            RETURNING """
            + SOURCE_COLUMNS_SQL
        ),
        {"source_id": str(source_id)},
    ).mappings().first()
    if source is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="source not found")
    db.commit()
    return source_response(source, include_secret=False)


@app.post("/v1/events", response_model=EventOut, status_code=status.HTTP_201_CREATED)
async def create_event(request: Request, response: Response, db: Session = Depends(get_db)):
    # Authenticate against the exact raw bytes before they are parsed, so the
    # signature covers precisely what the sender signed. This and the blocking
    # DB work run in a worker thread to avoid stalling the event loop.
    body = await request.body()

    def handle() -> tuple[dict, int]:
        source = authenticate(
            db,
            source_id_header=request.headers.get("X-Source-Id"),
            signed_at_header=request.headers.get("X-Signed-At"),
            signature_header=request.headers.get("X-Signature"),
            body=body,
            remote_addr=request.client.host if request.client else None,
        )
        parsed = parse_event_body(
            db,
            body,
            source=source,
            signed_at=source["signed_at"],
            remote_addr=request.client.host if request.client else None,
        )
        try:
            event_body = EventIn.model_validate(parsed)
        except Exception as exc:
            # Authenticated sender, malformed event: refuse it and keep the
            # rejection visible; no event row is created.
            log_attempt(
                db,
                disposition=INVALID_BODY,
                source_id=str(source["id"]),
                source_name=source["name"],
                dedupe_key=parsed.get("dedupe_key") if isinstance(parsed, dict) else None,
                event_type=parsed.get("event_type") if isinstance(parsed, dict) else None,
                signed_at=source["signed_at"],
                reason=f"event failed schema validation: {str(exc)[:500]}",
                remote_addr=request.client.host if request.client else None,
                commit=True,
            )
            raise AdmissionError(422, INVALID_BODY, "event failed schema validation")
        return store_event(db, event_body, source)

    try:
        result, code = await run_in_threadpool(handle)
    except AdmissionError as exc:
        # Every rejection was already written to ingestion_attempts by the
        # admission layer; nothing was ever inserted as an event.
        raise HTTPException(
            status_code=exc.status_code,
            detail={"error": exc.reason, "disposition": exc.disposition},
        )
    response.status_code = code
    return result


def store_event(db: Session, body: EventIn, source: dict) -> tuple[dict, int]:
    payload = json.dumps(body.payload)
    # Resolve the per-type acknowledgement threshold ("认完门槛") before the
    # event row is written. None means "every fanned-out for-real copy must
    # acknowledge". The effective required count is computed after fan-out
    # below and capped at the for-real subscribers actually present, so a
    # threshold larger than the current subscriber list can never make the
    # event un-finishable.
    ack_threshold = db.execute(
        text(
            """
            SELECT ack_threshold
            FROM event_type_ack_thresholds
            WHERE event_type = :event_type
            """
        ),
        {"event_type": body.event_type},
    ).mappings().first()
    ack_threshold = ack_threshold["ack_threshold"] if ack_threshold else None
    # Preview-consent policy ("预告 + 点头才给正文"). A gated type fans out as a
    # preview/body pair per confirmed subscriber; the body waits for that
    # address's own nod. preview_payload on a non-gated type is a client
    # mistake (it would otherwise be silently dropped), so refuse it loudly.
    policy = release_gate.get_policy(db, body.event_type)
    if policy is None and body.preview_payload is not None:
        log_attempt(
            db,
            disposition=INVALID_BODY,
            source_id=str(source["id"]),
            source_name=source["name"],
            dedupe_key=body.dedupe_key,
            event_type=body.event_type,
            signed_at=source["signed_at"],
            reason=(
                "preview_payload is only valid for an event type with a "
                "preview-consent policy (gated type)"
            ),
            remote_addr=None,
            commit=True,
        )
        raise AdmissionError(
            422,
            INVALID_BODY,
            "preview_payload is only valid for a gated event type",
        )
    preview_payload = (
        json.dumps(body.preview_payload)
        if policy is not None and body.preview_payload is not None
        else None
    )
    event = db.execute(
        text(
            """
            INSERT INTO events (source_id, event_type, dedupe_key, payload,
                                not_before, required_ack_count, preview_payload)
            VALUES (CAST(:source_id AS UUID), :event_type, :dedupe_key,
                    CAST(:payload AS JSONB), CAST(:not_before AS TIMESTAMPTZ),
                    CAST(:required_ack_count AS INTEGER),
                    CAST(:preview_payload AS JSONB))
            ON CONFLICT (dedupe_key) DO NOTHING
            RETURNING id, source_id, event_type, dedupe_key, payload, not_before,
                      cancelled_at, created_at, required_ack_count,
                      corrects_event_id, preview_payload
            """
        ),
        {
            "source_id": str(source["id"]),
            "event_type": body.event_type,
            "dedupe_key": body.dedupe_key,
            "payload": payload,
            "not_before": body.not_before,
            # Filled in after fan-out below; the row stays inside this
            # transaction so nobody can observe the placeholder.
            "required_ack_count": 0,
            "preview_payload": preview_payload,
        },
    ).mappings().first()

    if event is None:
        # Same dedupe_key pushed again — even from the same source. It must not
        # become a new event and must not fan out a second time. Record the
        # refusal and return the original event marked as a duplicate.
        existing = db.execute(
            text(EVENT_WITH_COUNTS_SQL.format(where="e.dedupe_key = :dedupe_key")),
            {"dedupe_key": body.dedupe_key},
        ).mappings().first()
        if existing is None:  # pragma: no cover - cannot happen after a conflict
            db.rollback()
            raise HTTPException(status_code=500, detail="event insert conflict lost")
        existing = dict(existing)
        attach_filter_counts(db, [existing])
        signed_at = source["signed_at"]
        log_attempt(
            db,
            disposition=DUPLICATE,
            source_id=str(source["id"]),
            source_name=source["name"],
            event_id=str(existing["id"]),
            dedupe_key=body.dedupe_key,
            event_type=body.event_type,
            signed_at=signed_at,
            reason="dedupe_key already ingested; no new event or deliveries created",
        )
        db.commit()
        result = event_response(existing)
        result["duplicate"] = True
        return result, 200

    # Fan out to the destinations subscribed to this type *and confirmed*
    # right now. A still-unconfirmed subscriber gets no delivery row at all:
    # the event is stored, but nothing can be reported as sent to it, and once
    # it later completes its handshake it only receives events ingested after
    # that point — old events are never backfilled. Each confirmed subscriber
    # gets its own queued copy with the next per-destination sequence number,
    # so one destination's backlog, retries or isolation never affect the
    # others. Destinations are locked in id order to keep concurrent fan-outs
    # deadlock-free. The confirmation gate is rechecked inside the INSERT: if
    # a concurrent URL change re-arms that destination between the two
    # statements the row is not created. The schedule gate (not_before) is
    # copied onto every delivery; each copy keeps its queue position while it
    # waits for its time.
    subscribers = db.execute(
        text(
            """
            SELECT s.destination_id, d.observe_only, s.filter_spec
            FROM destination_subscriptions s
            JOIN destinations d ON d.id = s.destination_id
            WHERE s.event_type = :event_type
              AND d.confirmation_state = 'confirmed'
            ORDER BY s.destination_id
            FOR UPDATE OF d
            """
        ),
        {"event_type": body.event_type},
    ).all()

    total_subscribers = db.execute(
        text(
            """
            SELECT COUNT(*)::int AS count
            FROM destination_subscriptions
            WHERE event_type = :event_type
            """
        ),
        {"event_type": body.event_type},
    ).mappings().one()["count"]

    # Per-address subscription conditions ("订阅条件"). Each confirmed
    # subscriber carrying a condition gets exactly one evaluation against
    # THIS exact body: matching subscribers stay in the fan-out list (the
    # condition is snapshotted onto their copies); non-matching subscribers
    # are taken out of it and recorded in subscription_filter_evaluations
    # with matched=false, so no delivery exists for them (nothing is sent and
    # it is never written as sent) while the trace still says "this address's
    # own condition did not match" — distinct from 'unrouted' (nobody
    # subscribes). The judgement is a pure function of (condition snapshot,
    # body), so re-evaluating the same pair always yields the same answer.
    # Conditions are evaluated here, once, so a later edit only affects later
    # events; already-fanned copies are never recalled.
    routed_subscribers = []
    filtered_real = 0
    filtered_shadow = 0
    for row in subscribers:
        destination_id, is_shadow, filter_spec = row
        if filter_spec is None:
            routed_subscribers.append(row)
            continue
        matches = evaluate_filter(filter_spec, body.payload)
        db.execute(
            text(
                """
                INSERT INTO subscription_filter_evaluations
                    (event_id, destination_id, event_type, matched,
                     filter_spec, observe_only)
                VALUES (
                    CAST(:event_id AS UUID), CAST(:destination_id AS UUID),
                    :event_type, :matched, CAST(:filter_spec AS JSONB),
                    :observe_only
                )
                ON CONFLICT (event_id, destination_id) DO NOTHING
                """
            ),
            {
                "event_id": event["id"],
                "destination_id": destination_id,
                "event_type": body.event_type,
                "matched": matches,
                "filter_spec": json.dumps(filter_spec),
                "observe_only": is_shadow,
            },
        )
        if matches:
            routed_subscribers.append(row)
        elif is_shadow:
            filtered_shadow += 1
        else:
            filtered_real += 1

    if policy is not None:
        # Gated type: every confirmed subscriber whose condition held gets a
        # preview first and a held body behind it. A non-matching subscriber
        # gets neither (the preview would itself reveal the event): its
        # withheld judgement is the evaluation row inserted above.
        pair_counts = release_gate.fan_out_gated_event(
            db,
            event_id=str(event["id"]),
            event_type=body.event_type,
            dedupe_key=body.dedupe_key,
            payload=payload,
            preview_payload=preview_payload,
            not_before=body.not_before,
            consent_timeout_seconds=policy["consent_timeout_seconds"],
            destinations=routed_subscribers,
            filter_specs={
                str(row[0]): row[2]
                for row in routed_subscribers
                if row[2] is not None
            },
        )
        real_pair_count = pair_counts["real_pairs"]
        shadow_pair_count = pair_counts["shadow_pairs"]
    else:
        real_pair_count = len(
            [row for row in routed_subscribers if not row[1]]
        )
        shadow_pair_count = len(
            [row for row in routed_subscribers if row[1]]
        )
        for row in routed_subscribers:
            db.execute(
                text(
                    """
                    WITH bumped AS (
                        UPDATE destinations
                        SET next_event_seq = next_event_seq + 1
                        WHERE id = :destination_id
                          AND confirmation_state = 'confirmed'
                        RETURNING id, next_event_seq, confirmation_generation
                    )
                    INSERT INTO deliveries
                        (event_id, destination_id, event_type, dedupe_key, payload,
                         destination_seq, not_before, confirmation_generation,
                         observe_only, filter_spec)
                    SELECT :event_id, id, :event_type, :dedupe_key,
                           CAST(:payload AS JSONB), next_event_seq,
                           CAST(:not_before AS TIMESTAMPTZ), confirmation_generation,
                           :observe_only, CAST(:filter_spec AS JSONB)
                    FROM bumped
                    """
                ),
                {
                    "event_id": event["id"],
                    "destination_id": row[0],
                    "event_type": body.event_type,
                    "dedupe_key": body.dedupe_key,
                    "payload": payload,
                    "not_before": body.not_before,
                    "observe_only": row[1],
                    "filter_spec": json.dumps(row[2]) if row[2] is not None else None,
                },
            )

    # Snapshot this event's acknowledgement requirement onto the event row in
    # the same transaction: a configured threshold capped at the current
    # for-real confirmed subscribers, otherwise every for-real copy. Shadow
    # subscribers never count toward it. For gated events the requirement is
    # the number of for-real *bodies* (one per for-real pair). A zero snapshot
    # (unrouted / shadow-only / pending-confirmation) keeps reconcile_status
    # pending — acknowledgement still requires at least one for-real success
    # receipt on a body.
    required_ack_count = (
        min(ack_threshold, real_pair_count)
        if ack_threshold is not None
        else real_pair_count
    )
    db.execute(
        text(
            """
            UPDATE events
            SET required_ack_count = :required_ack_count
            WHERE id = CAST(:event_id AS UUID)
            """
        ),
        {
            "event_id": event["id"],
            "required_ack_count": required_ack_count,
        },
    )

    signed_at = source["signed_at"]
    # No confirmed subscribers: still accepted and persisted. The disposition
    # says which case this is without ever describing the event as sent:
    # - nobody subscribes to this type at all        -> unrouted
    # - subscribers exist but none has confirmed yet -> pending_confirmation
    # - every confirmed subscriber's own condition
    #   withheld this exact body                      -> filtered
    # Each withheld judgement carries its condition snapshot in
    # subscription_filter_evaluations and stays distinct from "nobody
    # subscribed"; an unconditional subscriber (or a matching one) is simply
    # accepted/fanned out.
    if not routed_subscribers:
        if not subscribers:
            disposition = UNROUTED if total_subscribers == 0 else PENDING_CONFIRMATION
        else:
            disposition = FILTERED
    else:
        disposition = ACCEPTED
    log_attempt(
        db,
        disposition=disposition,
        source_id=str(source["id"]),
        source_name=source["name"],
        event_id=str(event["id"]),
        dedupe_key=body.dedupe_key,
        event_type=body.event_type,
        signed_at=signed_at,
        reason=(
            "every confirmed subscriber's subscription condition withheld "
            "this body"
            if disposition == FILTERED
            else None
        ),
    )
    db.commit()
    result = dict(event)
    result["required_ack_count"] = required_ack_count
    result["configured_ack_threshold"] = ack_threshold
    result["preview_gated"] = policy is not None
    # The counts SQL below groups bodies and previews separately; on the
    # immediate ingest response every for-real body is a live pending copy.
    result["delivery_count"] = real_pair_count
    result["delivered_count"] = 0
    result["pending_count"] = real_pair_count
    result["acknowledged_count"] = 0
    result["superseded_count"] = 0
    result["dead_lettered_count"] = 0
    result["failed_count"] = 0
    result["bodies_waiting_count"] = real_pair_count if policy is not None else 0
    result["bodies_released_count"] = 0
    result["bodies_denied_count"] = 0
    result["bodies_expired_count"] = 0
    result["bodies_voided_count"] = 0
    result["previews_delivered_count"] = 0
    result["shadow_delivery_count"] = shadow_pair_count
    result["shadow_delivered_count"] = 0
    result["shadow_pending_count"] = shadow_pair_count
    result["shadow_acknowledged_count"] = 0
    result["shadow_superseded_count"] = 0
    result["shadow_dead_lettered_count"] = 0
    result["shadow_failed_count"] = 0
    result["shadow_bodies_waiting_count"] = (
        shadow_pair_count if policy is not None else 0
    )
    result["shadow_bodies_released_count"] = 0
    result["shadow_bodies_closed_count"] = 0
    result["shadow_previews_delivered_count"] = 0
    # Withheld counts come from the evaluation rows just inserted, not from
    # deliveries.
    result["filtered_out_count"] = filtered_real
    result["shadow_filtered_out_count"] = filtered_shadow
    return event_response(result), 201


@app.get("/v1/events/{event_id}", response_model=EventOut)
def get_event(event_id: UUID, db: Session = Depends(get_db)):
    # Direct single-event lookup; same row the trace wraps. Distinct 404
    # from a trace-shaped response so callers do not have to parse trace.
    event = fetch_event_counts(
        db, "e.id = CAST(:event_id AS UUID)", {"event_id": str(event_id)}
    )
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return event_response(event)


@app.get("/v1/events/{event_id}/trace", response_model=EventTraceOut)
def get_event_trace(event_id: UUID, db: Session = Depends(get_db)):
    event = fetch_event_counts(
        db, "e.id = CAST(:event_id AS UUID)", {"event_id": str(event_id)}
    )
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")

    try:
        deliveries = db.execute(
            text(
                f"""
                SELECT {DELIVERY_COLUMNS}
                FROM deliveries d
                JOIN destinations dest ON dest.id = d.destination_id
                WHERE d.event_id = CAST(:event_id AS UUID)
                ORDER BY dest.url ASC, d.destination_seq ASC
                """
            ),
            {"event_id": event_id},
        ).mappings().all()
    except Exception:
        # Half-upgraded database (deliveries.filter_spec column missing):
        # never let the trace fail. Retry without the filter-feature column;
        # filter_spec on each copy then stays null.
        db.rollback()
        logger.warning(
            "deliveries query with filter columns failed for event_id=%s; "
            "retrying without them",
            event_id,
        )
        fallback_columns = DELIVERY_COLUMNS.replace("d.filter_spec, ", "")
        deliveries = db.execute(
            text(
                f"""
                SELECT {fallback_columns}
                FROM deliveries d
                JOIN destinations dest ON dest.id = d.destination_id
                WHERE d.event_id = CAST(:event_id AS UUID)
                ORDER BY dest.url ASC, d.destination_seq ASC
                """
            ),
            {"event_id": event_id},
        ).mappings().all()

    attempts = db.execute(
        text(
            """
            SELECT id, delivery_id, event_id, destination_id, attempt_no,
                   started_at, finished_at, success, status_code,
                   response_excerpt, error, lost_lease
            FROM delivery_attempts
            WHERE event_id = CAST(:event_id AS UUID)
            ORDER BY started_at ASC, id ASC
            """
        ),
        {"event_id": event_id},
    ).mappings().all()
    receipts = db.execute(
        text(
            """
            SELECT r.id, r.destination_id, r.dedupe_key, r.result,
                   r.delivery_id, r.disposition, r.received_at
            FROM receipts r
            JOIN deliveries d ON d.id = r.delivery_id
            WHERE d.event_id = CAST(:event_id AS UUID)
            ORDER BY r.received_at ASC, r.id ASC
            """
        ),
        {"event_id": event_id},
    ).mappings().all()
    return {
        "event": event_response(event),
        "deliveries": deliveries,
        "attempts": attempts,
        "receipts": receipts,
        "corrections": list_corrections_of(db, event_id),
        "filter_evaluations": list_filter_evaluations_of(db, event_id),
        "routing": build_event_routing(db, event_id),
    }


# Row-per-address routing for one event. A single query brings together, per
# address: the CURRENT subscription to this event's type (and its condition),
# the fan-out verdict taken against this exact body
# (subscription_filter_evaluations, including a null "never judged" row for
# confirmed unconditional subscribers), and the body copy that was created.
# Everything is LEFT-joined and COALESCEd so a partially-upgraded database or
# a missing judgement row degrades to an explicit outcome ("unconfirmed" /
# "unsubscribed") instead of failing the whole trace.
EVENT_ROUTING_SQL = """
    WITH ev AS (
        SELECT id, event_type FROM events WHERE id = CAST(:event_id AS UUID)
    )
    SELECT dest.id AS destination_id,
           dest.url AS destination_url,
           dest.observe_only AS observe_only,
           dest.confirmation_state = 'confirmed' AS is_confirmed,
           (sub.destination_id IS NOT NULL) AS subscribed,
           sub.filter_spec AS current_filter_spec,
           fe.matched AS matched,
           fe.filter_spec AS evaluated_filter_spec,
           body.id AS body_delivery_id,
           body.status AS body_status
    FROM ev
    -- The address universe relevant to this event: anything that currently
    -- subscribes to the type, OR got a copy, OR was judged at fan-out.
    JOIN destinations dest ON dest.id IN (
        SELECT s.destination_id
        FROM destination_subscriptions s
        JOIN ev ON ev.event_type = s.event_type
        UNION
        SELECT d.destination_id FROM deliveries d
        JOIN ev ON ev.id = d.event_id
        UNION
        SELECT fe.destination_id
        FROM subscription_filter_evaluations fe
        JOIN ev ON ev.id = fe.event_id
    )
    LEFT JOIN destination_subscriptions sub
           ON sub.destination_id = dest.id
          AND sub.event_type = (SELECT event_type FROM ev)
    LEFT JOIN subscription_filter_evaluations fe
           ON fe.destination_id = dest.id
          AND fe.event_id = (SELECT id FROM ev)
    LEFT JOIN LATERAL (
        SELECT bd.id, bd.status
        FROM deliveries bd
        WHERE bd.event_id = CAST(:event_id AS UUID)
          AND bd.destination_id = dest.id
          AND bd.phase = 'body'
        ORDER BY bd.destination_seq DESC
        LIMIT 1
    ) body ON TRUE
    ORDER BY dest.url ASC NULLS LAST, dest.id
"""


def _routing_outcome(row: RowMapping) -> str:
    """Derive the single routing verdict for an address from the joined row.

    Order matters: a body copy or an explicit fan-out verdict is
    authoritative; only when neither exists do we say why no copy was made
    (still unconfirmed, or no longer subscribed)."""
    if row["body_delivery_id"] is not None:
        # A body copy exists: the address passed its condition, or it had
        # none. The body's own lifecycle (release_*/dead-lettered/...) is
        # visible in body_status.
        return "matched" if row["evaluated_filter_spec"] is not None else "no_condition"
    if row["matched"] is not None:
        # Judged at fan-out but no body copy: false means withheld by its
        # own condition; true without a body can only be a gated body closed
        # before release — it passed the condition, so this is still matched.
        return "matched" if row["matched"] else "filtered"
    # No copy and no fan-out judgement row.
    if not row["subscribed"]:
        return "unsubscribed"
    if not row["is_confirmed"]:
        return "unconfirmed"
    # Confirmed and still subscribed. A condition-carrying address would have
    # an evaluation row, so a missing one with a current condition means the
    # judgement has aged out / predates this feature: report it honestly as
    # an unconfirmed-at-fan-out style "not routed" rather than as a pass.
    return "unconfirmed" if row["current_filter_spec"] is not None else "no_condition"


def build_event_routing(db: Session, event_id: UUID) -> list[dict]:
    """Collect one routing row per relevant address.

    This section is best-effort: the condition verdicts are the point of the
    trace, so a failure to assemble the summary must never take the whole
    event query down. Anything unexpected falls back to an empty list while
    the detailed deliveries/filter_evaluations sections are still returned.
    """
    try:
        rows = db.execute(
            text(EVENT_ROUTING_SQL), {"event_id": str(event_id)}
        ).mappings().all()
    except Exception:
        # A pre-filter-feature database that has not yet gained the table (or
        # any other structural mismatch) must still answer the event query.
        db.rollback()
        logger.warning(
            "event routing summary unavailable for event_id=%s; returning "
            "detailed sections only",
            event_id,
        )
        return []
    result = []
    for row in rows:
        result.append(
            {
                "destination_id": row["destination_id"],
                "destination_url": row["destination_url"],
                "observe_only": bool(row["observe_only"]),
                "subscribed": bool(row["subscribed"]),
                "filter_spec": row["current_filter_spec"],
                "outcome": _routing_outcome(row),
                "matched": row["matched"],
                "evaluated_filter_spec": row["evaluated_filter_spec"],
                "body_delivery_id": row["body_delivery_id"],
                "body_status": row["body_status"],
            }
        )
    return result


def list_filter_evaluations_of(db: Session, event_id: UUID) -> list:
    # Per-address subscription-condition judgements for this exact body.
    # matched=false rows are exactly the "this address's own condition did not
    # match, so it got no copy" outcomes — distinct from an unrouted event
    # (nobody subscribed) and from a subscriber still waiting on confirmation
    # (which is never evaluated and never appears here). Best-effort: a
    # pre-filter-feature database without the table must still return the
    # trace, just with an empty section.
    try:
        return db.execute(
            text(
                """
                SELECT id, event_id, destination_id, event_type, matched,
                       filter_spec, observe_only, created_at
                FROM subscription_filter_evaluations
                WHERE event_id = CAST(:event_id AS UUID)
                ORDER BY created_at ASC, id ASC
                """
            ),
            {"event_id": str(event_id)},
        ).mappings().all()
    except Exception:
        db.rollback()
        logger.warning(
            "subscription-filter evaluations unavailable for event_id=%s; "
            "returning an empty section",
            event_id,
        )
        return []


@app.get(
    "/v1/filter-evaluations",
    response_model=list[SubscriptionFilterEvaluationOut],
)
def list_filter_evaluations(
    event_id: UUID | None = None,
    destination_id: UUID | None = None,
    matched: bool | None = None,
    event_type: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    # Audit trail of subscription-condition judgements. Pulled WITHOUT a
    # matched filter it must return the WHOLE list (both passed and withheld
    # rows for the event): note the explicit CAST(:matched AS BOOLEAN) —
    # sending an untyped NULL parameter into "matched = :matched" makes
    # Postgres fail with "could not determine data type of parameter", which
    # previously masqueraded as an empty list. Filter matched=false /
    # destination_id=... to answer "why did this address not get this one";
    # each row carries the exact condition snapshot judged, so the outcome
    # never collapses into "nobody subscribed".
    try:
        return db.execute(
            text(
                """
                SELECT id, event_id, destination_id, event_type, matched,
                       filter_spec, observe_only, created_at
                FROM subscription_filter_evaluations
                WHERE (CAST(:event_id AS UUID) IS NULL
                       OR event_id = CAST(:event_id AS UUID))
                  AND (CAST(:destination_id AS UUID) IS NULL
                       OR destination_id = CAST(:destination_id AS UUID))
                  AND (CAST(:matched AS BOOLEAN) IS NULL
                       OR matched = CAST(:matched AS BOOLEAN))
                  AND (CAST(:event_type AS TEXT) IS NULL
                       OR event_type = CAST(:event_type AS TEXT))
                ORDER BY created_at DESC, id DESC
                LIMIT :limit
                """
            ),
            {
                "event_id": str(event_id) if event_id else None,
                "destination_id": str(destination_id) if destination_id else None,
                "matched": matched,
                "event_type": event_type,
                "limit": limit,
            },
        ).mappings().all()
    except SQLAlchemyError:
        # Only a genuine missing table on a pre-filter-feature database
        # degrades to an empty audit trail; any other query error must not be
        # silently turned into an empty whole-list answer (that previously hid
        # the untyped-NULL parameter bug).
        db.rollback()
        logger.warning(
            "subscription-filter evaluations table unavailable; "
            "returning an empty audit trail",
        )
        return []


# --- Corrections ("补一笔更正") ------------------------------------------------
#
# A correction is an additional entry submitted against an already-accepted
# event. The original event's copies that already went out are never recalled
# or rewritten; the correction is a new event row (linked by
# corrects_event_id) whose copies are fanned out only to the destinations the
# original was *really delivered* to (copies carrying delivered_at). Each
# correction copy joins the tail of its destination's queue with a fresh
# destination_seq, so it waits behind everything already queued and can never
# cut ahead of a copy currently in flight. Its reconciliation starts only
# when it is really sent (the usual delivered-at deadline), never from the
# correction's submit time. A correction copy that fails its send attempt
# ends in the terminal 'failed' state: accounted on its own, never charged to
# the destination's consecutive-failure tally (no isolation), and no longer
# blocking later copies of that destination.

CORRECTIONS_OF_EVENT_SQL = EVENT_WITH_COUNTS_SQL.format(
    where="e.corrects_event_id = CAST(:event_id AS UUID)"
) + " ORDER BY e.created_at ASC, e.id ASC"


def list_corrections_of(db: Session, event_id: UUID) -> list[dict]:
    rows = db.execute(
        text(CORRECTIONS_OF_EVENT_SQL), {"event_id": str(event_id)}
    ).mappings().all()
    rows = [dict(row) for row in rows]
    attach_filter_counts(db, rows)
    return [event_response(row) for row in rows]


@app.post(
    "/v1/events/{event_id}/corrections",
    response_model=EventOut,
    status_code=status.HTTP_201_CREATED,
)
def create_correction(
    event_id: UUID,
    body: CorrectionIn,
    response: Response,
    db: Session = Depends(get_db),
):
    event = db.execute(
        text(
            """
            SELECT id, source_id, event_type
            FROM events
            WHERE id = CAST(:event_id AS UUID)
            FOR UPDATE
            """
        ),
        {"event_id": str(event_id)},
    ).mappings().first()
    if event is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="event not found")

    # The correction goes exactly to the addresses the original really
    # reached: a copy that completed transport at least once carries
    # delivered_at. Copies still queued, cancelled, superseded or never
    # created (unrouted / pending-confirmation) are not corrected. The
    # destination rows are locked in id order (same discipline as ingest
    # fan-out) so concurrent corrections stay deadlock-free.
    delivered = db.execute(
        text(
            """
            SELECT DISTINCT reached.destination_id, reached.observe_only,
                   s.filter_spec
            FROM (
                -- One row per destination that really received a body of
                -- THIS original event (gated events also have a preview row:
                -- only the body counts).
                SELECT DISTINCT d.destination_id, d.observe_only
                FROM deliveries d
                WHERE d.event_id = CAST(:event_id AS UUID)
                  AND d.delivered_at IS NOT NULL
                  AND d.phase = 'body'
            ) reached
            -- The original event's type is single-valued (events.event_type),
            -- so this join is 1:1; joining without it would fan out across
            -- every type the destination subscribes to and could pick up
            -- another type's condition.
            JOIN events original ON original.id = CAST(:event_id AS UUID)
            JOIN destinations dest ON dest.id = reached.destination_id
            LEFT JOIN destination_subscriptions s
                   ON s.destination_id = reached.destination_id
                  AND s.event_type = original.event_type
            ORDER BY reached.destination_id
            """
        ),
        {"event_id": str(event_id)},
    ).mappings().all()

    if not delivered:
        # Nothing ever went out for this event: there is no delivered copy to
        # correct. Refuse loudly and create nothing — the event keeps reading
        # as "never sent", and the same correction dedupe_key stays usable
        # once something really goes out.
        total = db.execute(
            text(
                """
                SELECT COUNT(*)::int AS count
                FROM deliveries
                WHERE event_id = CAST(:event_id AS UUID)
                """
            ),
            {"event_id": str(event_id)},
        ).mappings().one()["count"]
        db.rollback()
        if total == 0:
            raise HTTPException(
                status_code=409,
                detail=(
                    "event was never routed to any destination (unrouted or "
                    "subscribers not yet confirmed); there is no delivered "
                    "copy to correct"
                ),
            )
        raise HTTPException(
            status_code=409,
            detail=(
                "no copy of this event has been sent out yet; there is no "
                "delivered copy to correct"
            ),
        )

    # A correction carries its own corrected body, so each eligible address's
    # CURRENT subscription condition is evaluated afresh against that body —
    # the snapshot on the original copy is not reused. Conditions later
    # replaced/added therefore apply to the next (correction) send, while
    # copies already queued for the address keep their own snapshots. The
    # judgement rows are inserted below, once the correction event exists.
    def _withheld(row) -> bool:
        return (
            row["filter_spec"] is not None
            and not evaluate_filter(row["filter_spec"], body.payload)
        )

    routed_delivered = [row for row in delivered if not _withheld(row)]
    filtered_real = sum(
        1 for row in delivered if _withheld(row) and not row["observe_only"]
    )
    filtered_shadow = sum(
        1 for row in delivered if _withheld(row) and row["observe_only"]
    )

    if not routed_delivered:
        # Every address the original really reached currently withholds the
        # correction body under its own condition: nothing is created and no
        # dedupe key is consumed. Distinct from "nothing was ever delivered"
        # above.
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail=(
                "every destination the original event was delivered to has a "
                "subscription condition that withholds this correction body"
            ),
        )

    payload = json.dumps(body.payload)
    correction = db.execute(
        text(
            """
            INSERT INTO events (source_id, event_type, dedupe_key, payload,
                                required_ack_count, corrects_event_id)
            VALUES (CAST(:source_id AS UUID), :event_type, :dedupe_key,
                    CAST(:payload AS JSONB), 0,
                    CAST(:corrects_event_id AS UUID))
            ON CONFLICT (dedupe_key) DO NOTHING
            RETURNING id, source_id, event_type, dedupe_key, payload, not_before,
                      cancelled_at, created_at, required_ack_count,
                      corrects_event_id
            """
        ),
        {
            "source_id": str(event["source_id"]) if event["source_id"] else None,
            "event_type": event["event_type"],
            "dedupe_key": body.dedupe_key,
            "payload": payload,
            "corrects_event_id": str(event_id),
        },
    ).mappings().first()

    if correction is None:
        # The dedupe key already exists. The same correction sent again is
        # applied exactly once: return the original correction marked as a
        # duplicate. A key belonging to a different event or correction is a
        # conflict, not a duplicate.
        existing = db.execute(
            text(
                """
                SELECT id, corrects_event_id
                FROM events
                WHERE dedupe_key = :dedupe_key
                """
            ),
            {"dedupe_key": body.dedupe_key},
        ).mappings().first()
        if existing is None:  # pragma: no cover - cannot happen after a conflict
            db.rollback()
            raise HTTPException(status_code=500, detail="correction insert conflict lost")
        if str(existing["corrects_event_id"]) != str(event_id):
            db.rollback()
            raise HTTPException(
                status_code=409,
                detail="dedupe_key is already used by another event or correction",
            )
        duplicate = db.execute(
            text(EVENT_WITH_COUNTS_SQL.format(where="e.id = CAST(:correction_id AS UUID)")),
            {"correction_id": str(existing["id"])},
        ).mappings().first()
        duplicate = dict(duplicate)
        attach_filter_counts(db, [duplicate])
        db.commit()
        result = event_response(duplicate)
        result["duplicate"] = True
        response.status_code = 200
        return result

    # Fan the correction out to exactly the delivered-and-still-matching set:
    # one additional copy per destination at the tail of that destination's
    # queue (a fresh destination_seq). The observe_only snapshot comes from the
    # original copy it corrects — a shadow address's correction copy stays a
    # shadow and can never count as for-real. The CURRENT condition is
    # snapshotted onto the correction copy, and each condition-carrying address
    # gets one evaluation row recording the judgement. Copies aimed at a
    # currently unconfirmed destination simply wait at the usual claim gate.
    real_copies = [row for row in routed_delivered if not row["observe_only"]]
    shadow_copies = [row for row in routed_delivered if row["observe_only"]]
    for row in routed_delivered:
        db.execute(
            text(
                """
                WITH bumped AS (
                    UPDATE destinations
                    SET next_event_seq = next_event_seq + 1
                    WHERE id = :destination_id
                    RETURNING id, next_event_seq, confirmation_generation
                )
                INSERT INTO deliveries
                    (event_id, destination_id, event_type, dedupe_key, payload,
                     destination_seq, confirmation_generation, observe_only,
                     filter_spec)
                SELECT :event_id, id, :event_type, :dedupe_key,
                       CAST(:payload AS JSONB), next_event_seq,
                       confirmation_generation, :observe_only,
                       CAST(:filter_spec AS JSONB)
                FROM bumped
                """
            ),
            {
                "event_id": correction["id"],
                "destination_id": row["destination_id"],
                "event_type": event["event_type"],
                "dedupe_key": body.dedupe_key,
                "payload": payload,
                "observe_only": row["observe_only"],
                "filter_spec": (
                    json.dumps(row["filter_spec"])
                    if row["filter_spec"] is not None
                    else None
                ),
            },
        )

    # Record one evaluation per condition-carrying eligible address,
    # including the withheld ones (they have no correction copy but the
    # trace must still say "this address's condition did not match").
    for row in delivered:
        if row["filter_spec"] is None:
            continue
        matches = not _withheld(row)
        db.execute(
            text(
                """
                INSERT INTO subscription_filter_evaluations
                    (event_id, destination_id, event_type, matched,
                     filter_spec, observe_only)
                VALUES (
                    CAST(:event_id AS UUID), CAST(:destination_id AS UUID),
                    :event_type, :matched, CAST(:filter_spec AS JSONB),
                    :observe_only
                )
                ON CONFLICT (event_id, destination_id) DO NOTHING
                """
            ),
            {
                "event_id": correction["id"],
                "destination_id": row["destination_id"],
                "event_type": event["event_type"],
                "matched": matches,
                "filter_spec": json.dumps(row["filter_spec"]),
                "observe_only": row["observe_only"],
            },
        )

    # Snapshot the correction's own acknowledgement requirement with the same
    # rule as ingest: the type's configured threshold capped at the for-real
    # copies of this correction, otherwise every for-real copy. Shadow
    # correction copies never count toward it.
    ack_threshold = db.execute(
        text(
            """
            SELECT ack_threshold
            FROM event_type_ack_thresholds
            WHERE event_type = :event_type
            """
        ),
        {"event_type": event["event_type"]},
    ).mappings().first()
    ack_threshold = ack_threshold["ack_threshold"] if ack_threshold else None
    required_ack_count = (
        min(ack_threshold, len(real_copies))
        if ack_threshold is not None
        else len(real_copies)
    )
    db.execute(
        text(
            """
            UPDATE events
            SET required_ack_count = :required_ack_count
            WHERE id = CAST(:event_id AS UUID)
            """
        ),
        {"event_id": correction["id"], "required_ack_count": required_ack_count},
    )
    db.commit()
    result = dict(correction)
    result["required_ack_count"] = required_ack_count
    result["configured_ack_threshold"] = ack_threshold
    result["delivery_count"] = len(real_copies)
    result["delivered_count"] = 0
    result["pending_count"] = len(real_copies)
    result["acknowledged_count"] = 0
    result["superseded_count"] = 0
    result["dead_lettered_count"] = 0
    result["failed_count"] = 0
    result["shadow_delivery_count"] = len(shadow_copies)
    result["shadow_delivered_count"] = 0
    result["shadow_pending_count"] = len(shadow_copies)
    result["shadow_acknowledged_count"] = 0
    result["shadow_superseded_count"] = 0
    result["shadow_dead_lettered_count"] = 0
    result["shadow_failed_count"] = 0
    result["filtered_out_count"] = filtered_real
    result["shadow_filtered_out_count"] = filtered_shadow
    return event_response(result)


@app.get("/v1/events/{event_id}/corrections", response_model=list[EventOut])
def list_corrections(event_id: UUID, db: Session = Depends(get_db)):
    event = db.execute(
        text("SELECT 1 FROM events WHERE id = CAST(:event_id AS UUID)"),
        {"event_id": str(event_id)},
    ).first()
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    # Events that never went out anywhere simply have no corrections here —
    # and can never acquire one (POST above refuses them), so nothing is ever
    # reported as corrected or sent for them.
    return list_corrections_of(db, event_id)


# --- Cancellation and rescheduling ------------------------------------------
#
# Both operations share one rule: once any copy of the event has been handed
# to a receiver (delivered) or is being handed over right now (in_flight),
# the event is frozen — what already went out cannot be taken back or moved.
# While every copy is still queued, cancellation drops them into the terminal
# 'cancelled' state (the worker never touches those again) and rescheduling
# rewrites the not_before gate on the queued copies in place, so each copy
# keeps its original per-destination queue position.

LOCK_EVENT_SQL = """
    SELECT id, cancelled_at
    FROM events
    WHERE id = CAST(:event_id AS UUID)
    FOR UPDATE
"""

# Locks every copy that is not yet terminally cancelled, in a stable order.
# Delivered copies are locked too so a concurrent requeue cannot flip one
# back to pending in the middle of the decision.
LOCK_EVENT_DELIVERIES_SQL = """
    SELECT id, status
    FROM deliveries
    WHERE event_id = CAST(:event_id AS UUID)
      AND status IN ('pending', 'in_flight', 'delivered')
    ORDER BY destination_id, destination_seq
    FOR UPDATE
"""


def lock_event(db: Session, event_id: UUID):
    return db.execute(
        text(LOCK_EVENT_SQL), {"event_id": str(event_id)}
    ).mappings().first()


def lock_event_deliveries(db: Session, event_id: UUID):
    return db.execute(
        text(LOCK_EVENT_DELIVERIES_SQL), {"event_id": str(event_id)}
    ).mappings().all()


def fetch_event_response(db: Session, event_id: UUID) -> dict:
    return event_response(fetch_event_counts(
        db, "e.id = CAST(:event_id AS UUID)", {"event_id": str(event_id)}
    ))


def event_already_sent(deliveries) -> bool:
    """True once any copy reached a receiver (or is in flight right now)."""
    return any(d["status"] in ("in_flight", "delivered") for d in deliveries)


@app.post("/v1/events/{event_id}/cancel", response_model=EventOut)
def cancel_event(event_id: UUID, db: Session = Depends(get_db)):
    event = lock_event(db, event_id)
    if event is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="event not found")
    if event["cancelled_at"] is not None:
        # Cancelling is idempotent: an already-cancelled event stays cancelled.
        db.rollback()
        return fetch_event_response(db, event_id)

    if event_already_sent(lock_event_deliveries(db, event_id)):
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail="event has already been sent out and can no longer be cancelled",
        )

    # Every copy is still queued and now locked by this transaction, so the
    # worker cannot claim any of them (its SKIP LOCKED pick skips locked
    # rows). Cancel them all: cancelled is terminal, they will never go out.
    # A held gated body keeps the whole-event 'cancelled' status too (its
    # release gate is moot once the event itself is cancelled).
    db.execute(
        text(
            """
            UPDATE deliveries
            SET status = 'cancelled',
                release_state = CASE
                    WHEN release_state = 'held' THEN 'release_voided'
                    ELSE release_state
                END,
                voided_at = CASE
                    WHEN release_state = 'held' THEN now() ELSE voided_at
                END,
                void_reason = CASE
                    WHEN release_state = 'held' THEN 'manual' ELSE void_reason
                END,
                updated_at = now()
            WHERE event_id = CAST(:event_id AS UUID)
              AND status = 'pending'
            """
        ),
        {"event_id": str(event_id)},
    )
    db.execute(
        text("UPDATE events SET cancelled_at = now() WHERE id = CAST(:event_id AS UUID)"),
        {"event_id": str(event_id)},
    )
    db.commit()
    return fetch_event_response(db, event_id)


@app.post("/v1/events/{event_id}/reschedule", response_model=EventOut)
def reschedule_event(
    event_id: UUID, body: EventRescheduleIn, db: Session = Depends(get_db)
):
    event = lock_event(db, event_id)
    if event is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="event not found")
    if event["cancelled_at"] is not None:
        db.rollback()
        raise HTTPException(
            status_code=409, detail="a cancelled event cannot be rescheduled"
        )

    if event_already_sent(lock_event_deliveries(db, event_id)):
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail="event has already been sent out and can no longer be rescheduled",
        )

    # Rewrite the gate on the event and on every still-queued copy. The copies
    # keep their destination_seq, so they rejoin their queues exactly where
    # they already were — only the earliest send time moves.
    db.execute(
        text(
            """
            UPDATE events
            SET not_before = CAST(:not_before AS TIMESTAMPTZ)
            WHERE id = CAST(:event_id AS UUID)
            """
        ),
        {"event_id": str(event_id), "not_before": body.not_before},
    )
    db.execute(
        text(
            """
            UPDATE deliveries
            SET not_before = CAST(:not_before AS TIMESTAMPTZ),
                updated_at = now()
            WHERE event_id = CAST(:event_id AS UUID)
              AND status = 'pending'
            """
        ),
        {"event_id": str(event_id), "not_before": body.not_before},
    )
    db.commit()
    return fetch_event_response(db, event_id)


@app.post(
    "/v1/events/{event_id}/requeue-unreconciled",
    response_model=EventBulkRequeueOut,
)
def requeue_event_unreconciled(event_id: UUID, db: Session = Depends(get_db)):
    event = db.execute(
        text(
            """
            SELECT id
            FROM events
            WHERE id = CAST(:event_id AS UUID)
            FOR UPDATE
            """
        ),
        {"event_id": str(event_id)},
    ).mappings().first()
    if event is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="event not found")

    # Lock the event's for-real delivery rows in a stable order before
    # counting, so the quorum decision and the requeue are atomic against a
    # receipt arriving in another transaction: a receipt application takes
    # FOR UPDATE on its single delivery row and therefore serializes here
    # (it never waits on another delivery, so the order cannot deadlock).
    # Only non-superseded rows matter — a superseded copy never went out and
    # is excluded from both the counts and the requirement. Shadow copies are
    # deliberately not locked: they never participate in this decision.
    db.execute(
        text(
            """
            SELECT id
            FROM deliveries
            WHERE event_id = CAST(:event_id AS UUID)
              AND NOT observe_only
              AND status <> 'superseded'
            ORDER BY destination_id, destination_seq
            FOR UPDATE
            """
        ),
        {"event_id": str(event_id)},
    ).all()

    # Once the event reached its acknowledgement requirement — every for-real
    # copy by default, or the per-type threshold — the list is never pulled
    # again: the unacknowledged copies keep their own lifecycle (they can
    # still time out / fail / be dead-lettered / be requeued individually),
    # but "re-throw what is still unacknowledged for the whole event" becomes
    # a no-op. Observe-only copies are never in the counts here.
    quorum = db.execute(
        text(
            """
            SELECT
                COUNT(d.id) FILTER (WHERE NOT d.observe_only)::int
                    AS live_count,
                COUNT(d.id) FILTER (
                    WHERE NOT d.observe_only
                      AND d.reconcile_state = 'acknowledged'
                )::int AS acknowledged_count,
                e.required_ack_count AS required_ack_count
            FROM events e
            LEFT JOIN deliveries d
                   ON d.event_id = e.id
                  AND d.status <> 'superseded'
            WHERE e.id = CAST(:event_id AS UUID)
            GROUP BY e.id, e.required_ack_count
            """
        ),
        {"event_id": str(event_id)},
    ).mappings().one()
    # Null snapshot (legacy events) means "every live for-real copy"; cap at
    # the current live count so copies that never went out after a location
    # change ('superseded') are not held against the requirement.
    required = quorum["required_ack_count"]
    if required is None:
        required = quorum["live_count"]
    required = min(required, quorum["live_count"])
    quorum_reached = (
        required > 0 and quorum["acknowledged_count"] >= required
    )
    if quorum_reached:
        db.commit()
        return {
            "event_id": event_id,
            "requeued_count": 0,
            "deliveries": [],
            "already_acknowledged": True,
        }

    # Requirement not yet met: only terminal receipt failures/timeouts on
    # FOR-REAL copies are re-sent. Copies that are acknowledged stay
    # untouched; copies still waiting for a receipt or still in transport are
    # also left alone, so this endpoint never duplicates work; observe-only
    # ("shadow") copies are never on this whole-event list.
    requeued = db.execute(
        text(
            f"""
            WITH target_deliveries AS (
                SELECT id
                FROM deliveries
                WHERE event_id = CAST(:event_id AS UUID)
                  AND {EVENT_REQUEUEABLE_WHERE}
                ORDER BY destination_id, destination_seq
                FOR UPDATE
            ), requeued_deliveries AS (
                UPDATE deliveries d
                SET {REQUEUE_SET_SQL}
                FROM target_deliveries t
                WHERE d.id = t.id
                RETURNING d.id, d.destination_id, d.destination_seq
            )
            SELECT id, destination_id, destination_seq
            FROM requeued_deliveries
            ORDER BY destination_id, destination_seq
            """
        ),
        {"event_id": str(event_id)},
    ).mappings().all()
    db.commit()
    return {
        "event_id": event_id,
        "requeued_count": len(requeued),
        "deliveries": [
            {
                "delivery_id": row["id"],
                "destination_id": row["destination_id"],
                "destination_seq": row["destination_seq"],
                "requeued": True,
            }
            for row in requeued
        ],
        "already_acknowledged": False,
    }


@app.post("/v1/destinations/{destination_id}/recover", response_model=RecoveryOut)
def recover_destination(destination_id: UUID, db: Session = Depends(get_db)):
    destination = db.execute(
        text(
            f"""
            SELECT {DESTINATION_CONFIRM_COLUMNS}
            FROM destinations
            WHERE id = CAST(:destination_id AS UUID)
            FOR UPDATE
            """
        ),
        {"destination_id": destination_id},
    ).mappings().first()

    if destination is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="destination not found")

    recovered = destination["status"] == "isolated"
    reset_count = 0
    if recovered:
        reset = db.execute(
            text(
                """
                WITH reset_deliveries AS (
                    UPDATE deliveries
                    SET next_attempt_at = now(),
                        updated_at = now()
                    WHERE destination_id = CAST(:destination_id AS UUID)
                      AND status = 'pending'
                    RETURNING id
                )
                SELECT count(*)::int AS count FROM reset_deliveries
                """
            ),
            {"destination_id": destination_id},
        ).mappings().one()
        reset_count = reset["count"]
        destination = db.execute(
            text(
                f"""
                UPDATE destinations
                SET status = 'active',
                    failure_count = 0,
                    recoverable_at = NULL
                WHERE id = CAST(:destination_id AS UUID)
                RETURNING {DESTINATION_CONFIRM_COLUMNS}
                """
            ),
            {"destination_id": destination_id},
        ).mappings().one()

    result = destination_response(db, destination)
    db.commit()
    return {
        "destination": result,
        "recovered": recovered,
        "pending_deliveries_reset": reset_count if recovered else None,
    }


# --- Operator-marked "not receiving" windows ---------------------------------
#
# An operator can mark a stretch of time during which a destination receives
# nothing (maintenance window, receiver-side freeze). The window is a
# claim-time gate only: queued copies keep their per-destination queue
# positions and wait, no attempt is made while the window is in effect (so
# the pause can never be charged as consecutive failures or trigger
# isolation), and the reconcile countdown of a waiting copy only starts when
# it is really sent after the window — never from submit time. Other
# destinations subscribed to the same event types keep draining normally.
# Copies already in flight when the window opens finish their HTTP call
# normally: what was really sent is not taken back.


@app.post("/v1/destinations/{destination_id}/pause", response_model=DestinationOut)
def pause_destination(
    destination_id: UUID, body: DestinationPauseIn, db: Session = Depends(get_db)
):
    # An empty body is a mistake, not a window: at least one bound must be
    # named. Explicit nulls are fine — {"paused_from": null} marks "from now
    # until explicitly resumed". Clearing the window is POST .../resume.
    if not ({"paused_from", "paused_until"} & body.model_fields_set):
        raise HTTPException(
            status_code=422,
            detail=(
                "provide paused_from and/or paused_until to mark a "
                "not-receiving window; use /resume to clear one"
            ),
        )
    destination = db.execute(
        text(
            f"""
            SELECT {DESTINATION_CONFIRM_COLUMNS}
            FROM destinations
            WHERE id = CAST(:destination_id AS UUID)
            FOR UPDATE
            """
        ),
        {"destination_id": destination_id},
    ).mappings().first()
    if destination is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="destination not found")

    # A missing start means "from right now" (database clock, so every
    # service agrees on when the window opened).
    now = db.execute(text("SELECT now() AS now")).mappings().one()["now"]
    paused_from = body.paused_from if body.paused_from is not None else now
    if body.paused_until is not None and body.paused_until <= paused_from:
        db.rollback()
        raise HTTPException(
            status_code=422,
            detail="paused_until must be later than paused_from",
        )
    if body.paused_until is not None and body.paused_until <= now:
        # A window that is already over can never take effect; it almost
        # always means a client clock/timezone mistake, so refuse it loudly.
        db.rollback()
        raise HTTPException(
            status_code=422,
            detail="paused_until is already in the past",
        )

    # Setting a new window replaces the old one wholesale; queued copies are
    # never touched — they simply become claimable again when it ends.
    destination = db.execute(
        text(
            f"""
            UPDATE destinations
            SET paused_from = CAST(:paused_from AS TIMESTAMPTZ),
                paused_until = CAST(:paused_until AS TIMESTAMPTZ)
            WHERE id = CAST(:destination_id AS UUID)
            RETURNING {DESTINATION_CONFIRM_COLUMNS}
            """
        ),
        {
            "destination_id": destination_id,
            "paused_from": paused_from,
            "paused_until": body.paused_until,
        },
    ).mappings().one()
    result = destination_response(db, destination)
    db.commit()
    return result


@app.post(
    "/v1/destinations/{destination_id}/resume",
    response_model=DestinationResumeOut,
)
def resume_destination(destination_id: UUID, db: Session = Depends(get_db)):
    destination = db.execute(
        text(
            f"""
            SELECT {DESTINATION_CONFIRM_COLUMNS}
            FROM destinations
            WHERE id = CAST(:destination_id AS UUID)
            FOR UPDATE
            """
        ),
        {"destination_id": destination_id},
    ).mappings().first()
    if destination is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="destination not found")
    # Idempotent: clearing a window that is not there changes nothing and
    # just reports resumed=false, like /recover on an active address.
    had_window = destination["paused_from"] is not None
    if had_window:
        destination = db.execute(
            text(
                f"""
                UPDATE destinations
                SET paused_from = NULL, paused_until = NULL
                WHERE id = CAST(:destination_id AS UUID)
                RETURNING {DESTINATION_CONFIRM_COLUMNS}
                """
            ),
            {"destination_id": destination_id},
        ).mappings().one()
    result = destination_response(db, destination)
    db.commit()
    return {"destination": result, "resumed": had_window}


# --- Inbound admission log --------------------------------------------------

INGESTION_DISPOSITIONS = (
    ACCEPTED,
    UNROUTED,
    PENDING_CONFIRMATION,
    FILTERED,
    DUPLICATE,
    "source_unknown",
    SOURCE_DISABLED,
    "bad_signature",
    "stale_timestamp",
    "future_timestamp",
    "invalid_timestamp",
    INVALID_BODY,
)


@app.get("/v1/ingestion/attempts", response_model=list[IngestionAttemptOut])
def list_ingestion_attempts(
    source_id: UUID | None = None,
    disposition: str | None = None,
    dedupe_key: str | None = None,
    rejected_only: bool = False,
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    # Every push is visible here — including the ones turned away at entry —
    # so a rejected event can always be audited instead of silently dropped.
    if disposition is not None and disposition not in INGESTION_DISPOSITIONS:
        raise HTTPException(status_code=422, detail="invalid disposition")
    rejected_list = "','".join(
        (
            "source_unknown",
            SOURCE_DISABLED,
            "bad_signature",
            "stale_timestamp",
            "future_timestamp",
            "invalid_timestamp",
            INVALID_BODY,
            DUPLICATE,
        )
    )
    return db.execute(
        text(
            f"""
            SELECT id, source_id, source_name, event_id, dedupe_key, event_type,
                   signed_at, disposition, reason, remote_addr, received_at
            FROM ingestion_attempts
            WHERE (CAST(:source_id AS UUID) IS NULL
                   OR source_id = CAST(:source_id AS UUID))
              AND (CAST(:disposition AS TEXT) IS NULL
                   OR disposition = CAST(:disposition AS TEXT))
              AND (CAST(:dedupe_key AS TEXT) IS NULL
                   OR dedupe_key = CAST(:dedupe_key AS TEXT))
              AND (NOT :rejected_only
                   OR disposition IN ('{rejected_list}'))
            ORDER BY received_at DESC, id DESC
            LIMIT :limit
            """
        ),
        {
            "source_id": str(source_id) if source_id else None,
            "disposition": disposition,
            "dedupe_key": dedupe_key,
            "rejected_only": rejected_only,
            "limit": limit,
        },
    ).mappings().all()


# --- Per-event-type acknowledgement thresholds ("认完门槛") -----------------
#
# A type threshold says: once this many FOR-REAL copies of an event of this
# type carry matching success receipts, the whole event counts as acknowledged
# and that standing never goes backwards. Observe-only ("shadow") copies never
# count toward it. The requirement is snapshotted onto each event at ingest
# time (and capped at the for-real confirmed subscribers then present), so
# changing or deleting the threshold here only ever affects later events —
# events already accepted keep their own snapshot. Types with no row keep the
# default rule: every fanned-out for-real copy must be acknowledged.


def _normalize_event_type_path(event_type: str) -> str:
    normalized = event_type.strip()
    if not normalized or len(normalized) > MAX_EVENT_TYPE_LENGTH:
        raise HTTPException(
            status_code=422,
            detail=(
                "event_type must be a non-empty string of at most "
                f"{MAX_EVENT_TYPE_LENGTH} characters"
            ),
        )
    return normalized


@app.get(
    "/v1/event-types/ack-thresholds",
    response_model=list[AckThresholdOut],
)
def list_ack_thresholds(
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text(
            """
            SELECT event_type, ack_threshold, created_at, updated_at
            FROM event_type_ack_thresholds
            ORDER BY event_type ASC
            LIMIT :limit
            """
        ),
        {"limit": limit},
    ).mappings().all()
    return rows


@app.get(
    "/v1/event-types/{event_type}/ack-threshold",
    response_model=AckThresholdOut,
)
def get_ack_threshold(event_type: str, db: Session = Depends(get_db)):
    event_type = _normalize_event_type_path(event_type)
    row = db.execute(
        text(
            """
            SELECT event_type, ack_threshold, created_at, updated_at
            FROM event_type_ack_thresholds
            WHERE event_type = :event_type
            """
        ),
        {"event_type": event_type},
    ).mappings().first()
    if row is None:
        # No configured threshold: the type uses the default all-for-real rule.
        return {
            "event_type": event_type,
            "ack_threshold": None,
            "created_at": None,
            "updated_at": None,
            "configured": False,
        }
    return dict(row) | {"configured": True}


@app.put(
    "/v1/event-types/{event_type}/ack-threshold",
    response_model=AckThresholdOut,
)
def set_ack_threshold(
    event_type: str,
    body: AckThresholdIn,
    db: Session = Depends(get_db),
):
    event_type = _normalize_event_type_path(event_type)
    if body.ack_threshold is None:
        # {"ack_threshold": null} on PUT clears the configured threshold and
        # restores the default all-for-real rule for later events.
        db.execute(
            text(
                """
                DELETE FROM event_type_ack_thresholds
                WHERE event_type = :event_type
                """
            ),
            {"event_type": event_type},
        )
        db.commit()
        return {
            "event_type": event_type,
            "ack_threshold": None,
            "created_at": None,
            "updated_at": None,
            "configured": False,
        }
    row = db.execute(
        text(
            """
            INSERT INTO event_type_ack_thresholds (event_type, ack_threshold)
            VALUES (:event_type, :ack_threshold)
            ON CONFLICT (event_type) DO UPDATE
                SET ack_threshold = EXCLUDED.ack_threshold,
                    updated_at = now()
            RETURNING event_type, ack_threshold, created_at, updated_at
            """
        ),
        {"event_type": event_type, "ack_threshold": body.ack_threshold},
    ).mappings().one()
    db.commit()
    return dict(row) | {"configured": True}


@app.delete(
    "/v1/event-types/{event_type}/ack-threshold",
    response_model=AckThresholdOut,
)
def clear_ack_threshold(event_type: str, db: Session = Depends(get_db)):
    event_type = _normalize_event_type_path(event_type)
    db.execute(
        text(
            """
            DELETE FROM event_type_ack_thresholds
            WHERE event_type = :event_type
            """
        ),
        {"event_type": event_type},
    )
    db.commit()
    return {
        "event_type": event_type,
        "ack_threshold": None,
        "created_at": None,
        "updated_at": None,
        "configured": False,
    }


# --- Receipt ingestion and reconciliation ---------------------------------

PREVIEW_CONSENT_DISPOSITIONS = (
    "released",
    "denied",
    "duplicate",
    "late_ignored",
    "conflict",
    "preview_not_delivered",
    "not_gated",
    "orphan",
)


@app.get("/v1/event-types/preview-policies", response_model=list[PreviewPolicyOut])
def list_preview_policies(
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    return db.execute(
        text(
            f"""
            SELECT {release_gate.POLICY_COLUMNS}
            FROM event_type_preview_policies
            ORDER BY event_type ASC
            LIMIT :limit
            """
        ),
        {"limit": limit},
    ).mappings().all()


@app.get(
    "/v1/event-types/{event_type}/preview-policy",
    response_model=PreviewPolicyOut,
)
def get_preview_policy(event_type: str, db: Session = Depends(get_db)):
    event_type = _normalize_event_type_path(event_type)
    row = release_gate.get_policy(db, event_type)
    if row is None:
        return {
            "event_type": event_type,
            "consent_timeout_seconds": None,
            "created_at": None,
            "updated_at": None,
            "gated": False,
        }
    return dict(row) | {"gated": True}


@app.put(
    "/v1/event-types/{event_type}/preview-policy",
    response_model=PreviewPolicyOut,
)
def set_preview_policy(
    event_type: str,
    body: PreviewPolicyIn,
    db: Session = Depends(get_db),
):
    # Mark the type gated: events ingested afterwards fan out as
    # preview/body pairs. The timeout is snapshotted per event pair, so the
    # change never rewrites events already accepted.
    event_type = _normalize_event_type_path(event_type)
    timeout_seconds = (
        body.consent_timeout_seconds
        if body.consent_timeout_seconds is not None
        else int(settings.preview_consent_timeout_seconds_default)
    )
    row = release_gate.upsert_policy(db, event_type, timeout_seconds)
    db.commit()
    return dict(row) | {"gated": True}


@app.delete(
    "/v1/event-types/{event_type}/preview-policy",
    response_model=PreviewPolicyOut,
)
def clear_preview_policy(event_type: str, db: Session = Depends(get_db)):
    event_type = _normalize_event_type_path(event_type)
    release_gate.delete_policy(db, event_type)
    db.commit()
    return {
        "event_type": event_type,
        "consent_timeout_seconds": None,
        "created_at": None,
        "updated_at": None,
        "gated": False,
    }


@app.post("/v1/events/{event_id}/consent", response_model=ConsentOut)
def post_consent(
    event_id: UUID,
    body: ConsentIn,
    destination_id: UUID = Query(...),
    db: Session = Depends(get_db),
):
    # The gate is always (event, destination): the destination_id query
    # parameter is mandatory so another address's answer can never release
    # this one. Idempotent — the same address answering the same preview twice
    # only ever produces one effective answer.
    try:
        outcome = release_gate.ingest_decision(
            db,
            destination_id=str(destination_id),
            event_id=str(event_id),
            decision=body.decision,
        )
    except release_gate.ConsentError as exc:
        db.rollback()
        raise HTTPException(status_code=exc.status_code, detail=exc.reason)
    return {
        "event_id": event_id,
        "destination_id": destination_id,
        "delivery_id": outcome.get("delivery_id"),
        "disposition": outcome["disposition"],
        "release_state": outcome["release_state"],
    }


# Gate row columns plus the preview's state for the gate query.
_GATE_COLUMNS = (
    DELIVERY_COLUMNS
    + ", p.status AS preview_status, p.delivered_at AS preview_delivered_at, "
    + "p.id AS preview_delivery"
)


def _gate_rows(db: Session, where_sql: str, params: dict) -> list:
    return db.execute(
        text(
            f"""
            SELECT {_GATE_COLUMNS}
            FROM deliveries d
            JOIN destinations dest ON dest.id = d.destination_id
            JOIN deliveries p ON p.id = d.preview_delivery_id
            WHERE d.phase = 'body'
              AND d.release_state IS NOT NULL
              AND {where_sql}
            ORDER BY d.created_at ASC, d.id ASC
            LIMIT :limit
            """
        ),
        {"limit": params.pop("limit", 500), **params},
    ).mappings().all()


@app.get("/v1/release-gates", response_model=list[ReleaseGateOut])
def list_release_gates(
    event_id: UUID | None = None,
    destination_id: UUID | None = None,
    release_state: str | None = None,
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    # "This address did not nod" is answerable: filter release_state=held with
    # an expired deadline (release_expired after the sweep), or
    # status=release_expired/release_denied/release_voided for closed bodies.
    # A closed body is never reported as delivered.
    if release_state is not None and release_state not in (
        release_gate.HELD,
        release_gate.RELEASED,
        release_gate.DENIED,
        release_gate.EXPIRED,
        release_gate.VOIDED,
    ):
        raise HTTPException(status_code=422, detail="invalid release_state")
    if status_filter is not None and status_filter not in (
        "pending",
        "in_flight",
        "delivered",
        "release_denied",
        "release_expired",
        "release_voided",
        "dead_lettered",
    ):
        raise HTTPException(status_code=422, detail="invalid status")
    clauses = [
        "(CAST(:event_id AS UUID) IS NULL OR d.event_id = CAST(:event_id AS UUID))",
        "(CAST(:destination_id AS UUID) IS NULL OR d.destination_id = CAST(:destination_id AS UUID))",
        "(CAST(:release_state AS TEXT) IS NULL OR d.release_state = CAST(:release_state AS TEXT))",
        "(CAST(:status_filter AS TEXT) IS NULL OR d.status = CAST(:status_filter AS TEXT))",
    ]
    return _gate_rows(
        db,
        " AND ".join(clauses),
        {
            "event_id": str(event_id) if event_id else None,
            "destination_id": str(destination_id) if destination_id else None,
            "release_state": release_state,
            "status_filter": status_filter,
            "limit": limit,
        },
    )


@app.get(
    "/v1/events/{event_id}/release-gates",
    response_model=list[ReleaseGateOut],
)
def list_event_release_gates(
    event_id: UUID,
    destination_id: UUID | None = None,
    release_state: str | None = None,
    db: Session = Depends(get_db),
):
    if release_state is not None and release_state not in (
        release_gate.HELD,
        release_gate.RELEASED,
        release_gate.DENIED,
        release_gate.EXPIRED,
        release_gate.VOIDED,
    ):
        raise HTTPException(status_code=422, detail="invalid release_state")
    return _gate_rows(
        db,
        "d.event_id = CAST(:event_id AS UUID) "
        "AND (CAST(:destination_id AS UUID) IS NULL "
        "     OR d.destination_id = CAST(:destination_id AS UUID)) "
        "AND (CAST(:release_state AS TEXT) IS NULL "
        "     OR d.release_state = CAST(:release_state AS TEXT))",
        {
            "event_id": str(event_id),
            "destination_id": str(destination_id) if destination_id else None,
            "release_state": release_state,
        },
    )


@app.get(
    "/v1/release-gate-decisions",
    response_model=list[ReleaseGateDecisionOut],
)
def list_release_gate_decisions(
    event_id: UUID | None = None,
    destination_id: UUID | None = None,
    disposition: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    # Audit trail of every nod/"no": effective answers (released/denied),
    # duplicates, conflicts, late answers, pre-delivery answers and orphans.
    if disposition is not None and disposition not in PREVIEW_CONSENT_DISPOSITIONS:
        raise HTTPException(status_code=422, detail="invalid disposition")
    return db.execute(
        text(
            """
            SELECT id, destination_id, event_id, delivery_id, decision,
                   disposition, reason, created_at
            FROM release_gate_decisions
            WHERE (CAST(:event_id AS UUID) IS NULL
                   OR event_id = CAST(:event_id AS UUID))
              AND (CAST(:destination_id AS UUID) IS NULL
                   OR destination_id = CAST(:destination_id AS UUID))
              AND (CAST(:disposition AS TEXT) IS NULL
                   OR disposition = CAST(:disposition AS TEXT))
            ORDER BY created_at DESC, id DESC
            LIMIT :limit
            """
        ),
        {
            "event_id": str(event_id) if event_id else None,
            "destination_id": str(destination_id) if destination_id else None,
            "disposition": disposition,
            "limit": limit,
        },
    ).mappings().all()


@app.post("/v1/deliveries/{delivery_id}/void-body", response_model=BodyVoidOut)
def void_gated_body_endpoint(delivery_id: UUID, db: Session = Depends(get_db)):
    # Manually void a gated body that has not gone out. The preview (already
    # delivered or still queued) is deliberately untouched; a body already in
    # flight or delivered can no longer be taken back (409).
    try:
        return release_gate.void_gated_body(db, str(delivery_id))
    except release_gate.ConsentError as exc:
        db.rollback()
        raise HTTPException(status_code=exc.status_code, detail=exc.reason)


# --- Receipt ingestion and reconciliation ---------------------------------

RECONCILE_STATES = ("awaiting", "acknowledged", "receipt_failed", "timed_out")


# Only copies whose transport finished but were never acknowledged inside the
# agreed window may be sent back out. They keep their original destination_seq,
# so they rejoin the per-destination queue at their original position. Two
# extra gates follow from the activation handshake:
#  * the destination must currently be confirmed — requeuing while it is still
#    unconfirmed would deliver before the handshake, which is forbidden;
#  * the copy's generation must match the current one — after a URL change an
#    old timed-out copy belongs to the old location and must not be resent to
#    the new one (it was never going to be backfilled).
REQUEUEABLE_WHERE = """
    status = 'delivered'
    AND reconcile_state IN ('timed_out', 'receipt_failed')
    AND EXISTS (
        SELECT 1
        FROM destinations dest
        WHERE dest.id = deliveries.destination_id
          AND dest.confirmation_state = 'confirmed'
          AND dest.confirmation_generation = deliveries.confirmation_generation
    )
"""

# Event-level "re-send the unacknowledged copies" only ever means for-real
# copies: an observe-only ("shadow") subscriber's timeout or failure must not
# land on the list of copies re-thrown for the whole event. A shadow copy can
# still be requeued explicitly via the per-delivery or per-destination
# endpoints — its lifecycle is its own.
EVENT_REQUEUEABLE_WHERE = REQUEUEABLE_WHERE + "    AND NOT observe_only\n"

REQUEUE_SET_SQL = """
    status = 'pending',
    reconcile_state = 'none',
    reconcile_deadline = NULL,
    reconciled_at = NULL,
    receipt_result = NULL,
    receipt_id = NULL,
    claim_token = NULL,
    claimed_at = NULL,
    lease_until = NULL,
    next_attempt_at = now(),
    requeue_count = requeue_count + 1,
    updated_at = now()
"""

# Revive a parked copy out of the dead-letter area. Unlike a normal requeue
# this is the only thing that can touch a 'dead_lettered' row, and it starts
# the copy's budgets fresh (the failure streak and the requeue-cycle counter
# reset so a parked copy is not immediately parked again). The copy keeps its
# original destination_seq, so it re-enters the per-destination queue at its
# original position; the worker additionally refuses to claim anything while
# another copy of that destination is in flight, so a revived copy never jumps
# ahead of one currently being delivered.
DEAD_LETTER_REVIVE_SQL = """
    status = 'pending',
    reconcile_state = 'none',
    reconcile_deadline = NULL,
    reconciled_at = NULL,
    receipt_result = NULL,
    receipt_id = NULL,
    claim_token = NULL,
    claimed_at = NULL,
    lease_until = NULL,
    next_attempt_at = now(),
    consecutive_failures = 0,
    requeue_count = 0,
    dead_letter_reason = NULL,
    dead_lettered_at = NULL,
    updated_at = now()
"""


@app.post("/v1/receipts", response_model=ReceiptOut)
def receive_receipt(body: ReceiptIn, db: Session = Depends(get_db)):
    destination = db.execute(
        text(
            """
            SELECT 1 FROM destinations
            WHERE id = CAST(:destination_id AS UUID)
            """
        ),
        {"destination_id": str(body.destination_id)},
    ).first()
    if destination is None:
        raise HTTPException(status_code=404, detail="destination not found")
    return ingest_receipt(db, body.destination_id, body.dedupe_key, body.result)


@app.get("/v1/receipts", response_model=list[ReceiptOut])
def list_receipts(
    destination_id: UUID | None = None,
    dedupe_key: str | None = None,
    disposition: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    if disposition is not None and disposition not in (
        "applied",
        "duplicate",
        "late",
        "orphan",
        "premature",
    ):
        raise HTTPException(status_code=422, detail="invalid disposition")
    return db.execute(
        text(
            """
            SELECT id, destination_id, dedupe_key, result, delivery_id,
                   disposition, received_at
            FROM receipts
            WHERE (CAST(:destination_id AS UUID) IS NULL
                   OR destination_id = CAST(:destination_id AS UUID))
              AND (CAST(:dedupe_key AS TEXT) IS NULL
                   OR dedupe_key = CAST(:dedupe_key AS TEXT))
              AND (CAST(:disposition AS TEXT) IS NULL
                   OR disposition = CAST(:disposition AS TEXT))
            ORDER BY received_at DESC, id DESC
            LIMIT :limit
            """
        ),
        {
            "destination_id": str(destination_id) if destination_id else None,
            "dedupe_key": dedupe_key,
            "disposition": disposition,
            "limit": limit,
        },
    ).mappings().all()


@app.get("/v1/reconciliations/summary", response_model=ReconciliationSummaryOut)
def reconciliation_summary(db: Session = Depends(get_db)):
    rows = db.execute(
        text(
            """
            SELECT reconcile_state, COUNT(*)::int AS count
            FROM deliveries
            WHERE reconcile_state <> 'none'
            GROUP BY reconcile_state
            """
        )
    ).mappings().all()
    counts = {row["reconcile_state"]: row["count"] for row in rows}
    return {
        "awaiting": counts.get("awaiting", 0),
        "acknowledged": counts.get("acknowledged", 0),
        "receipt_failed": counts.get("receipt_failed", 0),
        "timed_out": counts.get("timed_out", 0),
    }


@app.get("/v1/reconciliations/deliveries", response_model=list[DeliveryOut])
def list_reconciliation_deliveries(
    reconcile_state: str = "timed_out",
    destination_id: UUID | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    if reconcile_state not in RECONCILE_STATES:
        raise HTTPException(status_code=422, detail="invalid reconcile_state")
    return db.execute(
        text(
            f"""
            SELECT {DELIVERY_COLUMNS}
            FROM deliveries d
            JOIN destinations dest ON dest.id = d.destination_id
            WHERE d.reconcile_state = :reconcile_state
              AND (CAST(:destination_id AS UUID) IS NULL
                   OR d.destination_id = CAST(:destination_id AS UUID))
            ORDER BY d.reconcile_deadline ASC NULLS LAST, d.destination_seq ASC
            LIMIT :limit
            """
        ),
        {
            "reconcile_state": reconcile_state,
            "destination_id": str(destination_id) if destination_id else None,
            "limit": limit,
        },
    ).mappings().all()


@app.post("/v1/deliveries/{delivery_id}/requeue", response_model=RequeueOut)
def requeue_delivery(delivery_id: UUID, db: Session = Depends(get_db)):
    requeued = db.execute(
        text(
            f"""
            UPDATE deliveries
            SET {REQUEUE_SET_SQL}
            WHERE id = CAST(:delivery_id AS UUID)
              AND {REQUEUEABLE_WHERE}
            RETURNING id, destination_id, destination_seq
            """
        ),
        {"delivery_id": str(delivery_id)},
    ).mappings().first()
    if requeued is None:
        existing = db.execute(
            text(
                """
                SELECT d.status, d.reconcile_state, d.confirmation_generation,
                       dest.confirmation_state AS destination_state,
                       dest.confirmation_generation AS destination_generation
                FROM deliveries d
                JOIN destinations dest ON dest.id = d.destination_id
                WHERE d.id = CAST(:delivery_id AS UUID)
                """
            ),
            {"delivery_id": str(delivery_id)},
        ).mappings().first()
        if existing is None:
            db.rollback()
            raise HTTPException(status_code=404, detail="delivery not found")
        if existing["destination_state"] != "confirmed":
            db.rollback()
            raise HTTPException(
                status_code=409,
                detail=(
                    "destination has not completed its activation handshake; "
                    "deliveries cannot be sent to it yet"
                ),
            )
        if existing["confirmation_generation"] != existing["destination_generation"]:
            db.rollback()
            raise HTTPException(
                status_code=409,
                detail=(
                    "destination changed location after this copy was fanned "
                    "out; old copies are not resent to the new location"
                ),
            )
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail=(
                "delivery is not requeueable: only delivered copies in "
                "reconcile state timed_out or receipt_failed can be requeued "
                f"(current: status={existing['status']}, "
                f"reconcile_state={existing['reconcile_state']})"
            ),
        )
    db.commit()
    return {
        "delivery_id": requeued["id"],
        "destination_id": requeued["destination_id"],
        "destination_seq": requeued["destination_seq"],
        "requeued": True,
    }


@app.post(
    "/v1/destinations/{destination_id}/requeue-unreconciled",
    response_model=BulkRequeueOut,
)
def requeue_unreconciled(destination_id: UUID, db: Session = Depends(get_db)):
    destination = db.execute(
        text(
            """
            SELECT 1 FROM destinations
            WHERE id = CAST(:destination_id AS UUID)
            """
        ),
        {"destination_id": str(destination_id)},
    ).first()
    if destination is None:
        raise HTTPException(status_code=404, detail="destination not found")
    requeued = db.execute(
        text(
            f"""
            UPDATE deliveries
            SET {REQUEUE_SET_SQL}
            WHERE destination_id = CAST(:destination_id AS UUID)
              AND {REQUEUEABLE_WHERE}
            RETURNING id
            """
        ),
        {"destination_id": str(destination_id)},
    ).mappings().all()
    db.commit()
    return {"destination_id": destination_id, "requeued_count": len(requeued)}


# --- Dead-letter area -------------------------------------------------------
#
# A copy arrives here in exactly two ways:
#  * its own consecutive transport failures reached MAX_DELIVERY_ATTEMPTS
#    (reason delivery_attempts_exhausted, put here by the worker);
#  * it was handed off (2xx) but its receipt kept not matching after
#    MAX_REQUEUE_CYCLES requeues — a deadline timeout
#    (receipt_timeout_exhausted, put here by the reconciler) or a failure
#    receipt on the last allowed cycle (receipt_failure_exhausted, put here by
#    the receipt endpoint).
# Parked copies are never claimed, never block later copies of the same
# address, are never treated as acknowledged, and only leave via an explicit
# manual revive that puts them back at their original queue position.

DEAD_LETTER_REASONS = (
    "delivery_attempts_exhausted",
    "receipt_timeout_exhausted",
    "receipt_failure_exhausted",
)


@app.get("/v1/dead-letters/summary", response_model=DeadLetterSummaryOut)
def dead_letter_summary(db: Session = Depends(get_db)):
    rows = db.execute(
        text(
            """
            SELECT dead_letter_reason, COUNT(*)::int AS count
            FROM deliveries
            WHERE status = 'dead_lettered'
            GROUP BY dead_letter_reason
            """
        )
    ).mappings().all()
    counts = {row["dead_letter_reason"]: row["count"] for row in rows}
    return {
        "total": sum(counts.values()),
        "delivery_attempts_exhausted": counts.get("delivery_attempts_exhausted", 0),
        "receipt_timeout_exhausted": counts.get("receipt_timeout_exhausted", 0),
        "receipt_failure_exhausted": counts.get("receipt_failure_exhausted", 0),
    }


@app.get("/v1/dead-letters", response_model=list[DeliveryOut])
def list_dead_letters(
    destination_id: UUID | None = None,
    event_id: UUID | None = None,
    reason: str | None = None,
    dedupe_key: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    # Every parked copy stays answerable here: which copy (id/dedupe_key/event),
    # which address (destination id + url) and why/when it went in.
    if reason is not None and reason not in DEAD_LETTER_REASONS:
        raise HTTPException(status_code=422, detail="invalid dead_letter_reason")
    return db.execute(
        text(
            f"""
            SELECT {DELIVERY_COLUMNS}
            FROM deliveries d
            JOIN destinations dest ON dest.id = d.destination_id
            WHERE d.status = 'dead_lettered'
              AND (CAST(:destination_id AS UUID) IS NULL
                   OR d.destination_id = CAST(:destination_id AS UUID))
              AND (CAST(:event_id AS UUID) IS NULL
                   OR d.event_id = CAST(:event_id AS UUID))
              AND (CAST(:reason AS TEXT) IS NULL
                   OR d.dead_letter_reason = CAST(:reason AS TEXT))
              AND (CAST(:dedupe_key AS TEXT) IS NULL
                   OR d.dedupe_key = CAST(:dedupe_key AS TEXT))
            ORDER BY d.dead_lettered_at DESC, d.id DESC
            LIMIT :limit
            """
        ),
        {
            "destination_id": str(destination_id) if destination_id else None,
            "event_id": str(event_id) if event_id else None,
            "reason": reason,
            "dedupe_key": dedupe_key,
            "limit": limit,
        },
    ).mappings().all()


@app.post("/v1/dead-letters/{delivery_id}/revive", response_model=DeadLetterReviveOut)
def revive_dead_letter(delivery_id: UUID, db: Session = Depends(get_db)):
    # Lock the copy and its destination so a concurrent revive/requeue/claim
    # decision cannot interleave.
    existing = db.execute(
        text(
            """
            SELECT d.id, d.status, d.dead_letter_reason,
                   dest.confirmation_state AS destination_state,
                   dest.confirmation_generation AS destination_generation,
                   d.confirmation_generation AS delivery_generation
            FROM deliveries d
            JOIN destinations dest ON dest.id = d.destination_id
            WHERE d.id = CAST(:delivery_id AS UUID)
            FOR UPDATE OF d
            """
        ),
        {"delivery_id": str(delivery_id)},
    ).mappings().first()
    if existing is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="delivery not found")
    if existing["status"] != "dead_lettered":
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail=(
                "delivery is not in the dead-letter area "
                f"(current status: {existing['status']})"
            ),
        )
    # Same confirmation gate as a normal requeue: never send before the
    # handshake, never send an old-location copy to a new URL.
    if existing["destination_state"] != "confirmed":
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail=(
                "destination has not completed its activation handshake; "
                "the copy cannot be sent to it yet"
            ),
        )
    if existing["delivery_generation"] != existing["destination_generation"]:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail=(
                "destination changed location after this copy was fanned out; "
                "old copies are not resent to the new location"
            ),
        )

    revived = db.execute(
        text(
            f"""
            UPDATE deliveries
            SET {DEAD_LETTER_REVIVE_SQL}
            WHERE id = CAST(:delivery_id AS UUID)
              AND status = 'dead_lettered'
            RETURNING id, destination_id, destination_seq, dead_letter_reason
            """
        ),
        {"delivery_id": str(delivery_id)},
    ).mappings().first()
    if revived is None:  # pragma: no cover - the row was locked above
        db.rollback()
        raise HTTPException(status_code=409, detail="revive lost a concurrent update")
    db.commit()
    return {
        "delivery_id": revived["id"],
        "destination_id": revived["destination_id"],
        "destination_seq": revived["destination_seq"],
        "dead_letter_reason": existing["dead_letter_reason"],
        "revived": True,
    }
