import json
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from app.db import engine, get_db
from app.ingest_auth import (
    ACCEPTED,
    DUPLICATE,
    INVALID_BODY,
    SOURCE_DISABLED,
    UNROUTED,
    AdmissionError,
    authenticate,
    generate_secret,
    log_attempt,
    parse_event_body,
)
from app.models import init_db
from app.receipts import ingest_receipt
from app.schemas import (
    BulkRequeueOut,
    DeliveryOut,
    DestinationIn,
    DestinationOut,
    EventBulkRequeueOut,
    EventIn,
    EventOut,
    EventRescheduleIn,
    EventTraceOut,
    IngestionAttemptOut,
    ReceiptIn,
    ReceiptOut,
    ReconciliationSummaryOut,
    RecoveryOut,
    RequeueOut,
    SourceCreatedOut,
    SourceIn,
    SourceOut,
    SourceRotatedOut,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(engine)
    yield


app = FastAPI(
    title="Event Ingest Service",
    version="2.3.0",
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


def destination_response(db: Session, destination: RowMapping) -> dict:
    result = dict(destination)
    result["event_types"] = fetch_event_types(db, destination["id"])
    return result


def event_status(cancelled_at, delivery_count: int, delivered_count: int) -> str:
    if cancelled_at is not None:
        return "cancelled"
    if delivery_count == 0:
        return "unrouted"
    if delivered_count >= delivery_count:
        return "delivered"
    return "pending"


def event_reconcile_status(delivery_count: int, acknowledged_count: int) -> str:
    if delivery_count > 0 and acknowledged_count >= delivery_count:
        return "acknowledged"
    if acknowledged_count > 0:
        return "partially_acknowledged"
    return "pending"


def event_response(event: RowMapping | dict[str, Any]) -> dict:
    result = dict(event)
    result["status"] = event_status(
        result.get("cancelled_at"),
        result["delivery_count"],
        result["delivered_count"],
    )
    result["reconcile_status"] = event_reconcile_status(
        result["delivery_count"], result["acknowledged_count"]
    )
    result["unacknowledged_count"] = (
        result["delivery_count"] - result["acknowledged_count"]
    )
    return result


EVENT_WITH_COUNTS_SQL = """
    SELECT e.id, e.source_id, e.event_type, e.dedupe_key, e.payload, e.created_at,
           e.not_before, e.cancelled_at,
           COUNT(d.id)::int AS delivery_count,
           COUNT(d.id) FILTER (WHERE d.status = 'delivered')::int AS delivered_count,
           COUNT(d.id) FILTER (WHERE d.reconcile_state = 'acknowledged')::int
               AS acknowledged_count
    FROM events e
    LEFT JOIN deliveries d ON d.event_id = e.id
    WHERE {where}
    GROUP BY e.id
"""


@app.post(
    "/v1/destinations",
    response_model=DestinationOut,
    status_code=status.HTTP_201_CREATED,
)
def register_destination(body: DestinationIn, db: Session = Depends(get_db)):
    url = str(body.url)
    destination = db.execute(
        text(
            """
            INSERT INTO destinations (url)
            VALUES (:url)
            ON CONFLICT (url) DO UPDATE SET url = EXCLUDED.url
            RETURNING id, url, status, failure_count, recoverable_at, created_at
            """
        ),
        {"url": url},
    ).mappings().one()

    if body.event_types is not None:
        db.execute(
            text(
                """
                DELETE FROM destination_subscriptions
                WHERE destination_id = :destination_id
                """
            ),
            {"destination_id": destination["id"]},
        )
        if body.event_types:
            db.execute(
                text(
                    """
                    INSERT INTO destination_subscriptions (destination_id, event_type)
                    VALUES (:destination_id, :event_type)
                    ON CONFLICT DO NOTHING
                    """
                ),
                [
                    {"destination_id": destination["id"], "event_type": event_type}
                    for event_type in body.event_types
                ],
            )

    result = destination_response(db, destination)
    db.commit()
    return result


@app.get("/v1/destinations/{destination_id}", response_model=DestinationOut)
def get_destination(destination_id: UUID, db: Session = Depends(get_db)):
    destination = db.execute(
        text(
            """
            SELECT id, url, status, failure_count, recoverable_at, created_at
            FROM destinations
            WHERE id = CAST(:destination_id AS UUID)
            """
        ),
        {"destination_id": destination_id},
    ).mappings().first()
    if destination is None:
        raise HTTPException(status_code=404, detail="destination not found")
    return destination_response(db, destination)


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
    event = db.execute(
        text(
            """
            INSERT INTO events (source_id, event_type, dedupe_key, payload, not_before)
            VALUES (CAST(:source_id AS UUID), :event_type, :dedupe_key,
                    CAST(:payload AS JSONB), CAST(:not_before AS TIMESTAMPTZ))
            ON CONFLICT (dedupe_key) DO NOTHING
            RETURNING id, source_id, event_type, dedupe_key, payload, not_before,
                      cancelled_at, created_at
            """
        ),
        {
            "source_id": str(source["id"]),
            "event_type": body.event_type,
            "dedupe_key": body.dedupe_key,
            "payload": payload,
            "not_before": body.not_before,
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

    # Fan out to the destinations subscribed to this type right now. Each gets
    # its own queued copy with the next per-destination sequence number, so
    # one destination's backlog, retries or isolation never affect the others.
    # Destinations are locked in id order to keep concurrent fan-outs
    # deadlock-free. The schedule gate (not_before) is copied onto every
    # delivery; each copy keeps its queue position while it waits for its time.
    subscribers = db.execute(
        text(
            """
            SELECT destination_id
            FROM destination_subscriptions
            WHERE event_type = :event_type
            ORDER BY destination_id
            """
        ),
        {"event_type": body.event_type},
    ).all()

    for row in subscribers:
        db.execute(
            text(
                """
                WITH bumped AS (
                    UPDATE destinations
                    SET next_event_seq = next_event_seq + 1
                    WHERE id = :destination_id
                    RETURNING id, next_event_seq
                )
                INSERT INTO deliveries
                    (event_id, destination_id, event_type, dedupe_key, payload,
                     destination_seq, not_before)
                SELECT :event_id, id, :event_type, :dedupe_key,
                       CAST(:payload AS JSONB), next_event_seq,
                       CAST(:not_before AS TIMESTAMPTZ)
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
            },
        )

    signed_at = source["signed_at"]
    # No subscribers: still accepted and persisted (the event is "unrouted"),
    # but the disposition makes explicit that nothing was sent out.
    disposition = UNROUTED if not subscribers else ACCEPTED
    log_attempt(
        db,
        disposition=disposition,
        source_id=str(source["id"]),
        source_name=source["name"],
        event_id=str(event["id"]),
        dedupe_key=body.dedupe_key,
        event_type=body.event_type,
        signed_at=signed_at,
    )
    db.commit()
    result = dict(event)
    result["delivery_count"] = len(subscribers)
    result["delivered_count"] = 0
    result["acknowledged_count"] = 0
    return event_response(result), 201


@app.get("/v1/events/{event_id}/trace", response_model=EventTraceOut)
def get_event_trace(event_id: UUID, db: Session = Depends(get_db)):
    event = db.execute(
        text(EVENT_WITH_COUNTS_SQL.format(where="e.id = CAST(:event_id AS UUID)")),
        {"event_id": event_id},
    ).mappings().first()
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")

    deliveries = db.execute(
        text(
            """
            SELECT d.id, d.event_id, d.destination_id, dest.url AS destination_url,
                   d.destination_seq, d.status, d.attempts, d.next_attempt_at,
                   d.not_before, d.last_error, d.created_at, d.updated_at,
                   d.delivered_at, d.reconcile_state, d.reconcile_deadline,
                   d.reconciled_at, d.receipt_result, d.requeue_count
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
    }


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
    row = db.execute(
        text(EVENT_WITH_COUNTS_SQL.format(where="e.id = CAST(:event_id AS UUID)")),
        {"event_id": str(event_id)},
    ).mappings().first()
    return event_response(row)


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
    db.execute(
        text(
            """
            UPDATE deliveries
            SET status = 'cancelled',
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
    event_exists = db.execute(
        text("SELECT 1 FROM events WHERE id = CAST(:event_id AS UUID)"),
        {"event_id": str(event_id)},
    ).first()
    if event_exists is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="event not found")

    # Only terminal receipt failures/timeouts are re-sent. Copies that are
    # acknowledged stay untouched; copies still waiting for a receipt or still
    # in transport are also left alone, so this endpoint never duplicates work.
    requeued = db.execute(
        text(
            f"""
            WITH target_deliveries AS (
                SELECT id
                FROM deliveries
                WHERE event_id = CAST(:event_id AS UUID)
                  AND {REQUEUEABLE_WHERE}
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
    }


@app.post("/v1/destinations/{destination_id}/recover", response_model=RecoveryOut)
def recover_destination(destination_id: UUID, db: Session = Depends(get_db)):
    destination = db.execute(
        text(
            """
            SELECT id, url, status, failure_count, recoverable_at, created_at
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
                """
                UPDATE destinations
                SET status = 'active',
                    failure_count = 0,
                    recoverable_at = NULL
                WHERE id = CAST(:destination_id AS UUID)
                RETURNING id, url, status, failure_count, recoverable_at, created_at
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


# --- Inbound admission log --------------------------------------------------

INGESTION_DISPOSITIONS = (
    ACCEPTED,
    UNROUTED,
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


# --- Receipt ingestion and reconciliation ---------------------------------

RECONCILE_STATES = ("awaiting", "acknowledged", "receipt_failed", "timed_out")


# Only copies whose transport finished but were never acknowledged inside the
# agreed window may be sent back out. They keep their original destination_seq,
# so they rejoin the per-destination queue at their original position.
REQUEUEABLE_WHERE = """
    status = 'delivered'
    AND reconcile_state IN ('timed_out', 'receipt_failed')
"""

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
            """
            SELECT d.id, d.event_id, d.destination_id, dest.url AS destination_url,
                   d.destination_seq, d.status, d.attempts, d.next_attempt_at,
                   d.not_before, d.last_error, d.created_at, d.updated_at,
                   d.delivered_at, d.reconcile_state, d.reconcile_deadline,
                   d.reconciled_at, d.receipt_result, d.requeue_count
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
                SELECT status, reconcile_state
                FROM deliveries
                WHERE id = CAST(:delivery_id AS UUID)
                """
            ),
            {"delivery_id": str(delivery_id)},
        ).mappings().first()
        if existing is None:
            db.rollback()
            raise HTTPException(status_code=404, detail="delivery not found")
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
