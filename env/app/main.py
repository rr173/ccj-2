import json
import time
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from app.config import settings
from app.db import engine, get_db
from app.models import init_db
from app.receipts import ingest_receipt
from app.schemas import (
    BulkRequeueOut,
    DeliveryOut,
    DestinationIn,
    DestinationOut,
    EventIn,
    EventOut,
    EventTraceOut,
    ReceiptIn,
    ReceiptOut,
    ReconciliationSummaryOut,
    RecoveryOut,
    RequeueOut,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(engine)
    yield


app = FastAPI(
    title="Event Ingest Service",
    version="2.0.0",
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


def event_status(delivery_count: int, delivered_count: int) -> str:
    if delivery_count == 0:
        return "unrouted"
    if delivered_count >= delivery_count:
        return "delivered"
    return "pending"


def event_response(event: RowMapping) -> dict:
    result = dict(event)
    result["status"] = event_status(
        result["delivery_count"], result["delivered_count"]
    )
    return result


EVENT_WITH_COUNTS_SQL = """
    SELECT e.id, e.event_type, e.dedupe_key, e.payload, e.created_at,
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


@app.post("/v1/events", response_model=EventOut, status_code=status.HTTP_201_CREATED)
def create_event(body: EventIn, db: Session = Depends(get_db)):
    payload = json.dumps(body.payload)
    event = db.execute(
        text(
            """
            INSERT INTO events (event_type, dedupe_key, payload)
            VALUES (:event_type, :dedupe_key, CAST(:payload AS JSONB))
            ON CONFLICT (dedupe_key) DO NOTHING
            RETURNING id, event_type, dedupe_key, payload, created_at
            """
        ),
        {
            "event_type": body.event_type,
            "dedupe_key": body.dedupe_key,
            "payload": payload,
        },
    ).mappings().first()

    if event is None:
        existing = db.execute(
            text(EVENT_WITH_COUNTS_SQL.format(where="e.dedupe_key = :dedupe_key")),
            {"dedupe_key": body.dedupe_key},
        ).mappings().first()
        db.rollback()
        if existing is None:  # pragma: no cover - cannot happen after a conflict
            raise HTTPException(status_code=500, detail="event insert conflict lost")
        result = event_response(existing)
        result["duplicate"] = True
        return result

    # Fan out to the destinations subscribed to this type right now. Each gets
    # its own queued copy with the next per-destination sequence number, so
    # one destination's backlog, retries or isolation never affect the others.
    # Destinations are locked in id order to keep concurrent fan-outs
    # deadlock-free.
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
                     destination_seq)
                SELECT :event_id, id, :event_type, :dedupe_key,
                       CAST(:payload AS JSONB), next_event_seq
                FROM bumped
                """
            ),
            {
                "event_id": event["id"],
                "destination_id": row[0],
                "event_type": body.event_type,
                "dedupe_key": body.dedupe_key,
                "payload": payload,
            },
        )

    db.commit()
    result = dict(event)
    result["delivery_count"] = len(subscribers)
    result["delivered_count"] = 0
    result["acknowledged_count"] = 0
    result["status"] = event_status(len(subscribers), 0)
    return result


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
                   d.last_error, d.created_at, d.updated_at, d.delivered_at,
                   d.reconcile_state, d.reconcile_deadline, d.reconciled_at,
                   d.receipt_result, d.requeue_count
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
                   d.last_error, d.created_at, d.updated_at, d.delivered_at,
                   d.reconcile_state, d.reconcile_deadline, d.reconciled_at,
                   d.receipt_result, d.requeue_count
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
