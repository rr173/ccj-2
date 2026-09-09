import json
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from app.db import engine, get_db
from app.models import init_db
from app.schemas import (
    DestinationIn,
    DestinationOut,
    EventIn,
    EventOut,
    EventTraceOut,
    RecoveryOut,
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
           COUNT(d.id) FILTER (WHERE d.status = 'delivered')::int AS delivered_count
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
                   d.last_error, d.created_at, d.updated_at, d.delivered_at
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
    return {
        "event": event_response(event),
        "deliveries": deliveries,
        "attempts": attempts,
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
