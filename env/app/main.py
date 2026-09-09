import json
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import text
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
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready(db: Session = Depends(get_db)) -> dict[str, str]:
    db.execute(text("SELECT 1"))
    return {"status": "ready"}


@app.post(
    "/v1/destinations",
    response_model=DestinationOut,
    status_code=status.HTTP_201_CREATED,
)
def register_destination(body: DestinationIn, db: Session = Depends(get_db)):
    url = str(body.url)
    result = db.execute(
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
    db.commit()
    return result


@app.get("/v1/destinations/{destination_id}", response_model=DestinationOut)
def get_destination(destination_id: UUID, db: Session = Depends(get_db)):
    result = db.execute(
        text(
            """
            SELECT id, url, status, failure_count, recoverable_at, created_at
            FROM destinations
            WHERE id = CAST(:destination_id AS UUID)
            """
        ),
        {"destination_id": destination_id},
    ).mappings().first()
    if result is None:
        raise HTTPException(status_code=404, detail="destination not found")
    return result


@app.post("/v1/events", response_model=EventOut, status_code=status.HTTP_201_CREATED)
def create_event(body: EventIn, db: Session = Depends(get_db)):
    inserted = db.execute(
        text(
            """
            WITH locked_destination AS (
                SELECT id, next_event_seq
                FROM destinations
                WHERE url = :url
                FOR UPDATE
            ), inserted AS (
                INSERT INTO events
                    (destination_id, dedupe_key, payload, destination_seq)
                SELECT id, :dedupe_key, CAST(:payload AS JSONB),
                       next_event_seq + 1
                FROM locked_destination
                ON CONFLICT (destination_id, dedupe_key) DO NOTHING
                RETURNING *
            ), advance AS (
                UPDATE destinations d
                SET next_event_seq = next_event_seq + 1
                FROM inserted
                WHERE d.id = inserted.destination_id
            )
            SELECT *, FALSE AS duplicate FROM inserted
            """
        ),
        {
            "url": str(body.destination_url),
            "dedupe_key": body.dedupe_key,
            "payload": json.dumps(body.payload),
        },
    ).mappings().first()

    if inserted is not None:
        db.commit()
        return inserted

    existing = db.execute(
        text(
            """
            SELECT e.*
            FROM destinations d
            JOIN events e ON e.destination_id = d.id
            WHERE d.url = :url
              AND e.dedupe_key = :dedupe_key
            """
        ),
        {"url": str(body.destination_url), "dedupe_key": body.dedupe_key},
    ).mappings().first()

    if existing is None:
        db.rollback()
        raise HTTPException(status_code=400, detail="destination is not registered")

    existing_dict = dict(existing)
    existing_dict["duplicate"] = True
    db.rollback()
    return existing_dict

@app.get("/v1/events/{event_id}/trace", response_model=EventTraceOut)
def get_event_trace(event_id: UUID, db: Session = Depends(get_db)):
    event = db.execute(
        text(
            """
            SELECT *, FALSE AS duplicate
            FROM events
            WHERE id = CAST(:event_id AS UUID)
            """
        ),
        {"event_id": event_id},
    ).mappings().first()
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")

    attempts = db.execute(
        text(
            """
            SELECT id, event_id, destination_id, attempt_no,
                   started_at, finished_at, success, status_code,
                   response_excerpt, error, lost_lease
            FROM delivery_attempts
            WHERE event_id = CAST(:event_id AS UUID)
            ORDER BY attempt_no ASC, id ASC
            """
        ),
        {"event_id": event_id},
    ).mappings().all()
    return {"event": event, "attempts": attempts}


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
                WITH reset_events AS (
                    UPDATE events
                    SET next_attempt_at = now(),
                        updated_at = now()
                    WHERE destination_id = CAST(:destination_id AS UUID)
                      AND status = 'pending'
                    RETURNING id
                )
                SELECT count(*)::int AS count FROM reset_events
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

    db.commit()
    return {
        "destination": destination,
        "recovered": recovered,
        "pending_events_reset": reset_count if recovered else None,
    }
