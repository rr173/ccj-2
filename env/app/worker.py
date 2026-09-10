import logging
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Any
from uuid import uuid4

import httpx
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.config import settings
from app.db import SessionLocal, build_engine
from app.models import init_db

logger = logging.getLogger("event-worker")


class StaleClaimError(RuntimeError):
    pass


_thread_local = threading.local()


def get_http_client() -> httpx.Client:
    client = getattr(_thread_local, "http_client", None)
    if client is None or client.is_closed:
        client = httpx.Client(timeout=settings.http_timeout_seconds)
        _thread_local.http_client = client
    return client


REAP_SQL = text(
    """
    UPDATE deliveries
    SET status = 'pending',
        claim_token = NULL,
        claimed_at = NULL,
        lease_until = NULL,
        next_attempt_at = LEAST(next_attempt_at, now()),
        updated_at = now()
    WHERE status = 'in_flight'
      AND lease_until < now()
    """
)

HEARTBEAT_SQL = text(
    """
    UPDATE deliveries
    SET lease_until = now() + make_interval(secs => :lease_seconds)
    WHERE id = :delivery_id
      AND status = 'in_flight'
      AND claim_token = :claim_token
    """
)

CLAIM_SQL = text(
    """
    WITH candidate_destination AS (
        SELECT d.*
        FROM destinations d,
        LATERAL (
            SELECT e.id AS delivery_id,
                   e.status AS delivery_status,
                   e.next_attempt_at AS delivery_due_at
            FROM deliveries e
            WHERE e.destination_id = d.id
              AND e.status IN ('pending', 'in_flight')
            ORDER BY e.destination_seq
            LIMIT 1
        ) oldest
        WHERE oldest.delivery_status = 'pending'
          AND oldest.delivery_due_at <= now()
          AND (
                d.status = 'active'
             OR (d.status = 'isolated' AND d.recoverable_at <= now())
        )
          -- A requeued (unreconciled) copy re-enters the queue with its
          -- original, smaller destination_seq. Never claim any copy for a
          -- destination while another copy of it is still in flight: the
          -- requeued copy must not jump ahead of the one being delivered.
          AND NOT EXISTS (
                SELECT 1
                FROM deliveries other_in_flight
                WHERE other_in_flight.destination_id = d.id
                  AND other_in_flight.status = 'in_flight'
          )
        ORDER BY oldest.delivery_due_at
        LIMIT 1
        FOR UPDATE OF d SKIP LOCKED
    ), claimed_event AS (
        UPDATE deliveries e
        SET status = 'in_flight',
            attempts = attempts + 1,
            claim_token = :claim_token,
            claimed_at = now(),
            lease_until = now() + make_interval(secs => :lease_seconds),
            updated_at = now()
        FROM candidate_destination d,
        LATERAL (
            SELECT id, status, next_attempt_at
            FROM deliveries
            WHERE destination_id = d.id
              AND status IN ('pending', 'in_flight')
            ORDER BY destination_seq
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        ) picked
        WHERE e.id = picked.id
          AND picked.status = 'pending'
          AND picked.next_attempt_at <= now()
        RETURNING
            e.*,
            CASE
                WHEN d.status = 'isolated' AND d.recoverable_at <= now()
                THEN TRUE
                ELSE FALSE
            END AS recovered_destination
    ), recovered_destination AS (
        UPDATE destinations d
        SET status = 'active',
            failure_count = 0,
            recoverable_at = NULL
        FROM claimed_event e
        WHERE d.id = e.destination_id
          AND e.recovered_destination
        RETURNING d.id
    )
    SELECT
        e.id AS delivery_id,
        e.event_id,
        e.destination_id,
        e.event_type,
        e.dedupe_key,
        e.payload,
        e.destination_seq,
        e.attempts,
        e.claim_token,
        d.url AS destination_url,
        e.recovered_destination
    FROM claimed_event e
    JOIN candidate_destination d ON d.id = e.destination_id
    """
)

EVENT_SUCCESS_SQL = text(
    """
    UPDATE deliveries
    SET status = 'delivered',
        claim_token = NULL,
        claimed_at = NULL,
        lease_until = NULL,
        last_error = NULL,
        updated_at = now(),
        delivered_at = now(),
        reconcile_state = 'awaiting',
        reconcile_deadline = now() + make_interval(secs => :receipt_timeout_seconds),
        reconciled_at = NULL,
        receipt_result = NULL,
        receipt_id = NULL
    WHERE id = :delivery_id
      AND status = 'in_flight'
      AND claim_token = :claim_token
    """
)

DESTINATION_SUCCESS_SQL = text(
    """
    UPDATE destinations
    SET failure_count = 0
    WHERE id = :destination_id
    """
)

FAILURE_SQL = text(
    """
    WITH failed_event AS (
        UPDATE deliveries
        SET status = 'pending',
            claim_token = NULL,
            claimed_at = NULL,
            lease_until = NULL,
            last_error = :error,
            next_attempt_at = :next_attempt_at,
            updated_at = now()
        WHERE id = :delivery_id
          AND status = 'in_flight'
          AND claim_token = :claim_token
        RETURNING destination_id
    )
    UPDATE destinations d
    SET failure_count = failure_count + 1,
        status = CASE
            WHEN failure_count + 1 >= :failure_threshold THEN 'isolated'
            ELSE status
        END,
        recoverable_at = CASE
            WHEN failure_count + 1 >= :failure_threshold THEN :recoverable_at
            ELSE recoverable_at
        END
    FROM failed_event fe
    WHERE d.id = fe.destination_id
    """
)

ATTEMPT_SQL = text(
    """
    INSERT INTO delivery_attempts (
        delivery_id, event_id, destination_id, attempt_no, started_at,
        finished_at, success, status_code, response_excerpt, error, lost_lease
    ) VALUES (
        :delivery_id, :event_id, :destination_id, :attempt_no, :started_at,
        :finished_at, :success, :status_code, :response_excerpt, :error,
        :lost_lease
    )
    """
)

# Used when this worker's lease expired mid-delivery and another worker took
# over. We only append an audit row; the event/destination are owned by the
# new worker and must not be touched.
AUDIT_ATTEMPT_SQL = text(
    """
    INSERT INTO delivery_attempts (
        delivery_id, event_id, destination_id, attempt_no, started_at,
        finished_at, success, status_code, response_excerpt, error, lost_lease
    ) VALUES (
        :delivery_id, :event_id, :destination_id, :attempt_no, :started_at,
        :finished_at, :success, :status_code, :response_excerpt, :error, TRUE
    )
    """
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def backoff_delay(attempt: int) -> timedelta:
    base = settings.retry_backoff_base_seconds
    maximum = settings.retry_backoff_max_seconds
    delay = min(maximum, base * (2 ** max(0, attempt - 1)))
    jitter = random.uniform(0, delay * 0.25)
    return timedelta(seconds=delay + jitter)


def truncate(value: str | None) -> str | None:
    if value is None:
        return None
    return value[: settings.max_response_body_bytes]


class LeaseHeartbeat:
    """Renews an in-flight event's lease while its HTTP call is running.

    A slow receiver must not look like a dead worker: as long as this thread
    keeps renewing ``lease_until`` no other worker takes the event. If the
    process dies, the thread stops with it and the lease expires, allowing
    another worker to resume the undelivered event.
    """

    def __init__(self, claim: RowMapping) -> None:
        self.claim = claim
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"heartbeat-{claim['delivery_id']}", daemon=True
        )

    @property
    def lost(self) -> bool:
        return self._lost.is_set()

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=10)

    def _run(self) -> None:
        interval = max(
            1.0,
            min(
                settings.lease_heartbeat_seconds,
                settings.claim_lease_seconds / 3.0,
            ),
        )
        while not self._stop.wait(interval):
            db = SessionLocal()
            try:
                updated = db.execute(
                    HEARTBEAT_SQL,
                    {
                        "delivery_id": self.claim["delivery_id"],
                        "claim_token": self.claim["claim_token"],
                        "lease_seconds": settings.claim_lease_seconds,
                    },
                )
                db.commit()
                if updated.rowcount != 1:
                    # Another worker already took the lease over; stop acting
                    # like we still own this delivery.
                    self._lost.set()
                    return
            except SQLAlchemyError:
                # Keep retrying: a transient DB outage should not by itself
                # surrender the lease. If it lasts beyond the lease window the
                # lease genuinely expires, which is the desired takeover.
                db.rollback()
                logger.warning(
                    "lease heartbeat DB error for delivery_id=%s; retrying",
                    self.claim["delivery_id"],
                )
            finally:
                db.close()


def deliver(
    client: httpx.Client,
    url: str,
    event_id: str,
    delivery_id: str,
    event_type: str | None,
    destination_id: str,
    dedupe_key: str,
    payload: dict[str, Any],
    destination_seq: int,
) -> dict[str, Any]:
    body = {
        "event_id": event_id,
        "delivery_id": delivery_id,
        "event_type": event_type,
        "destination_id": destination_id,
        "destination_seq": destination_seq,
        "dedupe_key": dedupe_key,
        "payload": payload,
    }
    headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": dedupe_key,
        "X-Event-Id": event_id,
        "X-Delivery-Id": delivery_id,
    }
    if event_type is not None:
        headers["X-Event-Type"] = event_type
    started = utc_now()
    try:
        response = client.post(url, json=body, headers=headers)
        finished = utc_now()
        success = 200 <= response.status_code < 300
        return {
            "started_at": started,
            "finished_at": finished,
            "success": success,
            "status_code": response.status_code,
            "response_excerpt": truncate(response.content.decode(response.encoding or "utf-8", errors="replace")),
            "error": None if success else f"HTTP {response.status_code}",
        }
    except Exception as exc:
        return {
            "started_at": started,
            "finished_at": utc_now(),
            "success": False,
            "status_code": None,
            "response_excerpt": None,
            "error": truncate(f"{type(exc).__name__}: {exc}"),
        }


def still_owns_lease(db: Session, claim: RowMapping) -> bool:
    """Authoritative ownership check done in the result-writing transaction."""
    row = db.execute(
        text(
            """
            SELECT 1
            FROM deliveries
            WHERE id = :delivery_id
              AND status = 'in_flight'
              AND claim_token = :claim_token
            """
        ),
        {
            "delivery_id": claim["delivery_id"],
            "claim_token": claim["claim_token"],
        },
    ).first()
    return row is not None


def record_result(
    db: Session,
    claim: RowMapping,
    result: MappingProxyType | dict[str, Any],
    lease_lost: bool,
) -> None:
    lease_lost = lease_lost or not still_owns_lease(db, claim)
    attempt_params = {
        "delivery_id": claim["delivery_id"],
        "event_id": claim["event_id"],
        "destination_id": claim["destination_id"],
        "attempt_no": claim["attempts"],
        "started_at": result["started_at"],
        "finished_at": result["finished_at"],
        "success": result["success"],
        "status_code": result["status_code"],
        "response_excerpt": result["response_excerpt"],
        "error": result["error"],
    }

    # The lease expired while we were still talking to a slow receiver and
    # another worker took over. We must not mutate the delivery/destination
    # (those are owned by the new worker); only append an audit row so the
    # duplicate in-flight call stays visible in the trace.
    if lease_lost:
        db.execute(AUDIT_ATTEMPT_SQL, attempt_params)
        db.commit()
        logger.warning(
            "lease lost for delivery_id=%s during attempt=%s; result not applied "
            "(success=%s, status_code=%s)",
            claim["delivery_id"],
            claim["attempts"],
            result["success"],
            result["status_code"],
        )
        return

    next_attempt_at = utc_now() + backoff_delay(claim["attempts"])
    should_isolate = False
    recoverable_at = None
    if not result["success"]:
        should_isolate = claim["attempts"] >= settings.failure_threshold
        if should_isolate:
            recoverable_at = utc_now() + timedelta(seconds=settings.quarantine_seconds)

    if result["success"]:
        updated = db.execute(
            EVENT_SUCCESS_SQL,
            {
                "delivery_id": claim["delivery_id"],
                "claim_token": claim["claim_token"],
                "receipt_timeout_seconds": settings.receipt_timeout_seconds,
            },
        )
        if updated.rowcount != 1:
            raise StaleClaimError(f"delivery {claim['delivery_id']} is no longer owned by this worker")
        db.execute(
            DESTINATION_SUCCESS_SQL,
            {"destination_id": claim["destination_id"]},
        )
    else:
        updated = db.execute(
            FAILURE_SQL,
            {
                "delivery_id": claim["delivery_id"],
                "claim_token": claim["claim_token"],
                "error": result["error"],
                "next_attempt_at": next_attempt_at,
                "failure_threshold": settings.failure_threshold,
                "recoverable_at": recoverable_at,
            },
        )
        if updated.rowcount != 1:
            raise StaleClaimError(f"delivery {claim['delivery_id']} is no longer owned by this worker")

    db.execute(
        ATTEMPT_SQL,
        {**attempt_params, "lost_lease": False},
    )

    db.commit()
    if should_isolate:
        logger.warning(
            "destination_id=%s isolated after delivery_id=%s failed %s times",
            claim["destination_id"],
            claim["delivery_id"],
            claim["attempts"],
        )


def claim_next_event(db: Session) -> RowMapping | None:
    # Reap expired leases before selecting. Reaper and claim run in one
    # transaction. A genuinely live HTTP call keeps its heartbeat fresh, so
    # its lease never expires and it keeps holding the destination's head;
    # only a dead worker stops heartbeating and becomes eligible to take over.
    db.execute(REAP_SQL)
    result = db.execute(
        CLAIM_SQL,
        {
            "claim_token": uuid4(),
            "lease_seconds": settings.claim_lease_seconds,
        },
    ).mappings().first()
    db.commit()
    return result


def process_once() -> bool:
    db = SessionLocal()
    claim = None
    heartbeat = None
    try:
        claim = claim_next_event(db)
        if claim is None:
            return False

        if claim["recovered_destination"]:
            logger.info(
                "automatically recovered destination_id=%s",
                claim["destination_id"],
            )

        heartbeat = LeaseHeartbeat(claim)
        heartbeat.start()
        result = deliver(
            client=get_http_client(),
            url=claim["destination_url"],
            event_id=str(claim["event_id"]),
            delivery_id=str(claim["delivery_id"]),
            event_type=claim["event_type"],
            destination_id=str(claim["destination_id"]),
            dedupe_key=claim["dedupe_key"],
            payload=claim["payload"],
            destination_seq=claim["destination_seq"],
        )
        heartbeat.stop()

        record_result(db, claim, result, lease_lost=heartbeat.lost)
        if result["success"]:
            logger.info(
                "delivered delivery_id=%s event_id=%s destination_id=%s attempt=%s",
                claim["delivery_id"],
                claim["event_id"],
                claim["destination_id"],
                claim["attempts"],
            )
        else:
            logger.warning(
                "delivery failed delivery_id=%s event_id=%s destination_id=%s attempt=%s error=%s",
                claim["delivery_id"],
                claim["event_id"],
                claim["destination_id"],
                claim["attempts"],
                result["error"],
            )
        return True
    except StaleClaimError:
        db.rollback()
        logger.warning("stale delivery claim ignored; another worker will retry it")
        return False
    except SQLAlchemyError:
        db.rollback()
        logger.exception("database error while processing event")
        return False
    except Exception:
        db.rollback()
        logger.exception("unexpected worker error")
        return False
    finally:
        if heartbeat is not None:
            heartbeat.stop()
        db.close()


def worker_loop(stop_event: threading.Event) -> None:
    try:
        while not stop_event.is_set():
            did_work = process_once()
            if not did_work:
                stop_event.wait(settings.poll_interval_seconds)
    finally:
        client = getattr(_thread_local, "http_client", None)
        if client is not None and not client.is_closed:
            client.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    init_engine = build_engine(pool_size=1, max_overflow=0)
    try:
        init_db(init_engine)
    finally:
        init_engine.dispose()

    stop_event = threading.Event()
    threads = [
        threading.Thread(target=worker_loop, args=(stop_event,), name=f"worker-{i}")
        for i in range(max(1, settings.worker_concurrency))
    ]
    for thread in threads:
        thread.start()

    try:
        while all(thread.is_alive() for thread in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("shutting down worker")
    finally:
        stop_event.set()
        for thread in threads:
            thread.join(timeout=settings.http_timeout_seconds + 5)


if __name__ == "__main__":
    main()
