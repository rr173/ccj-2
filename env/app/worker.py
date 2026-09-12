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

from app.confirmation import (
    apply_echo,
    expire_due_rounds,
    log_confirmation_attempt,
)
from app.config import settings
from app.db import SessionLocal, build_engine
from app.models import init_db
from app import release as release_gate
from app import relay

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
                   -- A copy is due only when both its retry backoff
                   -- (next_attempt_at) and its scheduled send gate
                   -- (not_before) have passed. A not-yet-due scheduled copy
                   -- keeps its queue position: it is the head, so later
                   -- copies of this destination wait behind it.
                   GREATEST(
                       e.next_attempt_at,
                       COALESCE(e.not_before, '-infinity'::timestamptz)
                   ) AS delivery_due_at,
                   e.confirmation_generation AS delivery_generation,
                   e.consecutive_failures,
                   -- Relay chain ("接力") gate. A station copy is only
                   -- deliverable when it is station 1 or the immediately
                   -- preceding station of THIS event's chain version carries
                   -- a matching success receipt. Later stations stay pending
                   -- in queue position (and are skipped as the head) until
                   -- their predecessor acknowledges; a stop upstream moves
                   -- them to terminal relay_skipped, which is also excluded.
                   (
                        e.relay_chain_id IS NULL
                        OR e.relay_station_no = 1
                        OR EXISTS (
                            SELECT 1
                            FROM deliveries prev
                            WHERE prev.event_id = e.event_id
                              AND prev.relay_chain_id = e.relay_chain_id
                              AND prev.relay_station_no = e.relay_station_no - 1
                              AND prev.reconcile_state = 'acknowledged'
                        )
                   ) AS relay_open
            FROM deliveries e
            WHERE e.destination_id = d.id
              AND e.status IN ('pending', 'in_flight')
              -- Preview-consent gate ("预告 + 点头才给正文"). The head is
              -- picked only among deliverable copies: a gated body stays
              -- pending and queues in seq order, but it is not deliverable
              -- until this same address has nodded (release_state
              -- 'released'), so the preview queued immediately before it is
              -- what becomes the head. Previews, ordinary copies and
              -- corrections (release_state NULL) pass as before.
              AND (
                    e.release_state IS NULL
                 OR e.release_state = 'released'
              )
            ORDER BY e.destination_seq
            LIMIT 1
        ) oldest
        WHERE oldest.delivery_status = 'pending'
          AND oldest.delivery_due_at <= now()
          AND oldest.relay_open
          AND (
                d.status = 'active'
             OR (d.status = 'isolated' AND d.recoverable_at <= now())
        )
          -- Nothing is ever sent to a destination that has not completed its
          -- activation handshake, and an older-generation copy (fanned out
          -- before a URL change) can never be delivered to the new location.
          -- The head itself must be a current-generation confirmed copy; such
          -- a stale head is cleaned up by the supersede step below.
          AND d.confirmation_state = 'confirmed'
          AND oldest.delivery_generation = d.confirmation_generation
          -- A destination inside its operator-marked "not receiving" window
          -- is skipped entirely: its copies keep their queue positions and
          -- wait, no attempt is made (so the pause can never be charged as
          -- consecutive failures or trigger isolation), no reconcile
          -- countdown runs for them, and other destinations subscribed to
          -- the same types keep draining normally.
          AND (
                d.paused_from IS NULL
             OR d.paused_from > now()
             OR (d.paused_until IS NOT NULL AND d.paused_until <= now())
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
            SELECT id, status, next_attempt_at, not_before,
                   confirmation_generation, phase, release_state,
                   preview_delivery_id, relay_chain_id, relay_station_no
            FROM deliveries
            WHERE destination_id = d.id
              AND status IN ('pending', 'in_flight')
              -- Same deliverable gate as the candidate head: held gated
              -- bodies are skipped here too so the preceding preview is the
              -- copy picked.
              AND (release_state IS NULL OR release_state = 'released')
              -- Same relay gate as the candidate head: a station whose
              -- predecessor has not acknowledged yet is never claimed.
              AND (
                    relay_chain_id IS NULL
                 OR relay_station_no = 1
                 OR EXISTS (
                     SELECT 1 FROM deliveries prev
                     WHERE prev.event_id = deliveries.event_id
                       AND prev.relay_chain_id = deliveries.relay_chain_id
                       AND prev.relay_station_no = deliveries.relay_station_no - 1
                       AND prev.reconcile_state = 'acknowledged'
                 )
              )
            ORDER BY destination_seq
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        ) picked
        WHERE e.id = picked.id
          AND picked.status = 'pending'
          AND picked.next_attempt_at <= now()
          AND (picked.not_before IS NULL OR picked.not_before <= now())
          AND picked.confirmation_generation = d.confirmation_generation
          AND (picked.release_state IS NULL OR picked.release_state = 'released')
          AND (
                picked.relay_chain_id IS NULL
             OR picked.relay_station_no = 1
             OR EXISTS (
                 SELECT 1 FROM deliveries prev
                 WHERE prev.event_id = e.event_id
                   AND prev.relay_chain_id = picked.relay_chain_id
                   AND prev.relay_station_no = picked.relay_station_no - 1
                   AND prev.reconcile_state = 'acknowledged'
             )
          )
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
        e.consecutive_failures,
        e.confirmation_generation,
        e.phase,
        e.release_state,
        e.body_delivery_id,
        e.consent_timeout_seconds,
        e.relay_chain_id,
        e.relay_station_no,
        d.url AS destination_url,
        e.recovered_destination,
        ev.corrects_event_id,
        (ev.corrects_event_id IS NOT NULL) AS is_correction
    FROM claimed_event e
    JOIN candidate_destination d ON d.id = e.destination_id
    LEFT JOIN events ev ON ev.id = e.event_id
    """
)

# Old-generation pending copies (fanned out before the destination moved to a
# new URL) are never delivered to the new location and are never backfilled:
# move them to the terminal superseded state. An in-flight copy that was
# already talking to the old URL is not touched here — its result path decides
# whether it completes or is superseded.
SUPERSEDE_STALE_SQL = text(
    """
    WITH stale AS (
        SELECT e.id, e.relay_chain_id
        FROM deliveries e
        JOIN destinations d ON d.id = e.destination_id
        WHERE e.status = 'pending'
          AND e.confirmation_generation < d.confirmation_generation
        ORDER BY e.destination_id, e.destination_seq
        LIMIT 200
    )
    UPDATE deliveries e
    SET status = 'superseded',
        updated_at = now()
    FROM stale
    WHERE e.id = stale.id
    RETURNING e.id, stale.relay_chain_id
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
        consecutive_failures = 0,
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
        -- A copy whose own consecutive-failure streak reaches the limit is
        -- parked in the dead-letter area: it never goes back on the queue and
        -- the worker never claims 'dead_lettered' rows, so later copies of
        -- this destination immediately become the head and keep draining.
        SET status = CASE WHEN :dead_letter THEN 'dead_lettered' ELSE 'pending' END,
            consecutive_failures = consecutive_failures + 1,
            claim_token = NULL,
            claimed_at = NULL,
            lease_until = NULL,
            last_error = CASE
                WHEN :dead_letter THEN CONCAT(
                    'dead-lettered after ',
                    consecutive_failures + 1,
                    ' consecutive delivery failures; last error: ',
                    COALESCE(:error, '(none)')
                )
                ELSE :error
            END,
            next_attempt_at = :next_attempt_at,
            dead_letter_reason = CASE
                WHEN :dead_letter THEN 'delivery_attempts_exhausted'
                ELSE dead_letter_reason
            END,
            dead_lettered_at = CASE
                WHEN :dead_letter THEN now() ELSE dead_lettered_at
            END,
            updated_at = now()
        WHERE id = :delivery_id
          AND status = 'in_flight'
          AND claim_token = :claim_token
        RETURNING destination_id
    )
    UPDATE destinations d
    -- The parked copy is out of the queue, so it must not keep the address's
    -- later copies behind a quarantine wall either: its death resets the
    -- address failure tally and lets the next queued copy act as the probe.
    -- If that probe keeps failing the normal threshold re-isolates the
    -- address; other destinations are untouched either way.
    SET failure_count = CASE WHEN :dead_letter THEN 0 ELSE failure_count + 1 END,
        status = CASE
            WHEN :dead_letter THEN 'active'
            WHEN failure_count + 1 >= :failure_threshold THEN 'isolated'
            ELSE status
        END,
        recoverable_at = CASE
            WHEN :dead_letter THEN NULL
            WHEN failure_count + 1 >= :failure_threshold THEN :recoverable_at
            ELSE recoverable_at
        END
    FROM failed_event fe
    WHERE d.id = fe.destination_id
    """
)

# A copy that was already in flight toward the old URL when the destination
# moved must not be retried (the old location must not receive anything more):
# a failed call ends the copy in the terminal superseded state instead of going
# back on the queue, and the transport failure is not charged against the new
# destination.
SUPERSEDE_FAILED_SQL = text(
    """
    UPDATE deliveries
    SET status = 'superseded',
        claim_token = NULL,
        claimed_at = NULL,
        lease_until = NULL,
        last_error = CONCAT('superseded after location change; last error: ',
                            COALESCE(:error, '(none)')),
        updated_at = now()
    WHERE id = :delivery_id
      AND status = 'in_flight'
      AND claim_token = :claim_token
    """
)

# A correction copy that fails its send attempt ends right here in the
# terminal 'failed' state. The failure is accounted entirely on the copy
# itself: the original event's copies are different rows and are never
# touched, the destination's consecutive-failure tally is deliberately NOT
# incremented (a correction failure can never isolate the address), and the
# copy leaves the queue head so later copies of the same destination keep
# draining instead of waiting behind correction retries.
CORRECTION_FAILURE_SQL = text(
    """
    UPDATE deliveries
    SET status = 'failed',
        claim_token = NULL,
        claimed_at = NULL,
        lease_until = NULL,
        last_error = :error,
        updated_at = now()
    WHERE id = :delivery_id
      AND status = 'in_flight'
      AND claim_token = :claim_token
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
    corrects_event_id: str | None = None,
    phase: str = "body",
    preview_payload: dict[str, Any] | None = None,
    consent_timeout_seconds: int | None = None,
    body_delivery_id: str | None = None,
    relay_chain_id: str | None = None,
    relay_station_no: int | None = None,
) -> dict[str, Any]:
    # Previews ("预告") and bodies ("正文") are two distinct messages. A
    # preview never carries the real payload: the receiver only sees the
    # optional short preview text, how long it has to nod, and which event the
    # notice is about. The body message keeps the existing shape and is only
    # ever sent after this same address's nod.
    is_preview = phase == "preview"
    body: dict[str, Any] = {
        "event_id": event_id,
        "delivery_id": delivery_id,
        "event_type": event_type,
        "destination_id": destination_id,
        "destination_seq": destination_seq,
        "dedupe_key": dedupe_key,
        "payload": payload if not is_preview else {},
    }
    headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": dedupe_key,
        "X-Event-Id": event_id,
        "X-Delivery-Id": delivery_id,
    }
    if event_type is not None:
        headers["X-Event-Type"] = event_type
    if is_preview:
        # Lets the receiver route the notice apart from a body delivery and
        # answer via the consent endpoint instead of a business receipt.
        headers["X-Message-Type"] = "event_preview"
        body["message_type"] = "event_preview"
        body["preview_payload"] = preview_payload or {}
        body["consent_timeout_seconds"] = consent_timeout_seconds
        if body_delivery_id is not None:
            body["body_delivery_id"] = body_delivery_id
    if corrects_event_id is not None:
        # A correction is an additional send, not a recall: the receiver can
        # tell it apart and link it to the original event it corrects.
        body["corrects_event_id"] = corrects_event_id
        headers["X-Corrects-Event-Id"] = corrects_event_id
    if relay_chain_id is not None:
        # Relay-chain ("接力") routing: tells the receiver which chain
        # version this copy belongs to and which station it is, so a station
        # can tell ordered relay traffic apart from ordinary fan-out.
        body["relay_chain_id"] = relay_chain_id
        body["relay_station_no"] = relay_station_no
        headers["X-Relay-Chain-Id"] = relay_chain_id
        headers["X-Relay-Station-No"] = str(relay_station_no)
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
    should_dead_letter = False
    recoverable_at = None
    # A correction copy keeps its own failure accounting: its failure is never
    # charged to the destination's consecutive-failure tally (no isolation),
    # never dead-letters through the transport budget and never retries in
    # place — it ends terminally 'failed' further below.
    is_correction = bool(claim["is_correction"])
    # A preview ("预告") is transported/retried like any copy but it never
    # enters receipt reconciliation: a 2xx opens the consent window on its
    # body instead. A preview that exhausts transport attempts dead-letters and
    # its still-held body is cascaded to a terminal void — the address never
    # got the notice, so it can never receive the body.
    is_preview = claim["phase"] == "preview"
    if not result["success"] and not is_correction:
        should_isolate = claim["attempts"] >= settings.failure_threshold
        if should_isolate:
            recoverable_at = utc_now() + timedelta(seconds=settings.quarantine_seconds)
        # A manual revive resets the streak, so this counts failures since
        # the last 2xx (or revive) for this specific copy only.
        should_dead_letter = (
            claim["consecutive_failures"] + 1 >= settings.max_delivery_attempts
        )

    # A destination that changed location while this HTTP call was in flight
    # is no longer the generation this copy belongs to. A 2xx still counts as
    # handed off to the old URL (it is not taken back); a failure must not be
    # retried and must not count against the new location's failure tally.
    generation_row = db.execute(
        text(
            """
            SELECT confirmation_state, confirmation_generation
            FROM destinations
            WHERE id = CAST(:destination_id AS UUID)
            """
        ),
        {"destination_id": claim["destination_id"]},
    ).mappings().first()
    stale_generation = (
        generation_row is None
        or generation_row["confirmation_generation"] != claim["confirmation_generation"]
    )

    if result["success"]:
        if is_preview:
            # The notice landed: mark it delivered (no receipt reconciliation)
            # and open the consent deadline on the still-held body. The body's
            # queue position never changes.
            release_gate.mark_preview_delivered(db, claim["delivery_id"])
        else:
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
    elif stale_generation:
        updated = db.execute(
            SUPERSEDE_FAILED_SQL,
            {
                "delivery_id": claim["delivery_id"],
                "claim_token": claim["claim_token"],
                "error": result["error"],
            },
        )
        if updated.rowcount != 1:
            raise StaleClaimError(f"delivery {claim['delivery_id']} is no longer owned by this worker")
        if is_preview:
            # A preview that died while the destination was relocating never
            # reached this address: its still-held body is voided too (the
            # queued copies are separately superseded by arm_round / the
            # stale cleanup).
            release_gate.void_body_after_preview_failure(
                db,
                preview_id=claim["delivery_id"],
                reason="preview_superseded",
            )
        if claim.get("relay_chain_id") is not None:
            # A relay station whose queued copy was superseded by a location
            # change stops its run: every later pending station is skipped.
            relay.cascade_after_stop(db, str(claim["delivery_id"]))
        logger.info(
            "superseded failed in-flight delivery_id=%s after destination location change",
            claim["delivery_id"],
        )
    elif is_correction:
        updated = db.execute(
            CORRECTION_FAILURE_SQL,
            {
                "delivery_id": claim["delivery_id"],
                "claim_token": claim["claim_token"],
                "error": result["error"],
            },
        )
        if updated.rowcount != 1:
            raise StaleClaimError(f"delivery {claim['delivery_id']} is no longer owned by this worker")
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
                "dead_letter": should_dead_letter,
            },
        )
        if updated.rowcount != 1:
            raise StaleClaimError(f"delivery {claim['delivery_id']} is no longer owned by this worker")
        if is_preview and should_dead_letter:
            # The notice exhausted every transport attempt and is parked in
            # the dead-letter area. Void its still-held body: the address
            # never received the preview, so the content must never go out and
            # must be queryable as "closed (preview never delivered)", not as
            # a delivered body.
            release_gate.void_body_after_preview_failure(
                db,
                preview_id=claim["delivery_id"],
                reason="preview_dead_lettered",
            )
        if should_dead_letter and claim.get("relay_chain_id") is not None:
            # A relay station that exhausted its own transport attempts
            # stops the whole run behind it: no later station is backfilled.
            relay.cascade_after_stop(db, str(claim["delivery_id"]))

    db.execute(
        ATTEMPT_SQL,
        {**attempt_params, "lost_lease": False},
    )

    db.commit()
    if is_correction and not result["success"] and not stale_generation:
        logger.warning(
            "correction delivery_id=%s failed terminally (destination_id=%s, "
            "event_id=%s, error=%s); destination failure tally untouched",
            claim["delivery_id"],
            claim["destination_id"],
            claim["event_id"],
            result["error"],
        )
    elif should_dead_letter and not stale_generation:
        logger.error(
            "delivery_id=%s moved to dead letter after %s consecutive failures "
            "(destination_id=%s, event_id=%s, error=%s)",
            claim["delivery_id"],
            claim["consecutive_failures"] + 1,
            claim["destination_id"],
            claim["event_id"],
            result["error"],
        )
    elif should_isolate and not stale_generation:
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
    # Abandon queued copies of earlier confirmation generations (the
    # destination changed location): they must never go to the new URL and
    # must not block its head as pending forever.
    superseded = db.execute(SUPERSEDE_STALE_SQL).mappings().all()
    # A relay station superseded by a location change stops its whole run:
    # every later station still pending is closed relay_skipped.
    for row in superseded:
        if row["relay_chain_id"] is not None:
            relay.cascade_after_stop(db, str(row["id"]))
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
            corrects_event_id=(
                str(claim["corrects_event_id"])
                if claim["corrects_event_id"]
                else None
            ),
            phase=claim["phase"],
            # Preview rows store the short preview text in payload itself; the
            # real content only exists on the body row queued behind it.
            preview_payload=claim["payload"] if claim["phase"] == "preview" else None,
            consent_timeout_seconds=claim.get("consent_timeout_seconds"),
            body_delivery_id=(
                str(claim["body_delivery_id"])
                if claim.get("body_delivery_id")
                else None
            ),
            relay_chain_id=(
                str(claim["relay_chain_id"])
                if claim.get("relay_chain_id")
                else None
            ),
            relay_station_no=claim.get("relay_station_no"),
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


# --- Activation handshake (confirmer) --------------------------------------


def confirmation_backoff_delay(attempt_count: int) -> float:
    base = settings.confirm_backoff_base_seconds
    maximum = settings.confirm_backoff_max_seconds
    delay = min(maximum, base * (2 ** max(0, attempt_count - 1)))
    return delay + random.uniform(0, delay * 0.25)


def send_confirmation_probe(
    client: httpx.Client,
    *,
    url: str,
    destination_id: str,
    challenge: str,
    round_no: int,
) -> dict[str, Any]:
    body = {
        "type": "activation_challenge",
        "destination_id": destination_id,
        "confirmation_round": round_no,
        "challenge": challenge,
        "challenge_expires_at": None,
    }
    headers = {
        "Content-Type": "application/json",
        # Lets a receiver route this apart from event deliveries and answer
        # by simply echoing the challenge from its 2xx response.
        "X-Message-Type": "activation_challenge",
        "X-Confirmation-Round": str(round_no),
    }
    started = utc_now()
    try:
        response = client.post(url, json=body, headers=headers)
        finished = utc_now()
        excerpt = truncate(
            response.content.decode(response.encoding or "utf-8", errors="replace")
        )
        echo = None
        try:
            parsed = response.json()
            if isinstance(parsed, dict):
                echo = parsed.get("echo") or parsed.get("challenge")
        except Exception:  # noqa: BLE001 - any non-JSON body is just "no echo"
            echo = None
        return {
            "started_at": started,
            "finished_at": finished,
            "success": 200 <= response.status_code < 300,
            "status_code": response.status_code,
            "response_excerpt": excerpt,
            "error": None if 200 <= response.status_code < 300 else f"HTTP {response.status_code}",
            "echo": echo,
        }
    except Exception as exc:
        return {
            "started_at": started,
            "finished_at": utc_now(),
            "success": False,
            "status_code": None,
            "response_excerpt": None,
            "error": truncate(f"{type(exc).__name__}: {exc}"),
            "echo": None,
        }


def process_confirmation_once(db: Session, client: httpx.Client) -> bool:
    # First, close out rounds that expired unanswered and arm fresh ones.
    expired = expire_due_rounds(db)
    if expired:
        db.commit()
        logger.info("re-armed %s expired confirmation rounds", expired)

    # claim_next needs a delay that reflects the count after the increment;
    # fetch the row first to size the backoff, then schedule the next probe.
    claimed = db.execute(
        text(
            """
            SELECT id, url, challenge_token, challenge_expires_at,
                   confirmation_round, confirmation_attempt_count
            FROM destinations
            WHERE confirmation_state = 'pending'
              AND next_probe_at <= now()
            ORDER BY next_probe_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """
        )
    ).mappings().first()
    if claimed is None:
        db.commit()
        return expired > 0

    destination_id = str(claimed["id"])
    next_attempt = claimed["confirmation_attempt_count"] + 1
    delay = confirmation_backoff_delay(next_attempt)
    db.execute(
        text(
            """
            UPDATE destinations
            SET confirmation_attempt_count = :attempt_count,
                next_probe_at = now() + make_interval(secs => :delay_seconds)
            WHERE id = CAST(:destination_id AS UUID)
            """
        ),
        {
            "attempt_count": next_attempt,
            "delay_seconds": delay,
            "destination_id": destination_id,
        },
    )
    round_no = claimed["confirmation_round"]
    challenge = claimed["challenge_token"]
    url = claimed["url"]
    db.commit()

    result = send_confirmation_probe(
        client,
        url=url,
        destination_id=destination_id,
        challenge=challenge,
        round_no=round_no,
    )

    db2 = SessionLocal()
    try:
        log_confirmation_attempt(
            db2,
            destination_id=destination_id,
            confirmation_round=round_no,
            kind="challenge",
            result="sent" if result["success"] else "failed",
            status_code=result["status_code"],
            response_excerpt=result["response_excerpt"],
            error=result["error"],
        )
        # The receiver answered the probe directly with a matching echo.
        if result["success"] and isinstance(result["echo"], str) and result["echo"]:
            outcome = apply_echo(db2, destination_id, result["echo"])
            if outcome["disposition"] == "confirmed":
                logger.info(
                    "destination_id=%s confirmed by probe echo", destination_id
                )
        db2.commit()
    except SQLAlchemyError:
        db2.rollback()
        logger.exception("failed to record confirmation probe result")
    finally:
        db2.close()
    return True


def confirmer_loop(stop_event: threading.Event) -> None:
    client = httpx.Client(timeout=settings.http_timeout_seconds)
    while not stop_event.is_set():
        db = SessionLocal()
        try:
            did_work = process_confirmation_once(db, client)
        except SQLAlchemyError:
            db.rollback()
            logger.exception("confirmer database error; retrying")
            did_work = False
        except Exception:  # noqa: BLE001 - keep the confirmer alive
            db.rollback()
            logger.exception("unexpected confirmer error")
            did_work = False
        finally:
            db.close()
        if not did_work:
            stop_event.wait(settings.confirm_poll_interval_seconds)
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
    # Exactly one confirmer thread per worker process. Scale worker replicas
    # horizontally and set CONFIRMATION_ENABLED=false on all but one process
    # if duplicate probes to a destination are undesirable (they are harmless:
    # only a correct echo changes state).
    if settings.confirmation_enabled:
        threads.append(
            threading.Thread(
                target=confirmer_loop, args=(stop_event,), name="confirmer"
            )
        )
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
