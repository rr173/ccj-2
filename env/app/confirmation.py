"""Destination activation handshake: challenge generation, echo matching,
round expiry and the audit trail.

A destination must prove once that it is live at its registered URL before
any subscribed event is fanned out to it. The handshake is challenge/response:

1. Registration (and a URL change) *arms* a round: a random challenge token is
   stored together with a deadline (``CONFIRM_TIMEOUT_SECONDS``).
2. The confirmer thread in the worker process probes the URL, carrying the
   challenge.
3. The receiver answers in either of two ways:
   - returns HTTP 2xx with ``{"echo": "<challenge>"}`` on the probe itself; or
   - calls ``POST /v1/destinations/{id}/confirm`` with the challenge.
4. A correct echo before the deadline moves the destination to ``confirmed``;
   a wrong echo is logged as ``invalid`` and changes nothing; an unanswered
   round expires, is logged, and a brand-new round is armed.

While ``pending``, events are ingested and stored exactly as usual but no
delivery row is created for that destination, so nothing can be reported as
sent to it and nothing old can be backfilled after confirmation — only events
ingested later fan out. A URL change additionally bumps the confirmation
generation; queued copies of the old generation are marked ``superseded``.
"""

import secrets
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import settings


def new_challenge_token() -> str:
    return secrets.token_urlsafe(32)


INSERT_CONFIRMATION_ATTEMPT_SQL = text(
    """
    INSERT INTO confirmation_attempts
        (destination_id, confirmation_round, kind, result, status_code,
         response_excerpt, error)
    VALUES (
        CAST(:destination_id AS UUID), :confirmation_round, :kind, :result,
        :status_code, :response_excerpt, :error
    )
    """
)

# Columns returned whenever a destination's handshake state is selected.
DESTINATION_CONFIRM_COLUMNS = (
    "id, url, status, failure_count, recoverable_at, created_at, "
    "confirmation_state, challenge_token, challenge_expires_at, confirmed_at, "
    "confirmation_generation, confirmation_round, confirmation_attempt_count, "
    "next_probe_at"
)


def log_confirmation_attempt(
    db: Session,
    *,
    destination_id: str,
    confirmation_round: int,
    kind: str,
    result: str,
    status_code: int | None = None,
    response_excerpt: str | None = None,
    error: str | None = None,
) -> None:
    db.execute(
        INSERT_CONFIRMATION_ATTEMPT_SQL,
        {
            "destination_id": destination_id,
            "confirmation_round": confirmation_round,
            "kind": kind,
            "result": result,
            "status_code": status_code,
            "response_excerpt": response_excerpt,
            "error": error,
        },
    )


def arm_round(
    db: Session,
    destination_id: str,
    *,
    bump_generation: bool,
) -> dict[str, Any] | None:
    """(Re)arm a confirmation round with a fresh challenge.

    ``bump_generation`` is true when the destination moved to a new URL, which
    also invalidates every still-queued copy of the previous location by
    moving it to the terminal ``superseded`` state. Copies already delivered
    or in flight are deliberately left alone: what was already sent out is not
    taken back.
    """
    result = db.execute(
        text(
            f"""
            UPDATE destinations
            SET confirmation_state = 'pending',
                challenge_token = :challenge,
                challenge_expires_at =
                    now() + make_interval(secs => :timeout_seconds),
                confirmed_at = NULL,
                confirmation_round = confirmation_round + 1,
                confirmation_attempt_count = 0,
                next_probe_at = now(),
                confirmation_generation = confirmation_generation
                    + CASE WHEN :bump_generation THEN 1 ELSE 0 END
            WHERE id = CAST(:destination_id AS UUID)
            RETURNING {DESTINATION_CONFIRM_COLUMNS}
            """
        ),
        {
            "destination_id": destination_id,
            "challenge": new_challenge_token(),
            "timeout_seconds": settings.confirm_timeout_seconds,
            "bump_generation": bump_generation,
        },
    ).mappings().first()
    if result is None or not bump_generation:
        return dict(result) if result is not None else None

    # Old location: abandon queued copies. Pending copies never reach the wire;
    # an in-flight copy is left to finish its current HTTP call (already sent,
    # not recalled) but its generation now mismatches, so the worker neither
    # retries it on failure nor blocks the new queue on it for long.
    db.execute(
        text(
            """
            UPDATE deliveries
            SET status = 'superseded',
                updated_at = now()
            WHERE destination_id = CAST(:destination_id AS UUID)
              AND confirmation_generation < (
                  SELECT confirmation_generation FROM destinations
                  WHERE id = CAST(:destination_id AS UUID)
              )
              AND status = 'pending'
            """
        ),
        {"destination_id": destination_id},
    )
    return dict(result)


def apply_echo(db: Session, destination_id: str, challenge: str) -> dict[str, Any]:
    """Match one echo against the destination's current round.

    Returns ``{"disposition", "destination"}``. The destination row is locked
    for the whole decision, so concurrent/duplicate echoes serialize: exactly
    one transition to confirmed can happen. Expired rounds are rotated here
    using the same lock.
    """
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
        return {"disposition": "not_found", "destination": None}

    destination = dict(destination)
    round_no = destination["confirmation_round"]

    if destination["confirmation_state"] == "confirmed":
        # Idempotent: an already-confirmed destination stays confirmed; the
        # repeated answer is still recorded in the handshake log.
        log_confirmation_attempt(
            db,
            destination_id=destination_id,
            kind="echo",
            result="confirmed",
            confirmation_round=round_no,
        )
        return {"disposition": "already_confirmed", "destination": destination}

    if destination["challenge_expires_at"] is not None:
        now = db.execute(text("SELECT now() AS now")).mappings().one()["now"]
        expired = now > destination["challenge_expires_at"]
    else:
        expired = False
    if expired:
        # The deadline passed before this answer arrived. Rotate the round:
        # the old event is never backfilled, only later events fan out.
        log_confirmation_attempt(
            db,
            destination_id=destination_id,
            confirmation_round=round_no,
            kind="expired",
            result="expired",
        )
        destination = arm_round(db, destination_id, bump_generation=False)
        log_confirmation_attempt(
            db,
            destination_id=destination_id,
            kind="echo",
            result="invalid",
            confirmation_round=destination["confirmation_round"],
            error="echo arrived after the round expired; new round issued",
        )
        return {"disposition": "expired", "destination": destination}

    if secrets.compare_digest(challenge, destination["challenge_token"] or ""):
        confirmed = db.execute(
            text(
                f"""
                UPDATE destinations
                SET confirmation_state = 'confirmed',
                    confirmed_at = now(),
                    challenge_token = NULL,
                    challenge_expires_at = NULL,
                    next_probe_at = now()
                WHERE id = CAST(:destination_id AS UUID)
                RETURNING {DESTINATION_CONFIRM_COLUMNS}
                """
            ),
            {"destination_id": destination_id},
        ).mappings().one()
        log_confirmation_attempt(
            db,
            destination_id=destination_id,
            kind="echo",
            result="confirmed",
            confirmation_round=round_no,
        )
        return {"disposition": "confirmed", "destination": dict(confirmed)}

    # Wrong answer: the round stays open; nothing is confirmed or re-armed.
    log_confirmation_attempt(
        db,
        destination_id=destination_id,
        kind="echo",
        result="invalid",
        confirmation_round=round_no,
        error="echo did not match the outstanding challenge",
    )
    return {"disposition": "invalid", "destination": destination}


def expire_due_rounds(db: Session, *, limit: int = 50) -> int:
    """Rotate every pending round whose deadline passed. Returns the count."""
    rows = db.execute(
        text(
            """
            SELECT id, confirmation_round
            FROM destinations
            WHERE confirmation_state = 'pending'
              AND challenge_expires_at < now()
            ORDER BY challenge_expires_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """
        ),
        {"limit": limit},
    ).mappings().all()
    for row in rows:
        destination_id = str(row["id"])
        log_confirmation_attempt(
            db,
            destination_id=destination_id,
            confirmation_round=row["confirmation_round"],
            kind="expired",
            result="expired",
        )
        arm_round(db, destination_id, bump_generation=False)
    return len(rows)
