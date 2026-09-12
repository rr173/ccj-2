"""Preview-consent gate ("预告 + 点头才给正文").

An event type can be marked *gated* (one row in
``event_type_preview_policies``). An event of a gated type does not get one
delivery copy per confirmed subscriber; it gets a pair of copies, queued
back-to-back in that destination's FIFO:

1. ``phase = 'preview'`` — the notice. It carries no real payload, only the
   optional short ``preview_payload``; it is transported and retried like any
   copy but it never enters receipt reconciliation.
2. ``phase = 'body'`` — the actual content. It is created ``pending`` with
   ``release_state = 'held'`` and is *never* claimable by the worker until
   this same destination's own preview has completed transport and that
   destination has nodded yes (``POST /v1/events/{id}/consent``) before the
   agreed deadline.

Guarantees enforced here (see README §1.5):

* the body can never reach an address before its own preview — the body sits
  behind the preview in the same FIFO queue and additionally fails the worker
  claim gate until ``release_state = 'released'``;
* a preview already out is never taken back: denying/voiding only moves the
  not-yet-out body to a terminal release state;
* "no" (``deny``) ends the body for that address for good — a later nod never
  revives it;
* no nod before the deadline voids the body as ``release_expired``; the gate
  stays answerable as "this address did not nod", and a nod arriving late is
  recorded ``late_ignored`` without reviving the body — the body is never
  written as delivered;
* a decision is per (event, destination): another destination's nod can never
  release this one — the lookup keys on both;
* manually voiding a still-queued body touches nothing else (the preview,
  already delivered or already released elsewhere, keeps its state);
* a body already handed off (in flight / delivered) cannot be voided;
* destinations not subscribed to the type get neither preview nor body —
  fan-out only sees current confirmed subscribers, exactly like ordinary
  events;
* the same destination nodding the same preview twice counts once — the
  effective answer has a unique constraint on the body and the second answer
  is recorded ``duplicate``.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("release-gate")

# A gated preview's business key derives from the event's key. It must never
# collide with the body key (the receipts/deliveries uniqueness is per
# destination+key) and must stay within the dedupe_key length budget.
PREVIEW_KEY_PREFIX = "prev:"
# release-gate state names reused by the API/worker/reconciler.
HELD = "held"
RELEASED = "released"
DENIED = "release_denied"
EXPIRED = "release_expired"
VOIDED = "release_voided"
DEADLINE_EXPIRED = "deadline_expired"
TERMINAL_RELEASE_STATES = (DENIED, EXPIRED, VOIDED, DEADLINE_EXPIRED)

# Delivery statuses a gated body ends in without ever going out.
BODY_VOID_STATUSES = (
    "release_denied",
    "release_expired",
    "release_voided",
    "deadline_expired",
)

# Policy table column list.
POLICY_COLUMNS = "event_type, consent_timeout_seconds, created_at, updated_at"


class ConsentError(Exception):
    """A consent answer that cannot be applied (unknown event/destination)."""

    def __init__(self, status_code: int, reason: str):
        super().__init__(reason)
        self.status_code = status_code
        self.reason = reason


def preview_key(event_dedupe_key: str) -> str:
    return f"{PREVIEW_KEY_PREFIX}{event_dedupe_key}"


def get_policy(db: Session, event_type: str) -> dict[str, Any] | None:
    row = db.execute(
        text(
            f"""
            SELECT {POLICY_COLUMNS}
            FROM event_type_preview_policies
            WHERE event_type = :event_type
            """
        ),
        {"event_type": event_type},
    ).mappings().first()
    return dict(row) if row is not None else None


def upsert_policy(
    db: Session, event_type: str, consent_timeout_seconds: int
) -> dict[str, Any]:
    return dict(
        db.execute(
            text(
                f"""
                INSERT INTO event_type_preview_policies
                    (event_type, consent_timeout_seconds)
                VALUES (:event_type, :consent_timeout_seconds)
                ON CONFLICT (event_type) DO UPDATE
                    SET consent_timeout_seconds = EXCLUDED.consent_timeout_seconds,
                        updated_at = now()
                RETURNING {POLICY_COLUMNS}
                """
            ),
            {
                "event_type": event_type,
                "consent_timeout_seconds": consent_timeout_seconds,
            },
        ).mappings().one()
    )


def delete_policy(db: Session, event_type: str) -> None:
    db.execute(
        text(
            "DELETE FROM event_type_preview_policies WHERE event_type = :event_type"
        ),
        {"event_type": event_type},
    )


def fan_out_gated_event(
    db: Session,
    *,
    event_id: str,
    event_type: str,
    dedupe_key: str,
    payload: str,
    preview_payload: str | None,
    not_before: Any,
    deliver_by: Any,
    consent_timeout_seconds: int,
    observe_only: bool | None = None,
    destinations: list | None = None,
    filter_specs: dict[str, Any] | None = None,
) -> dict[str, int]:
    """Create the preview/body pair for every confirmed subscriber (or the
    explicit ``destinations`` list used by ingest/corrections) whose own
    subscription condition held.

    Each destination's FIFO gets two consecutive sequence numbers; the pair
    is created inside the same per-destination row bump so a concurrent
    fan-out cannot interleave another event between preview and body. The
    subscription condition is snapshotted onto both copies (``filter_specs``
    maps destination id -> JSON text; a withheld destination is simply not in
    the passed ``destinations`` list). Returns the number of for-real /
    shadow pairs created.
    """
    if destinations is None:
        destination_rows = db.execute(
            text(
                """
                SELECT s.destination_id, d.observe_only
                FROM destination_subscriptions s
                JOIN destinations d ON d.id = s.destination_id
                WHERE s.event_type = :event_type
                  AND d.confirmation_state = 'confirmed'
                ORDER BY s.destination_id
                FOR UPDATE OF d
                """
            ),
            {"event_type": event_type},
        ).all()
    else:
        destination_rows = [
            (row[0], row[1]) for row in destinations
        ]
    filter_specs = filter_specs or {}

    real_pairs = 0
    shadow_pairs = 0
    prev_key = preview_key(dedupe_key)
    for destination_id, is_shadow in destination_rows:
        filter_spec = filter_specs.get(str(destination_id))
        filter_param = (
            json.dumps(filter_spec) if filter_spec is not None else None
        )
        # One destination lock + two seq bumps in a single UPDATE ... RETURNING
        # chain would be ideal; two locked updates keep the code aligned with
        # the existing fan-out discipline (lock destination in id order, bump
        # next_event_seq, insert).
        db.execute(
            text(
                """
                SELECT id FROM destinations
                WHERE id = CAST(:destination_id AS UUID)
                FOR UPDATE
                """
            ),
            {"destination_id": str(destination_id)},
        )
        # Preview (seq N). It is inserted first so the body can point at it;
        # its body_delivery_id placeholder is filled once the body exists.
        preview_row = db.execute(
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
                     destination_seq, not_before, deliver_by, confirmation_generation,
                     observe_only, filter_spec, phase, release_state,
                     consent_timeout_seconds, body_delivery_id)
                SELECT :event_id, id, :event_type, :preview_key,
                       CAST(:preview_payload AS JSONB), next_event_seq,
                       CAST(:not_before AS TIMESTAMPTZ),
                       CAST(:deliver_by AS TIMESTAMPTZ), confirmation_generation,
                       :observe_only, CAST(:filter_spec AS JSONB), 'preview', NULL,
                       :consent_timeout_seconds,
                       CAST(:placeholder AS UUID)
                FROM bumped
                RETURNING id
                """
            ),
            {
                "event_id": event_id,
                "destination_id": str(destination_id),
                "event_type": event_type,
                "preview_key": prev_key,
                "preview_payload": preview_payload or json.dumps({}),
                "not_before": not_before,
                "deliver_by": deliver_by,
                "observe_only": is_shadow,
                "filter_spec": filter_param,
                "consent_timeout_seconds": consent_timeout_seconds,
                "placeholder": "00000000-0000-0000-0000-000000000000",
            },
        ).mappings().one()
        preview_id = str(preview_row["id"])
        # Body (seq N+1), held behind the preview. It points at its preview via
        # preview_delivery_id. The deadline is null until the preview really
        # completes transport — the countdown only starts from that moment,
        # never from ingest/queue time.
        body_row = db.execute(
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
                     destination_seq, not_before, deliver_by, confirmation_generation,
                     observe_only, filter_spec, phase, release_state,
                     consent_timeout_seconds, preview_delivery_id)
                SELECT :event_id, id, :event_type, :dedupe_key,
                       CAST(:payload AS JSONB), next_event_seq,
                       CAST(:not_before AS TIMESTAMPTZ),
                       CAST(:deliver_by AS TIMESTAMPTZ), confirmation_generation,
                       :observe_only, CAST(:filter_spec AS JSONB), 'body', 'held',
                       :consent_timeout_seconds, CAST(:preview_id AS UUID)
                FROM bumped
                RETURNING id
                """
            ),
            {
                "event_id": event_id,
                "destination_id": str(destination_id),
                "event_type": event_type,
                "dedupe_key": dedupe_key,
                "payload": payload,
                "not_before": not_before,
                "deliver_by": deliver_by,
                "observe_only": is_shadow,
                "filter_spec": filter_param,
                "consent_timeout_seconds": consent_timeout_seconds,
                "preview_id": preview_id,
            },
        ).mappings().one()
        body_id = str(body_row["id"])
        # Back-link the preview to its body; the pair is now complete.
        db.execute(
            text(
                """
                UPDATE deliveries SET body_delivery_id = CAST(:body_id AS UUID)
                WHERE id = CAST(:preview_id AS UUID)
                """
            ),
            {"body_id": body_id, "preview_id": preview_id},
        )
        if is_shadow:
            shadow_pairs += 1
        else:
            real_pairs += 1
    return {"real_pairs": real_pairs, "shadow_pairs": shadow_pairs}


# -- Preview send outcomes (called by the worker) -----------------------------


def mark_preview_delivered(
    db: Session, delivery_id: str, finished_at: Any | None = None
) -> bool:
    """A preview completed transport before its cutoff.

    Unlike an ordinary copy it does not enter receipt reconciliation; instead
    the consent deadline is opened on its body (delivered-at + the timeout
    snapshotted on the pair). The body's queue position never changes. Returns
    False when transport completed after the latest-delivery cutoff: the
    preview and its body are closed instead, never marked delivered.
    """
    expired = db.execute(
        text(
            """
            UPDATE deliveries
            SET status = 'deadline_expired',
                deliver_by_expired_at = COALESCE(deliver_by_expired_at, now()),
                claim_token = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                next_attempt_at = now(),
                updated_at = now()
            WHERE id = CAST(:delivery_id AS UUID)
              AND status = 'in_flight'
              AND deliver_by IS NOT NULL
              AND deliver_by <= COALESCE(CAST(:finished_at AS TIMESTAMPTZ), now())
            RETURNING body_delivery_id
            """
        ),
        {
            "delivery_id": delivery_id,
            "finished_at": finished_at,
        },
    ).mappings().first()
    if expired is not None:
        # The notice only completed after the cutoff and is therefore not a
        # delivered preview. Close its body as a missed deadline too.
        if expired["body_delivery_id"] is not None:
            db.execute(
                text(
                    """
                    UPDATE deliveries
                    SET status = 'deadline_expired',
                        release_state = 'deadline_expired',
                        voided_at = now(),
                        void_reason = 'preview_deadline_expired',
                        claim_token = NULL,
                        claimed_at = NULL,
                        lease_until = NULL,
                        next_attempt_at = now(),
                        updated_at = now()
                    WHERE id = CAST(:body_id AS UUID)
                      AND status = 'pending'
                      AND release_state IN ('held', 'released')
                    """
                ),
                {"body_id": str(expired["body_delivery_id"])},
            )
        return False

    delivered = db.execute(
        text(
            """
            UPDATE deliveries
            SET status = 'delivered',
                claim_token = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                last_error = NULL,
                updated_at = now(),
                delivered_at = now(),
                consecutive_failures = 0
            WHERE id = CAST(:delivery_id AS UUID)
              AND status = 'in_flight'
              AND (
                    deliver_by IS NULL
                 OR deliver_by > COALESCE(CAST(:finished_at AS TIMESTAMPTZ), now())
              )
            """
        ),
        {
            "delivery_id": delivery_id,
            "finished_at": finished_at,
        },
    )
    if delivered.rowcount != 1:
        # Lost the claim while the HTTP call was running; the worker owning
        # the result records the attempt elsewhere. Do not open a consent
        # window or reset the destination tally for a copy we did not update.
        return False

    # Open the decision window on the still-held body. If it is already
    # terminal (voided manually / superseded) nothing is opened.
    db.execute(
        text(
            """
            UPDATE deliveries
            SET consent_deadline =
                    now() + (consent_timeout_seconds * INTERVAL '1 second'),
                updated_at = now()
            WHERE id = (
                    SELECT body_delivery_id FROM deliveries
                    WHERE id = CAST(:preview_id AS UUID)
                )
              AND phase = 'body'
              AND status = 'pending'
              AND release_state = 'held'
            """
        ),
        {"preview_id": delivery_id},
    )
    db.execute(
        text(
            "UPDATE destinations SET failure_count = 0 "
            "WHERE id = (SELECT destination_id FROM deliveries "
            "            WHERE id = CAST(:delivery_id AS UUID))"
        ),
        {"delivery_id": delivery_id},
    )
    return True


def void_body_after_preview_failure(
    db: Session,
    *,
    preview_id: str,
    reason: str,
) -> None:
    """A preview that will never make it through parks its body too.

    Used when the preview dead-letters (transport attempts exhausted) or is
    superseded by a destination relocation. The body never went out and never
    will; it is moved to a terminal release state carrying the reason, so the
    gate query can answer "this address never received the body" truthfully.
    """
    _void_held_bodies(
        db,
        """
            phase = 'body'
            AND preview_delivery_id = CAST(:preview_id AS UUID)
        """,
        {"preview_id": preview_id},
        reason,
    )


def relocate_held_bodies(db: Session, destination_id: str) -> None:
    """Close every still-held gated body of an address that is relocating.

    Run before the generic old-generation supersede in ``arm_round``: a held
    body queued behind a preview that the destination change makes undeliverable
    must end in the gate's own terminal state (release_voided) rather than a
    plain 'superseded' row that still reads release_state='held'. Bodies
    already released and still queued are left for the generic supersede.
    """
    _void_held_bodies(
        db,
        """
            phase = 'body'
            AND destination_id = CAST(:destination_id AS UUID)
            AND confirmation_generation < (
                SELECT confirmation_generation FROM destinations
                WHERE id = CAST(:destination_id AS UUID)
            )
        """,
        {"destination_id": destination_id},
        "preview_superseded",
    )


def _void_held_bodies(db: Session, where_sql: str, params: dict, reason: str) -> None:
    db.execute(
        text(
            f"""
            UPDATE deliveries
            SET release_state = :release_state,
                status = :status,
                void_reason = :reason,
                voided_at = now(),
                claim_token = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                updated_at = now()
            WHERE status = 'pending'
              AND release_state = 'held'
              AND {where_sql}
            """
        ),
        {
            "release_state": VOIDED,
            "status": "release_voided",
            "reason": reason,
            **params,
        },
    )


# -- Consent decisions ("点头" / "不要") ---------------------------------------


def ingest_decision(
    db: Session,
    *,
    destination_id: str,
    event_id: str,
    decision: str,
) -> dict[str, Any]:
    """Apply one preview decision, atomically and idempotently.

    The body row is the gate: it is keyed by (event, destination) and locked
    FOR UPDATE for the whole decision, so concurrent or repeated answers from
    the same address serialize and exactly one effective answer exists
    (unique release_gate_decisions.delivery_id). Another address's answer can
    never match this gate because the lookup includes destination_id.
    """
    body = db.execute(
        text(
            """
            SELECT id, status, release_state, phase, delivered_at,
                   consent_deadline, preview_delivery_id
            FROM deliveries
            WHERE event_id = CAST(:event_id AS UUID)
              AND destination_id = CAST(:destination_id AS UUID)
              AND phase = 'body'
            FOR UPDATE
            """
        ),
        {"event_id": event_id, "destination_id": destination_id},
    ).mappings().first()

    disposition: str
    reason: str | None = None
    applied_delivery_id: str | None = None
    release_state_after: str | None = body["release_state"] if body else None

    def record() -> None:
        db.execute(
            text(
                """
                INSERT INTO release_gate_decisions
                    (destination_id, event_id, delivery_id, decision,
                     disposition, reason)
                VALUES (
                    CAST(:destination_id AS UUID),
                    CAST(:event_id AS UUID),
                    CAST(:delivery_id AS UUID),
                    :decision, :disposition, :reason
                )
                """
            ),
            {
                "destination_id": destination_id,
                "event_id": event_id,
                "delivery_id": applied_delivery_id,
                "decision": decision,
                "disposition": disposition,
                "reason": reason,
            },
        )

    if body is None:
        # No gate for this (event, address): the address isn't a confirmed
        # subscriber of this gated event (or the event doesn't exist/is not
        # gated). Nothing is released; keep the answer visible as an orphan.
        event_exists = db.execute(
            text(
                "SELECT 1 FROM events WHERE id = CAST(:event_id AS UUID)"
            ),
            {"event_id": event_id},
        ).first()
        if event_exists is None:
            raise ConsentError(404, "event not found")
        disposition = "orphan"
        reason = "no gated body for this (event, destination)"
        record()
        db.commit()
        return {
            "disposition": disposition,
            "release_state": None,
            "event_id": event_id,
            "destination_id": destination_id,
        }

    body_id = str(body["id"])
    state = body["release_state"]

    # Only the single effective answer (released / denied) carries the body id
    # — release_gate_decisions.delivery_id is unique. Every later audit row
    # (duplicate / conflict / late_ignored / preview_not_delivered) leaves it
    # null; it still links to the gate via (event_id, destination_id).
    if state in (RELEASED, DENIED) or state in TERMINAL_RELEASE_STATES:
        applied_delivery_id = None

    if state == RELEASED:
        # A body already released by this address's earlier nod: a second nod
        # counts once. An opposite answer after release is a conflict and
        # changes nothing either.
        disposition = "duplicate" if decision == "approve" else "conflict"
        record()
        db.commit()
        return _decision_response(body_id, disposition, state)

    if state == DENIED:
        disposition = "duplicate" if decision == "deny" else "conflict"
        record()
        db.commit()
        return _decision_response(body_id, disposition, state)

    if state in (EXPIRED, VOIDED, DEADLINE_EXPIRED):
        # Body already gone (consent cutoff, manual/cascading void, or the
        # event's deliver-by cutoff): neither a late nod nor a late "no"
        # revives or moves it.
        disposition = "late_ignored"
        reason = f"body already {state}"
        record()
        db.commit()
        return _decision_response(body_id, disposition, state)

    # release_state = 'held'.
    if body["status"] != "pending":
        # In flight or already handed off cannot be re-gated here; the normal
        # path would have released it first.
        disposition = "late_ignored"
        reason = f"body already {body['status']}"
        # Keep the decision audit row but do not bind the unique effective
        # slot to a non-decided body.
        applied_delivery_id = None
        record()
        db.commit()
        return _decision_response(body_id, disposition, state)

    preview = db.execute(
        text(
            """
            SELECT delivered_at, status
            FROM deliveries
            WHERE id = CAST(:preview_id AS UUID)
            FOR UPDATE
            """
        ),
        {"preview_id": str(body["preview_delivery_id"])},
    ).mappings().one()

    if preview["delivered_at"] is None:
        # The notice never reached this address (still queued / dead-lettered):
        # there is nothing for it to answer. The body stays held; record and
        # wait — answering before delivery must not release content.
        disposition = "preview_not_delivered"
        reason = f"preview status={preview['status']}"
        applied_delivery_id = None
        record()
        db.commit()
        return _decision_response(body_id, disposition, state)

    now_row = db.execute(text("SELECT now() AS now")).mappings().one()
    now = now_row["now"]
    deadline = body["consent_deadline"]
    if deadline is None or now > deadline:
        # Deadline is authoritative even if the sweeper has not run yet:
        # close the body as expired and judge the answer late.
        db.execute(
            text(
                """
                UPDATE deliveries
                SET release_state = :expired,
                    status = 'release_expired',
                    voided_at = now(),
                    updated_at = now()
                WHERE id = CAST(:body_id AS UUID)
                """
            ),
            {"expired": EXPIRED, "body_id": body_id},
        )
        disposition = "late_ignored"
        reason = "consent deadline already passed"
        # Late answers never occupy the unique effective-answer slot.
        applied_delivery_id = None
        record()
        db.commit()
        return _decision_response(body_id, disposition, EXPIRED)

    if decision == "deny":
        db.execute(
            text(
                """
                UPDATE deliveries
                SET release_state = :denied,
                    status = 'release_denied',
                    voided_at = now(),
                    updated_at = now()
                WHERE id = CAST(:body_id AS UUID)
                """
            ),
            {"denied": DENIED, "body_id": body_id},
        )
        disposition = "denied"
        release_state_after = DENIED
    else:
        db.execute(
            text(
                """
                UPDATE deliveries
                SET release_state = :released,
                    released_at = now(),
                    updated_at = now()
                WHERE id = CAST(:body_id AS UUID)
                """
            ),
            {"released": RELEASED, "body_id": body_id},
        )
        disposition = "released"
        release_state_after = RELEASED

    # Exactly one effective answer per gate: this row occupies the unique
    # delivery_id slot, so a concurrent/duplicate answer cannot also apply.
    applied_delivery_id = body_id
    record()
    db.commit()
    return _decision_response(body_id, disposition, release_state_after)


def _decision_response(
    body_id: str, disposition: str, release_state: str | None
) -> dict[str, Any]:
    return {
        "delivery_id": body_id,
        "disposition": disposition,
        "release_state": release_state,
    }


# -- Expiry sweep (reconciler process) ----------------------------------------


def sweep_expired_gates(db: Session, *, limit: int = 500) -> int:
    """Void held bodies whose preview was delivered and whose consent deadline
    passed without an effective answer. Returns the number voided.

    Bodies whose preview never completed transport (deadline still null) are
    not touched here — they are still waiting for the notice, not for the nod.
    """
    row = db.execute(
        text(
            """
            WITH due AS (
                SELECT id
                FROM deliveries
                WHERE phase = 'body'
                  AND status = 'pending'
                  AND release_state = 'held'
                  AND consent_deadline IS NOT NULL
                  AND consent_deadline < now()
                ORDER BY consent_deadline
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
            ), swept AS (
                UPDATE deliveries d
                SET release_state = :expired,
                    status = 'release_expired',
                    voided_at = now(),
                    updated_at = now()
                FROM due
                WHERE d.id = due.id
                RETURNING d.id
            )
            SELECT count(*)::int AS count FROM swept
            """
        ),
        {"limit": limit, "expired": EXPIRED},
    ).mappings().one()
    return row["count"]


# -- Manual void of a not-yet-out gated body ----------------------------------


def void_gated_body(db: Session, delivery_id: str) -> dict[str, Any]:
    body = db.execute(
        text(
            """
            SELECT id, status, phase, release_state, event_id, destination_id
            FROM deliveries
            WHERE id = CAST(:delivery_id AS UUID)
            FOR UPDATE
            """
        ),
        {"delivery_id": delivery_id},
    ).mappings().first()
    if body is None:
        db.rollback()
        raise ConsentError(404, "delivery not found")
    if body["phase"] != "body" or body["release_state"] is None:
        db.rollback()
        raise ConsentError(
            409,
            "delivery is not a gated body waiting on a preview decision "
            f"(phase={body['phase']}, release_state={body['release_state']})",
        )
    if body["status"] in ("in_flight", "delivered"):
        # What already went out the door cannot be taken back.
        db.rollback()
        raise ConsentError(
            409,
            f"body has already gone out (status={body['status']}) and can no "
            "longer be voided",
        )
    if body["status"] in BODY_VOID_STATUSES:
        # Idempotent: an already-closed body stays closed.
        db.rollback()
        return {
            "delivery_id": str(body["id"]),
            "voided": False,
            "status": body["status"],
            "release_state": body["release_state"],
        }

    db.execute(
        text(
            """
            UPDATE deliveries
            SET release_state = :voided,
                status = 'release_voided',
                void_reason = 'manual',
                voided_at = now(),
                updated_at = now()
            WHERE id = CAST(:delivery_id AS UUID)
            """
        ),
        {"voided": VOIDED, "delivery_id": delivery_id},
    )
    db.commit()
    return {
        "delivery_id": str(body["id"]),
        "voided": True,
        "status": "release_voided",
        "release_state": VOIDED,
    }
