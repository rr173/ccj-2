"""Event-wide latest-delivery promise ("最晚送到").

A submitted event may carry ``deliver_by``: the latest time its copies may be
handed to a receiving address. The timestamp is snapshotted onto each copy at
fan-out and changed later only on copies that have not completed transport.

A copy whose cutoff passes while it is still queued becomes the terminal
``deadline_expired`` state. It is never sent by the worker and must not be
reported as delivered. Already-delivered copies are never recalled and are not
rewritten by a later deadline change. Events/copies without ``deliver_by``
retain their ordinary lifecycle.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

DEADLINE_EXPIRED = "deadline_expired"


def update_event_deadline(
    db: Session, event_id: str, deliver_by: Any
) -> dict[str, int]:
    """Set/clear an event's cutoff and snapshot it onto undelivered copies.

    The event row keeps the current promise for audit. Only copies that are
    queued or in flight are updated; delivered and already-terminal copies are
    not rewritten. A clear (``deliver_by=None``) means "send normally again" for the
    copies that remain undelivered.
    """
    updated = db.execute(
        text(
            """
            WITH event_lock AS (
                SELECT id FROM events
                WHERE id = CAST(:event_id AS UUID)
                FOR UPDATE
            ), event_update AS (
                UPDATE events e
                SET deliver_by = CAST(:deliver_by AS TIMESTAMPTZ)
                FROM event_lock l
                WHERE e.id = l.id
                RETURNING e.id
            ), delivery_update AS (
                UPDATE deliveries d
                SET deliver_by = CAST(:deliver_by AS TIMESTAMPTZ),
                    updated_at = now()
                FROM event_lock l
                WHERE d.event_id = l.id
                  AND d.status IN ('pending', 'in_flight')
                  AND d.delivered_at IS NULL
                RETURNING d.id, d.observe_only
            )
            SELECT
                COUNT(*)::int AS updated_count,
                COUNT(*) FILTER (WHERE NOT observe_only)::int AS real_count,
                COUNT(*) FILTER (WHERE observe_only)::int AS shadow_count
            FROM delivery_update
            """
        ),
        {"event_id": event_id, "deliver_by": deliver_by},
    ).mappings().one()
    return dict(updated)


def expire_due_deliveries(db: Session, *, limit: int = 500) -> list[str]:
    """Close pending copies whose deliver_by cutoff has passed.

    Returns relay delivery ids that expired while open (earlier stations whose
    later tail must be skipped). When a gated preview misses the cutoff, its
    still-held body is also closed (the notice never arrived, so the content must not go out); when an open relay station
    misses it, its later stations are skipped by the caller.
    """
    rows = db.execute(
        text(
            """
            WITH due AS (
                SELECT id, event_id, destination_id, phase, release_state,
                       relay_chain_id, relay_station_no, body_delivery_id
                FROM deliveries
                WHERE status = 'pending'
                  AND deliver_by IS NOT NULL
                  AND deliver_by < now()
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
                ORDER BY deliver_by, id
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
            ), expired AS (
                UPDATE deliveries d
                SET status = :expired_status,
                    deliver_by_expired_at = COALESCE(deliver_by_expired_at, now()),
                    release_state = CASE
                        WHEN d.phase = 'body'
                             AND d.release_state IN ('held', 'released')
                        THEN :expired_status
                        ELSE d.release_state
                    END,
                    voided_at = CASE
                        WHEN d.phase = 'body'
                             AND d.release_state IN ('held', 'released')
                        THEN now()
                        ELSE d.voided_at
                    END,
                    void_reason = CASE
                        WHEN d.phase = 'body'
                             AND d.release_state IN ('held', 'released')
                        THEN 'deliver_by_expired'
                        ELSE d.void_reason
                    END,
                    claim_token = NULL,
                    claimed_at = NULL,
                    lease_until = NULL,
                    next_attempt_at = now(),
                    updated_at = now()
                FROM due
                WHERE d.id = due.id
                RETURNING d.id, d.relay_chain_id, d.relay_station_no,
                          d.phase, d.body_delivery_id
            ), bodies AS (
                UPDATE deliveries body
                SET status = :expired_status,
                    release_state = :expired_status,
                    voided_at = now(),
                    void_reason = 'preview_deadline_expired',
                    claim_token = NULL,
                    claimed_at = NULL,
                    lease_until = NULL,
                    next_attempt_at = now(),
                    updated_at = now()
                FROM expired preview
                WHERE body.id = preview.body_delivery_id
                  AND preview.phase = 'preview'
                  AND body.status = 'pending'
                  AND body.release_state IN ('held', 'released')
                RETURNING body.id
            )
            SELECT id, relay_chain_id
            FROM expired
            WHERE relay_chain_id IS NOT NULL
            """
        ),
        {"limit": limit, "expired_status": DEADLINE_EXPIRED},
    ).mappings().all()
    return [str(row["id"]) for row in rows]


def expire_in_flight_delivery(
    db: Session, delivery_id: str, finished_at: Any | None = None
) -> bool:
    """Close an in-flight copy only when its cutoff has now passed.

    ``finished_at`` defaults to the database clock but callers pass the HTTP
    response's finish time so a quick result-write delay cannot make a timely
    delivery look late. Returns True when the copy was closed; in that case it
    is not marked delivered, even if HTTP returned 2xx.
    """
    result = db.execute(
        text(
            """
            UPDATE deliveries
            SET status = :expired_status,
                deliver_by_expired_at = COALESCE(deliver_by_expired_at, now()),
                release_state = CASE
                    WHEN phase = 'body'
                         AND release_state IN ('held', 'released')
                    THEN :expired_status
                    ELSE release_state
                END,
                voided_at = CASE
                    WHEN phase = 'body'
                         AND release_state IN ('held', 'released')
                    THEN now()
                    ELSE voided_at
                END,
                void_reason = CASE
                    WHEN phase = 'body'
                         AND release_state IN ('held', 'released')
                    THEN 'deliver_by_expired'
                    ELSE void_reason
                END,
                claim_token = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                next_attempt_at = now(),
                updated_at = now()
            WHERE id = CAST(:delivery_id AS UUID)
              AND status = 'in_flight'
              AND deliver_by IS NOT NULL
              AND deliver_by <= COALESCE(CAST(:finished_at AS TIMESTAMPTZ), now())
            RETURNING id, phase, body_delivery_id
            """
        ),
        {
            "delivery_id": delivery_id,
            "expired_status": DEADLINE_EXPIRED,
            "finished_at": finished_at,
        },
    ).mappings().first()
    if result is None:
        return False

    if result["phase"] == "preview" and result["body_delivery_id"] is not None:
        db.execute(
            text(
                """
                UPDATE deliveries
                SET status = :expired_status,
                    release_state = :expired_status,
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
            {
                "body_id": str(result["body_delivery_id"]),
                "expired_status": DEADLINE_EXPIRED,
            },
        )
    return True
