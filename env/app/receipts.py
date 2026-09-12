"""Receipt ingestion and reconciliation matching.

Receivers call back with the dedupe key they were given plus whether their
processing succeeded. A receipt only "counts" when it matches a delivery that
is still inside its reconciliation window; everything else is recorded with an
explicit disposition so nothing is silently treated as acknowledged:

- applied    — matched a delivery still awaiting its receipt; the delivery is
               now acknowledged (result=success) or receipt_failed (result=failure)
- duplicate  — the delivery was already reconciled; the same receipt is only
               ever applied once
- late       — the reconciliation deadline had already passed (the delivery is
               marked/kept timed_out and is NOT flipped to acknowledged)
- orphan     — no delivery exists for (destination_id, dedupe_key)
- premature  — the delivery exists but has not completed transport yet
"""

import logging
import time
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import settings
from app import relay

logger = logging.getLogger("receipts")

LOCK_DELIVERY_SQL = text(
    """
    SELECT id, status, reconcile_state, requeue_count, relay_chain_id,
           (reconcile_deadline IS NOT NULL AND reconcile_deadline < now())
               AS reconcile_expired
    FROM deliveries
    WHERE destination_id = CAST(:destination_id AS UUID)
      AND dedupe_key = :dedupe_key
    FOR UPDATE
    """
)

APPLY_RECEIPT_SQL = text(
    """
    UPDATE deliveries
    SET reconcile_state = :reconcile_state,
        reconciled_at = now(),
        receipt_result = :receipt_result,
        receipt_id = CAST(:receipt_id AS UUID),
        updated_at = now()
    WHERE id = CAST(:delivery_id AS UUID)
    """
)

MARK_TIMED_OUT_SQL = text(
    """
    UPDATE deliveries
    SET reconcile_state = 'timed_out',
        updated_at = now()
    WHERE id = CAST(:delivery_id AS UUID)
    """
)

# A failure receipt after the copy's requeue budget is exhausted: the receiver
# keeps saying it cannot process this copy, so it is parked in the dead-letter
# area instead of cycling forever. The failure receipt still counts as the
# reconciliation outcome (receipt_failed), so the copy is never mistaken for
# acknowledged.
PARK_FAILED_RECEIPT_SQL = text(
    """
    UPDATE deliveries
    SET status = 'dead_lettered',
        dead_letter_reason = 'receipt_failure_exhausted',
        dead_lettered_at = now(),
        updated_at = now()
    WHERE id = CAST(:delivery_id AS UUID)
      AND status = 'delivered'
      AND requeue_count >= :max_requeue_cycles
    """
)

INSERT_RECEIPT_SQL = text(
    """
    INSERT INTO receipts (id, destination_id, dedupe_key, result, delivery_id, disposition)
    VALUES (
        CAST(:receipt_id AS UUID),
        CAST(:destination_id AS UUID),
        :dedupe_key,
        :result,
        CAST(:delivery_id AS UUID),
        :disposition
    )
    RETURNING id, destination_id, dedupe_key, result, delivery_id, disposition, received_at
    """
)


def _lock_delivery(db: Session, destination_id: UUID, dedupe_key: str):
    return db.execute(
        LOCK_DELIVERY_SQL,
        {"destination_id": str(destination_id), "dedupe_key": dedupe_key},
    ).mappings().first()


def ingest_receipt(db: Session, destination_id: UUID, dedupe_key: str, result: str):
    """Record one receipt and reconcile it against its delivery, atomically.

    The delivery row is locked for the whole decision, so concurrent duplicate
    receipts serialize: the first one applies, the rest see the reconciled
    state and are logged as duplicates.
    """
    delivery = _lock_delivery(db, destination_id, dedupe_key)

    # A well-behaved receiver sends its receipt right after answering the
    # delivery call, which can beat the worker's result write by a few
    # milliseconds. While the delivery is still in flight, wait briefly for
    # the worker to commit instead of misjudging the receipt as premature.
    grace_deadline = time.monotonic() + settings.receipt_delivery_grace_seconds
    while (
        delivery is not None
        and delivery["reconcile_state"] == "none"
        and delivery["status"] == "in_flight"
        and time.monotonic() < grace_deadline
    ):
        db.rollback()  # release the row lock and read snapshot, then retry
        time.sleep(0.2)
        delivery = _lock_delivery(db, destination_id, dedupe_key)

    receipt_id = uuid4()
    delivery_id = delivery["id"] if delivery is not None else None
    parked_dead_letter = False
    relay_stopped = False
    relay_delivery_id: UUID | None = None

    if delivery is None:
        disposition = "orphan"
    else:
        state = delivery["reconcile_state"]
        if state == "awaiting":
            if delivery["reconcile_expired"]:
                # The deadline is authoritative even if the sweeper has not
                # run yet: this receipt is late and must not acknowledge.
                db.execute(MARK_TIMED_OUT_SQL, {"delivery_id": str(delivery_id)})
                disposition = "late"
                # A relay station whose deadline passed (even if the sweeper
                # has not run yet) stops its run; the late receipt never
                # opens the next station.
                if delivery["relay_chain_id"] is not None:
                    relay_stopped = True
                    relay_delivery_id = delivery_id
            else:
                db.execute(
                    APPLY_RECEIPT_SQL,
                    {
                        "reconcile_state": (
                            "acknowledged" if result == "success" else "receipt_failed"
                        ),
                        "receipt_result": result,
                        "receipt_id": str(receipt_id),
                        "delivery_id": str(delivery_id),
                    },
                )
                disposition = "applied"
                # A relay station answering failure stops the run behind it
                # whether or not the copy is parked in the dead-letter area;
                # the next station only ever follows a success receipt.
                if result == "failure" and delivery["relay_chain_id"] is not None:
                    relay_stopped = True
                    relay_delivery_id = delivery_id
                # The receiver explicitly failed this copy for the last
                # allowed requeue cycle: park it instead of leaving another
                # receipt_failed copy that operators must chase forever.
                if (
                    result == "failure"
                    and delivery["status"] == "delivered"
                    and delivery["requeue_count"] >= settings.max_requeue_cycles
                ):
                    parked = db.execute(
                        PARK_FAILED_RECEIPT_SQL,
                        {
                            "delivery_id": str(delivery_id),
                            "max_requeue_cycles": settings.max_requeue_cycles,
                        },
                    ).rowcount
                    parked_dead_letter = bool(parked)
        elif state == "none" and delivery["status"] == "delivered":
            # Delivered before reconciliation existed (pre-upgrade row): accept
            # the receipt so legacy deliveries can still be reconciled.
            db.execute(
                APPLY_RECEIPT_SQL,
                {
                    "reconcile_state": (
                        "acknowledged" if result == "success" else "receipt_failed"
                    ),
                    "receipt_result": result,
                    "receipt_id": str(receipt_id),
                    "delivery_id": str(delivery_id),
                },
            )
            disposition = "applied"
        elif state in ("acknowledged", "receipt_failed"):
            # Same receipt delivered again: applied exactly once, this is a no-op.
            disposition = "duplicate"
        elif state == "timed_out" or (
            delivery["status"] == "deadline_expired"
        ):
            # After the receipt deadline, or after the copy was closed because
            # it missed its promised latest-delivery time: visible as late,
            # never flips the row to acknowledged/delivered.
            disposition = "late"
        else:
            # Delivery exists but transport has not completed (pending, or still
            # in flight beyond the grace window).
            disposition = "premature"

    receipt = db.execute(
        INSERT_RECEIPT_SQL,
        {
            "receipt_id": str(receipt_id),
            "destination_id": str(destination_id),
            "dedupe_key": dedupe_key,
            "result": result,
            "delivery_id": str(delivery_id) if delivery_id is not None else None,
            "disposition": disposition,
        },
    ).mappings().one()
    if relay_stopped and relay_delivery_id is not None:
        # Close every still-pending later station of this relay run in the
        # same transaction that recorded the stop; nothing later can read the
        # run as still advancing. Idempotent and keyed on this trigger only.
        relay.cascade_after_stop(db, str(relay_delivery_id))
    db.commit()
    if parked_dead_letter:
        logger.warning(
            "delivery_id=%s destination_id=%s dedupe_key=%s moved to dead letter "
            "after failure receipt exhausted %s requeue cycles",
            delivery_id,
            destination_id,
            dedupe_key,
            settings.max_requeue_cycles,
        )
    return receipt
