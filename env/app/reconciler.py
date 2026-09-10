"""Receipt reconciliation sweeper.

Runs as its own process (``python -m app.reconciler``), separate from the
outbound delivery worker, so reconciliation can be deployed and scaled
independently. Its only job: deliveries that were handed off successfully but
never got a matching receipt within the agreed window
(``RECEIPT_TIMEOUT_SECONDS``) are flipped from ``awaiting`` to ``timed_out``.

A timed-out delivery is NOT acknowledged — it stays visible as unreconciled
until an operator requeues it for redelivery. Receipts that arrive after the
deadline are recorded as ``late`` by the ingest API and never flip a timed-out
delivery back to acknowledged.

After ``MAX_REQUEUE_CYCLES`` requeued redeliveries still time out (a delivery
whose ``requeue_count`` already reached the limit), the sweeper parks the copy
in the dead-letter area instead of leaving it as yet another timed-out copy:
it stays queryable (which copy, which address, why), is never sent again on
its own, and no longer blocks later copies of the same address. An explicit
manual revive resets the cycle budget and puts the copy back in its original
queue position.
"""

import logging
import time

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.config import settings
from app.db import SessionLocal, build_engine
from app.models import init_db

logger = logging.getLogger("reconciler")

SWEEP_SQL = text(
    """
    WITH expired AS (
        SELECT id,
               requeue_count >= :max_requeue_cycles AS exhausted
        FROM deliveries
        WHERE reconcile_state = 'awaiting'
          AND reconcile_deadline < now()
    ), swept AS (
        UPDATE deliveries d
        SET reconcile_state = 'timed_out',
            -- Requeue cycles spent; the receipt still never matched: park the
            -- copy in the dead-letter area rather than keep cycling it.
            status = CASE WHEN e.exhausted THEN 'dead_lettered' ELSE d.status END,
            dead_letter_reason = CASE
                WHEN e.exhausted THEN 'receipt_timeout_exhausted'
                ELSE d.dead_letter_reason
            END,
            dead_lettered_at = CASE
                WHEN e.exhausted THEN now() ELSE d.dead_lettered_at
            END,
            updated_at = now()
        FROM expired e
        WHERE d.id = e.id
        RETURNING e.exhausted AS exhausted
    )
    SELECT
        COUNT(*) FILTER (WHERE NOT exhausted)::int AS timed_out,
        COUNT(*) FILTER (WHERE exhausted)::int AS dead_lettered
    FROM swept
    """
)


def sweep_once() -> tuple[int, int]:
    """Sweep overdue awaiting deliveries.

    Returns ``(timed_out_count, dead_lettered_count)``: copies still within
    their requeue budget are simply marked timed out; copies whose requeue
    cycles are exhausted are moved to the dead-letter area.
    """
    db = SessionLocal()
    try:
        row = db.execute(
            SWEEP_SQL,
            {"max_requeue_cycles": settings.max_requeue_cycles},
        ).mappings().one()
        db.commit()
        return row["timed_out"], row["dead_lettered"]
    except SQLAlchemyError:
        db.rollback()
        logger.exception("reconciliation sweep failed; retrying next interval")
        return 0, 0
    finally:
        db.close()


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

    logger.info(
        "reconciler started; sweep interval=%ss, receipt timeout=%ss",
        settings.reconcile_sweep_interval_seconds,
        settings.receipt_timeout_seconds,
    )
    try:
        while True:
            timed_out, dead_lettered = sweep_once()
            if timed_out:
                logger.info("marked %s deliveries as timed_out", timed_out)
            if dead_lettered:
                logger.warning(
                    "moved %s deliveries to dead letter after exhausting %s "
                    "requeue cycles",
                    dead_lettered,
                    settings.max_requeue_cycles,
                )
            time.sleep(settings.reconcile_sweep_interval_seconds)
    except KeyboardInterrupt:
        logger.info("shutting down reconciler")


if __name__ == "__main__":
    main()
