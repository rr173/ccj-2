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
    UPDATE deliveries
    SET reconcile_state = 'timed_out',
        updated_at = now()
    WHERE reconcile_state = 'awaiting'
      AND reconcile_deadline < now()
    """
)


def sweep_once() -> int:
    """Mark every overdue awaiting delivery as timed out. Returns the count."""
    db = SessionLocal()
    try:
        result = db.execute(SWEEP_SQL)
        db.commit()
        return result.rowcount
    except SQLAlchemyError:
        db.rollback()
        logger.exception("reconciliation sweep failed; retrying next interval")
        return 0
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
            timed_out = sweep_once()
            if timed_out:
                logger.info("marked %s deliveries as timed_out", timed_out)
            time.sleep(settings.reconcile_sweep_interval_seconds)
    except KeyboardInterrupt:
        logger.info("shutting down reconciler")


if __name__ == "__main__":
    main()
