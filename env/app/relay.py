"""Per-event-type relay chains ("接力").

An event type can have at most one ACTIVE relay chain: an ordered list of
receiving addresses ("stations"). Events of that type accepted AFTER a chain
is defined no longer fan out to every subscriber; they walk the chain:

* station 1 gets its own delivery copy first;
* every later station's copy is created up front (so the run is always fully
  auditable as "which stations exist, which have acknowledged, which have not
  been reached yet") but is withheld — pending, never claimable by the worker
  — until the immediately preceding station's copy carries a matching
  SUCCESS receipt ("认了") within its reconciliation window;
* once the predecessor acknowledges, exactly the next station becomes
  deliverable; the rest keep waiting.

Stop rule ("后面各站都不要再补"): when a station answers with a failure
receipt, runs past its reconcile deadline without a receipt, gets parked in
the dead-letter area, or is superseded because its destination changed
location before the copy went out, every later station still pending is
moved to the terminal ``relay_skipped`` state carrying the stop reason and a
pointer to the station that stopped it. Skipped rows never go out, are never
backfilled and are never written as "sent"; they stay visible as
"not reached because an earlier station stopped".

Only events accepted AFTER a chain is defined walk it; events accepted
beforehand have no relay snapshot (``relay_chain_id`` NULL) and keep their
ordinary lifecycle. Re-defining a chain order creates a new chain VERSION:
each delivery snapshots the version and station number at fan-out, so an
event already on its way keeps exactly the stations it set out with while
only later accepted events follow the new order. The same destination can
never occupy two stations of one chain.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("relay-chain")

RELAY_SKIPPED = "relay_skipped"

# Reasons written onto relay_skipped rows (and checked by the schema). They
# mirror the triggering station's own stop cause.
REASON_TRANSPORT_EXHAUSTED = "delivery_attempts_exhausted"
REASON_RECEIPT_TIMEOUT_EXHAUSTED = "receipt_timeout_exhausted"
REASON_RECEIPT_FAILURE_EXHAUSTED = "receipt_failure_exhausted"
REASON_RECEIPT_FAILURE = "receipt_failed"
REASON_RECEIPT_TIMEOUT = "receipt_timeout"
REASON_SUPERSEDED = "superseded"

# Terminal/stop states a station copy can be in that make the run stop behind
# it. A station is NOT a stop while it is merely waiting (pending / in_flight
# / delivered-awaiting / retried).
_STOP_STATUSES = ("dead_lettered", "superseded")
_STOP_RECONCILE_STATES = ("receipt_failed", "timed_out")

CHAIN_COLUMNS = "id, event_type, version, active, created_at"
STATION_COLUMNS = "chain_id, station_no, destination_id"


class RelayConfigError(Exception):
    """The submitted chain definition is invalid."""

    def __init__(self, status_code: int, reason: str):
        super().__init__(reason)
        self.status_code = status_code
        self.reason = reason


def get_active_chain(db: Session, event_type: str) -> dict[str, Any] | None:
    """The active chain version for an event type, with its stations in
    order, or None when the type is not chained right now."""
    row = db.execute(
        text(
            """
            SELECT id, event_type, version, created_at
            FROM relay_chains
            WHERE event_type = :event_type AND active = TRUE
            """
        ),
        {"event_type": event_type},
    ).mappings().first()
    if row is None:
        return None
    chain = dict(row)
    stations = db.execute(
        text(
            f"""
            SELECT {STATION_COLUMNS}
            FROM relay_chain_stations
            WHERE chain_id = CAST(:chain_id AS UUID)
            ORDER BY station_no
            """
        ),
        {"chain_id": str(chain["id"])},
    ).mappings().all()
    chain["stations"] = [dict(s) for s in stations]
    return chain


def list_active_chains(db: Session) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            f"""
            SELECT {CHAIN_COLUMNS}
            FROM relay_chains
            WHERE active = TRUE
            ORDER BY event_type
            """
        )
    ).mappings().all()
    chains = []
    for row in rows:
        chain = dict(row)
        stations = db.execute(
            text(
                f"""
                SELECT {STATION_COLUMNS}
                FROM relay_chain_stations
                WHERE chain_id = CAST(:chain_id AS UUID)
                ORDER BY station_no
                """
            ),
            {"chain_id": str(chain["id"])},
        ).mappings().all()
        chain["stations"] = [dict(s) for s in stations]
        chains.append(chain)
    return chains


def define_chain(
    db: Session,
    event_type: str,
    destination_ids: list[str],
) -> dict[str, Any]:
    """Create a new ACTIVE chain version for an event type.

    The previous active version (if any) becomes inactive atomically but is
    retained: deliveries already fanned out under it keep their snapshot. The
    station list must be non-empty, reference existing destinations and
    contain no duplicate destination.
    """
    if not destination_ids:
        raise RelayConfigError(422, "a relay chain needs at least one station")
    if len(set(destination_ids)) != len(destination_ids):
        raise RelayConfigError(
            422, "the same destination cannot occupy two stations of one chain"
        )
    # Lock the type's chain namespace so concurrent PUTs cannot each mark the
    # other's version inactive. pg_advisory_xact_lock takes a bigint; a stable
    # 63-bit hash of the type name is good enough to serialize per-type.
    digest = hashlib.sha256(event_type.encode("utf-8")).digest()
    type_hash = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
    db.execute(
        text("SELECT pg_advisory_xact_lock(:key)"), {"key": type_hash}
    )

    existing = db.execute(
        text(
            "SELECT id FROM destinations WHERE id = ANY(CAST(:ids AS UUID[]))"
        ),
        {"ids": destination_ids},
    ).mappings().all()
    found = {str(row["id"]) for row in existing}
    missing = [d for d in destination_ids if d not in found]
    if missing:
        raise RelayConfigError(
            404, f"unknown destination id(s) in chain stations: {missing}"
        )

    # A type cannot be both preview-gated and relay-chained: the preview gate
    # is per-destination while a relay is cross-destination sequential;
    # combining the two would make "this address nodded" and "the previous
    # address acknowledged" two different gates on the same body.
    gated = db.execute(
        text(
            "SELECT 1 FROM event_type_preview_policies WHERE event_type = :event_type"
        ),
        {"event_type": event_type},
    ).first()
    if gated is not None:
        raise RelayConfigError(
            409,
            "event type already has a preview-consent policy; a type cannot be "
            "both preview-gated and relay-chained",
        )

    # Deactivate prior versions FIRST: the partial unique index allows only
    # one active row per event type, so inserting the new active version while
    # the old one is still active would violate it. Old rows are retained —
    # deliveries already fanned out under them keep their version snapshot.
    db.execute(
        text(
            """
            UPDATE relay_chains SET active = FALSE
            WHERE event_type = :event_type AND active = TRUE
            """
        ),
        {"event_type": event_type},
    )
    row = db.execute(
        text(
            """
            INSERT INTO relay_chains (event_type, version, active)
            SELECT :event_type,
                   COALESCE(MAX(version), 0) + 1,
                   TRUE
            FROM relay_chains
            WHERE event_type = :event_type
            RETURNING id, event_type, version, active, created_at
            """
        ),
        {"event_type": event_type},
    ).mappings().one()
    chain_id = str(row["id"])
    db.execute(
        text(
            """
            INSERT INTO relay_chain_stations (chain_id, station_no, destination_id)
            SELECT CAST(:chain_id AS UUID), station_no,
                   CAST(station_id AS UUID)
            FROM unnest(CAST(:station_ids AS UUID[]),
                        CAST(:station_nos AS INTEGER[]))
                 AS t(station_id, station_no)
            """
        ),
        {
            "chain_id": chain_id,
            "station_ids": destination_ids,
            "station_nos": list(range(1, len(destination_ids) + 1)),
        },
    )
    chain = dict(row)
    chain["stations"] = [
        {"chain_id": row["id"], "station_no": i + 1, "destination_id": d}
        for i, d in enumerate(destination_ids)
    ]
    return chain


def delete_active_chain(db: Session, event_type: str) -> bool:
    """Deactivate the current chain. Only events accepted afterwards return
    to ordinary fan-out; deliveries already fanned out under the old version
    keep walking it. Returns False when no active chain existed."""
    updated = db.execute(
        text(
            """
            UPDATE relay_chains SET active = FALSE
            WHERE event_type = :event_type AND active = TRUE
            RETURNING id
            """
        ),
        {"event_type": event_type},
    ).mappings().all()
    return len(updated) > 0


def fan_out_relay_event(
    db: Session,
    *,
    event_id: str,
    chain: dict[str, Any],
    dedupe_key: str,
    payload: str,
    not_before: Any,
) -> None:
    """Create one body delivery per chain station, in station order.

    Every copy carries the chain/version snapshot and its station number.
    Station 1 is a normal pending copy; later stations are withheld by the
    worker claim gate (predecessor must be acknowledged). All destinations are
    for-real for the run regardless of their observe-only flag, and
    confirmation state is not required to create the copy — an unconfirmed
    station simply cannot be claimed until its own handshake completes. The
    per-destination sequence numbers are bumped under a row lock in station
    order, the same discipline as ordinary fan-out.
    """
    chain_id = str(chain["id"])
    # Stations come from get_active_chain (dict rows) or define_chain (plain
    # id strings); normalize both to destination id strings in order.
    station_ids = [
        str(s["destination_id"]) if isinstance(s, dict) else str(s)
        for s in chain["stations"]
    ]
    # Lock destinations in a stable order (id) to keep concurrent fan-outs
    # deadlock-free; inserts themselves follow station order.
    db.execute(
        text(
            """
            SELECT id FROM destinations
            WHERE id = ANY(CAST(:ids AS UUID[]))
            ORDER BY id
            FOR UPDATE
            """
        ),
        {"ids": station_ids},
    ).all()
    for station_no, destination_id in enumerate(station_ids, start=1):
        db.execute(
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
                     destination_seq, not_before, confirmation_generation,
                     observe_only, relay_chain_id, relay_station_no)
                SELECT :event_id, id, :event_type, :dedupe_key,
                       CAST(:payload AS JSONB), next_event_seq,
                       CAST(:not_before AS TIMESTAMPTZ), confirmation_generation,
                       FALSE, CAST(:relay_chain_id AS UUID), :station_no
                FROM bumped
                """
            ),
            {
                "event_id": event_id,
                "destination_id": destination_id,
                "event_type": chain["event_type"],
                "dedupe_key": dedupe_key,
                "payload": payload,
                "not_before": not_before,
                "relay_chain_id": chain_id,
                "station_no": station_no,
            },
        )


def cascade_after_stop(db: Session, delivery_id: str) -> int:
    """Mark every still-pending later station of this run as relay_skipped.

    Called right after ONE station reaches a stop state (failure receipt,
    reconcile timeout / receipt dead-letter, transport dead-letter,
    supersede). Only pending rows for later stations of the same
    (event, chain version) are closed; already-acknowledged/delivered earlier
    stations and the trigger itself are untouched. Idempotent: once a run has
    been closed behind a stop there are no pending later rows left.
    """
    result = db.execute(
        text(
            """
            WITH trigger_row AS (
                SELECT event_id, relay_chain_id, relay_station_no
                FROM deliveries
                WHERE id = CAST(:delivery_id AS UUID)
                  AND relay_chain_id IS NOT NULL
            ), reason AS (
                -- The skip reason mirrors the triggering station's own stop
                -- cause: a parked copy keeps its dead-letter reason; a
                -- superseded copy is 'superseded'; a delivered copy that got
                -- a failure receipt / ran past its reconcile deadline uses
                -- the matching receipt reason.
                SELECT
                    CASE
                        WHEN d.status = 'dead_lettered' THEN d.dead_letter_reason
                        WHEN d.status = 'superseded' THEN 'superseded'
                        WHEN d.reconcile_state = 'receipt_failed' THEN 'receipt_failed'
                        WHEN d.reconcile_state = 'timed_out' THEN 'receipt_timeout'
                        ELSE NULL
                    END AS reason
                FROM deliveries d
                WHERE d.id = CAST(:delivery_id AS UUID)
            ), skipped AS (
                UPDATE deliveries later
                SET status = 'relay_skipped',
                    relay_skipped_at = now(),
                    relay_skip_reason = (SELECT reason FROM reason),
                    relay_stopped_by_delivery_id = CAST(:delivery_id AS UUID),
                    claim_token = NULL,
                    claimed_at = NULL,
                    lease_until = NULL,
                    next_attempt_at = now(),
                    updated_at = now()
                FROM trigger_row t, reason r
                WHERE later.event_id = t.event_id
                  AND later.relay_chain_id = t.relay_chain_id
                  AND later.relay_station_no > t.relay_station_no
                  AND later.status = 'pending'
                  AND r.reason IS NOT NULL
                RETURNING later.id
            )
            SELECT count(*)::int AS count FROM skipped
            """
        ),
        {"delivery_id": delivery_id},
    ).mappings().one()
    return result["count"]


def cascade_superseded_destination(
    db: Session, destination_id: str
) -> int:
    """Close relay runs whose pending station at this destination was just
    superseded by a location change.

    The arm-round supersede in the worker/API marks pending old-generation
    copies 'superseded' in bulk; this runs the relay stop cascade for each
    relay copy among them. Every later pending station of its run is closed;
    a delivery that already reached the old location is left to its own
    result path (and cascades there if it later fails).
    """
    rows = db.execute(
        text(
            """
            SELECT d.id
            FROM deliveries d
            WHERE d.destination_id = CAST(:destination_id AS UUID)
              AND d.relay_chain_id IS NOT NULL
              AND d.status = 'superseded'
            ORDER BY d.event_id, d.relay_station_no
            """
        ),
        {"destination_id": destination_id},
    ).mappings().all()
    total = 0
    for row in rows:
        total += cascade_after_stop(db, str(row["id"]))
    return total


def build_event_relay(db: Session, event: dict[str, Any]) -> dict[str, Any] | None:
    """Assemble the run view for one event from its station copies.

    Returns None for an ordinary (non-relay) event. For a relay event every
    station is listed in order with its copy's current state, which stations
    have acknowledged and which have not been reached yet, plus the current
    ("walking") station and — once stopped — the station/reason it stopped at.
    """
    rows = db.execute(
        text(
            """
            SELECT d.id AS delivery_id, d.destination_id, dest.url AS destination_url,
                   d.relay_station_no AS station_no, d.status,
                   d.reconcile_state, d.delivered_at,
                   d.relay_skip_reason, d.relay_skipped_at,
                   d.relay_stopped_by_delivery_id, d.dead_letter_reason,
                   d.confirmation_generation,
                   dest.confirmation_state,
                   dest.confirmation_generation AS destination_generation
            FROM deliveries d
            JOIN destinations dest ON dest.id = d.destination_id
            WHERE d.event_id = CAST(:event_id AS UUID)
              AND d.relay_chain_id IS NOT NULL
              AND d.phase = 'body'
            ORDER BY d.relay_station_no
            """
        ),
        {"event_id": str(event["id"])},
    ).mappings().all()
    if not rows:
        return None

    stations = []
    acknowledged = 0
    delivered = 0
    skipped = 0
    stop_station = None
    stop_reason = None
    current_station = None
    for row in rows:
        no = row["station_no"]
        state = row["status"]
        reconcile = row["reconcile_state"]
        is_ack = reconcile == "acknowledged"
        is_skipped = state == RELAY_SKIPPED
        if is_ack:
            acknowledged += 1
        if row["delivered_at"] is not None:
            delivered += 1
        if is_skipped:
            skipped += 1
        # "认了": a success receipt matched. "已到": delivered at least once.
        if state in _STOP_STATUSES or reconcile in _STOP_RECONCILE_STATES:
            if stop_station is None:
                stop_station = no
                stop_reason = _reason_of(row)
        # The current station is the first one not yet settled: it either
        # holds a copy that is still moving/waiting, or the run has stopped
        # right at it.
        settled_ack = is_ack
        if current_station is None and not settled_ack:
            current_station = no
        stations.append(
            {
                "station_no": no,
                "destination_id": str(row["destination_id"]),
                "destination_url": row["destination_url"],
                "delivery_id": str(row["delivery_id"]),
                "status": state,
                "reconcile_state": reconcile,
                "acknowledged": is_ack,
                "delivered": row["delivered_at"] is not None,
                "reached": row["delivered_at"] is not None
                or state in ("in_flight", "dead_lettered", "superseded")
                or reconcile in ("awaiting", "receipt_failed", "timed_out", "acknowledged"),
                "skip_reason": row["relay_skip_reason"],
                "skipped_at": row["relay_skipped_at"],
                "stopped_by_delivery_id": (
                    str(row["relay_stopped_by_delivery_id"])
                    if row["relay_stopped_by_delivery_id"]
                    else None
                ),
            }
        )

    total = len(rows)
    completed = acknowledged >= total and total > 0
    # Once stopped, the "current" station is the earliest one that stopped
    # (the tail stations are relay_skipped, never themselves "in progress").
    stopped = stop_station is not None and not completed
    current_station = stop_station if stopped else (
        None if completed else current_station
    )
    chain_id = db.execute(
        text(
            """
            SELECT DISTINCT relay_chain_id
            FROM deliveries
            WHERE event_id = CAST(:event_id AS UUID)
              AND relay_chain_id IS NOT NULL
            LIMIT 1
            """
        ),
        {"event_id": str(event["id"])},
    ).mappings().first()["relay_chain_id"]
    version = db.execute(
        text(
            "SELECT event_type, version, active FROM relay_chains WHERE id = :id"
        ),
        {"id": chain_id},
    ).mappings().first()

    return {
        "chained": True,
        "event_type": version["event_type"] if version else event.get("event_type"),
        "chain_id": str(chain_id),
        "chain_version": version["version"] if version else None,
        "chain_active": bool(version["active"]) if version else False,
        "station_count": total,
        "acknowledged_count": acknowledged,
        "delivered_count": delivered,
        "skipped_count": skipped,
        # 1-based station the run is at: the first station that has not
        # acknowledged; null once every station acknowledged.
        "current_station_no": None if completed else current_station,
        "stopped": stop_station is not None and not completed,
        "stopped_at_station_no": stop_station if stop_station is not None and not completed else None,
        "stop_reason": stop_reason if stop_station is not None and not completed else None,
        "completed": completed,
        "stations": stations,
    }


def _reason_of(row) -> str | None:
    if row["status"] == "superseded":
        return REASON_SUPERSEDED
    if row["status"] == "dead_lettered":
        return row["dead_letter_reason"] or REASON_TRANSPORT_EXHAUSTED
    if row["reconcile_state"] == "receipt_failed":
        return REASON_RECEIPT_FAILURE
    if row["reconcile_state"] == "timed_out":
        return REASON_RECEIPT_TIMEOUT
    return None
