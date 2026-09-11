"""Admission control for inbound events.

An external system first registers an ``event_sources`` row and receives a
secret. Every event it pushes must then prove three things:

1. who it is          — ``X-Source-Id`` identifies the registered source;
2. when it sent this  — ``X-Signed-At`` is the sender's Unix send time;
3. that it holds the  — ``X-Signature`` is HMAC-SHA256(secret,
   source's secret      ``"<X-Signed-At>." + raw request body``), hex-encoded.

A failed admission is *recorded, not accepted*: every attempt (including bad
signature, stale/replayed send time and a disabled source) is written to
``ingestion_attempts`` with an explicit disposition, and no event or delivery
row is created. Existing traces/queries therefore can never describe a
rejected push as accepted or sent out.
"""

import hashlib
import hmac
import json
import secrets
import time
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import settings

SOURCE_ID_HEADER = "X-Source-Id"
SIGNED_AT_HEADER = "X-Signed-At"
SIGNATURE_HEADER = "X-Signature"

# Dispositions stored in ingestion_attempts.
ACCEPTED = "accepted"
UNROUTED = "unrouted"
# The event was accepted and stored, but every subscriber was still waiting
# to complete its activation handshake: no delivery rows were created, so it
# was not written as "sent" and can never be backfilled after confirmation.
PENDING_CONFIRMATION = "pending_confirmation"
# The event was accepted and stored, subscribers exist and are confirmed, but
# every one of them carries its own subscription condition that withheld this
# exact body: no delivery rows were created (so it was never written as sent),
# while the per-address judgements stay visible in
# subscription_filter_evaluations. Distinct from 'unrouted' (nobody
# subscribes) and from 'pending_confirmation'.
FILTERED = "filtered"
DUPLICATE = "duplicate"
SOURCE_UNKNOWN = "source_unknown"
SOURCE_DISABLED = "source_disabled"
BAD_SIGNATURE = "bad_signature"
STALE_TIMESTAMP = "stale_timestamp"
FUTURE_TIMESTAMP = "future_timestamp"
INVALID_TIMESTAMP = "invalid_timestamp"
INVALID_BODY = "invalid_body"

REJECTION_DISPOSITIONS = {
    SOURCE_UNKNOWN,
    SOURCE_DISABLED,
    BAD_SIGNATURE,
    STALE_TIMESTAMP,
    FUTURE_TIMESTAMP,
    INVALID_TIMESTAMP,
    INVALID_BODY,
}


class AdmissionError(Exception):
    """A rejected inbound attempt. The caller logs it and returns the code."""

    def __init__(self, status_code: int, disposition: str, reason: str):
        super().__init__(reason)
        self.status_code = status_code
        self.disposition = disposition
        self.reason = reason


def generate_secret() -> str:
    return secrets.token_urlsafe(32)


def compute_signature(secret: str, signed_at: str, body: bytes) -> str:
    """HMAC-SHA256 over ``"<send-time>." + raw body``, hex-encoded."""
    message = signed_at.encode("ascii") + b"." + body
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def signature_matches(secret: str, signed_at: str, body: bytes, signature: str) -> bool:
    provided = signature.removeprefix("sha256=").strip()
    expected = compute_signature(secret, signed_at, body)
    # Constant-time comparison: a wrong key must not be guessable from timing.
    return hmac.compare_digest(provided, expected)


def log_attempt(
    db: Session,
    *,
    disposition: str,
    source_id: str | None = None,
    source_name: str | None = None,
    event_id: str | None = None,
    dedupe_key: str | None = None,
    event_type: str | None = None,
    signed_at: datetime | None = None,
    reason: str | None = None,
    remote_addr: str | None = None,
    commit: bool = False,
) -> None:
    """Persist one inbound attempt. Rejections commit on their own so they stay
    queryable even though no event was created."""
    db.execute(
        text(
            """
            INSERT INTO ingestion_attempts
                (source_id, source_name, event_id, dedupe_key, event_type,
                 signed_at, disposition, reason, remote_addr)
            VALUES
                (CAST(:source_id AS UUID), :source_name, CAST(:event_id AS UUID),
                 :dedupe_key, :event_type, CAST(:signed_at AS TIMESTAMPTZ),
                 :disposition, :reason, :remote_addr)
            """
        ),
        {
            "source_id": source_id,
            "source_name": source_name,
            "event_id": event_id,
            "dedupe_key": dedupe_key,
            "event_type": event_type,
            "signed_at": signed_at,
            "disposition": disposition,
            "reason": reason,
            "remote_addr": remote_addr,
        },
    )
    if commit:
        db.commit()


def _parse_signed_at(raw: str) -> tuple[datetime, str]:
    """Parse the send-time header into (UTC datetime, normalized string)."""
    try:
        seconds = float(raw)
    except ValueError:
        raise AdmissionError(
            401,
            INVALID_TIMESTAMP,
            f"{SIGNED_AT_HEADER} must be a Unix timestamp in seconds",
        )
    return datetime.fromtimestamp(seconds, tz=timezone.utc), raw


def _is_uuid(value: str | None) -> bool:
    if not value:
        return False
    try:
        UUID(value)
        return True
    except ValueError:
        return False


def authenticate(
    db: Session,
    *,
    source_id_header: str | None,
    signed_at_header: str | None,
    signature_header: str | None,
    body: bytes,
    remote_addr: str | None = None,
) -> dict[str, Any]:
    """Validate headers/signature/timestamp and return the active source row.

    On failure, records the attempt and raises :class:`AdmissionError`.
    Signature verification precedes the freshness/disabled checks so an
    outsider cannot distinguish a disabled source from an unknown one.
    """
    source_id = (source_id_header or "").strip()
    signed_at_raw = (signed_at_header or "").strip()
    signature = (signature_header or "").strip()

    source = None
    if _is_uuid(source_id):
        source = db.execute(
            text(
                """
                SELECT id, name, secret, disabled_at, created_at, key_rotated_at
                FROM event_sources
                WHERE id = CAST(:source_id AS UUID)
                """
            ),
            {"source_id": source_id},
        ).mappings().first()

    signed_at: datetime | None = None
    if signed_at_raw:
        signed_at, signed_at_raw = _parse_signed_at(signed_at_raw)

    def reject(disposition: str, status_code: int, reason: str) -> None:
        log_attempt(
            db,
            disposition=disposition,
            source_id=source_id if _is_uuid(source_id) else None,
            source_name=source["name"] if source else None,
            signed_at=signed_at,
            reason=reason,
            remote_addr=remote_addr,
            commit=True,
        )
        raise AdmissionError(status_code, disposition, reason)

    if not _is_uuid(source_id):
        reject(SOURCE_UNKNOWN, 401, f"missing or malformed {SOURCE_ID_HEADER}")
    if not signed_at_raw:
        reject(INVALID_TIMESTAMP, 401, f"missing {SIGNED_AT_HEADER}")
    if not signature:
        reject(BAD_SIGNATURE, 401, f"missing {SIGNATURE_HEADER}")
    if source is None:
        reject(SOURCE_UNKNOWN, 401, "source is not registered")
    if not signature_matches(source["secret"], signed_at_raw, body, signature):
        reject(BAD_SIGNATURE, 401, "signature does not match this source's secret")

    age = (datetime.now(timezone.utc) - signed_at).total_seconds()
    if age > settings.ingest_max_age_seconds:
        reject(
            STALE_TIMESTAMP,
            401,
            f"signed event is {int(age)}s old, older than the "
            f"{int(settings.ingest_max_age_seconds)}s replay window",
        )
    if -age > settings.ingest_max_future_skew_seconds:
        reject(
            FUTURE_TIMESTAMP,
            401,
            f"signed send time is {int(-age)}s in the future, beyond the "
            f"{int(settings.ingest_max_future_skew_seconds)}s allowed clock skew",
        )
    if source["disabled_at"] is not None:
        reject(
            SOURCE_DISABLED,
            403,
            "source has been disabled; new events from it are no longer admitted",
        )

    result = dict(source)
    result["signed_at"] = signed_at
    result["signed_at_raw"] = signed_at_raw
    return result


def parse_event_body(
    db: Session,
    body: bytes,
    *,
    source: dict[str, Any] | None,
    signed_at: datetime | None,
    remote_addr: str | None = None,
) -> dict[str, Any]:
    """Decode the JSON request body into a plain dict for EventIn validation.

    Malformed JSON is a rejected attempt (logged against the authenticated
    source) but never an accepted event.
    """
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        log_attempt(
            db,
            disposition=INVALID_BODY,
            source_id=str(source["id"]) if source else None,
            source_name=source["name"] if source else None,
            signed_at=signed_at,
            reason=f"request body must be valid JSON: {exc}",
            remote_addr=remote_addr,
            commit=True,
        )
        raise AdmissionError(422, INVALID_BODY, "request body must be valid JSON")
    if not isinstance(parsed, dict):
        log_attempt(
            db,
            disposition=INVALID_BODY,
            source_id=str(source["id"]) if source else None,
            source_name=source["name"] if source else None,
            signed_at=signed_at,
            reason="request body must be a JSON object",
            remote_addr=remote_addr,
            commit=True,
        )
        raise AdmissionError(422, INVALID_BODY, "request body must be a JSON object")
    return parsed


def current_signed_at() -> str:
    return str(int(time.time()))
