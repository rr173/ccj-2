from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS destinations (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        url TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL DEFAULT 'active',
        failure_count INTEGER NOT NULL DEFAULT 0,
        next_event_seq BIGINT NOT NULL DEFAULT 0,
        recoverable_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CHECK (status IN ('active', 'isolated')),
        CHECK (failure_count >= 0),
        CHECK (next_event_seq >= 0)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        destination_id UUID NOT NULL REFERENCES destinations(id),
        dedupe_key TEXT NOT NULL,
        payload JSONB NOT NULL,
        destination_seq BIGINT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        claim_token UUID,
        claimed_at TIMESTAMPTZ,
        next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        delivered_at TIMESTAMPTZ,
        UNIQUE (destination_id, destination_seq),
        UNIQUE (destination_id, dedupe_key),
        CHECK (status IN ('pending', 'in_flight', 'delivered')),
        CHECK (attempts >= 0),
        CHECK (destination_seq >= 0)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS delivery_attempts (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        event_id UUID NOT NULL REFERENCES events(id),
        destination_id UUID NOT NULL REFERENCES destinations(id),
        attempt_no INTEGER NOT NULL,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ NOT NULL,
        success BOOLEAN NOT NULL,
        status_code INTEGER,
        response_excerpt TEXT,
        error TEXT,
        CHECK (attempt_no > 0),
        CHECK (finished_at >= started_at)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS events_delivery_idx
        ON events (destination_id, destination_seq)
        WHERE status IN ('pending', 'in_flight')
    """,
    """
    CREATE INDEX IF NOT EXISTS events_stale_claim_idx
        ON events (claimed_at)
        WHERE status = 'in_flight'
    """,
    """
    CREATE INDEX IF NOT EXISTS destinations_recovery_idx
        ON destinations (recoverable_at)
        WHERE status = 'isolated'
    """,
]


def init_db(bind: Engine | Connection) -> None:
    def _run(conn: Connection) -> None:
        for statement in SCHEMA_STATEMENTS:
            conn.execute(text(statement))

    if isinstance(bind, Engine):
        with bind.begin() as conn:
            _run(conn)
    else:
        _run(bind)
        bind.commit()
