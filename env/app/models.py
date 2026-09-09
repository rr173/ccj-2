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
    # Pre-pub/sub databases stored per-destination copies in "events". Rename
    # it to "deliveries" so in-flight rows keep their queue/lease state; the
    # new logical events table is created fresh below.
    """
    DO $$
    BEGIN
        IF to_regclass('public.events') IS NOT NULL
           AND to_regclass('public.deliveries') IS NULL
           AND EXISTS (
               SELECT 1
               FROM information_schema.columns
               WHERE table_name = 'events' AND column_name = 'destination_id'
           ) THEN
            ALTER TABLE events RENAME TO deliveries;
        END IF;
    END $$
    """,
    # One row per submitted event. Fan-out to subscribed destinations happens
    # at ingest time; an event with no subscribers simply has no deliveries.
    """
    CREATE TABLE IF NOT EXISTS events (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        event_type TEXT NOT NULL,
        dedupe_key TEXT NOT NULL UNIQUE,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    # One row per (event, subscribed destination). This is the per-destination
    # queue the worker drains in destination_seq order; payload/dedupe_key are
    # denormalized onto the copy so the worker never joins back to events.
    """
    CREATE TABLE IF NOT EXISTS deliveries (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        event_id UUID REFERENCES events(id),
        destination_id UUID NOT NULL REFERENCES destinations(id),
        event_type TEXT,
        dedupe_key TEXT NOT NULL,
        payload JSONB NOT NULL,
        destination_seq BIGINT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        claim_token UUID,
        claimed_at TIMESTAMPTZ,
        lease_until TIMESTAMPTZ,
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
    CREATE TABLE IF NOT EXISTS destination_subscriptions (
        destination_id UUID NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
        event_type TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (destination_id, event_type)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS delivery_attempts (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        delivery_id UUID REFERENCES deliveries(id),
        event_id UUID REFERENCES events(id),
        destination_id UUID NOT NULL REFERENCES destinations(id),
        attempt_no INTEGER NOT NULL,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ NOT NULL,
        success BOOLEAN NOT NULL,
        status_code INTEGER,
        response_excerpt TEXT,
        error TEXT,
        lost_lease BOOLEAN NOT NULL DEFAULT FALSE,
        CHECK (attempt_no > 0),
        CHECK (finished_at >= started_at)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS events_delivery_idx
        ON deliveries (destination_id, destination_seq)
        WHERE status IN ('pending', 'in_flight')
    """,
    # Idempotent upgrades for databases created before the lease heartbeat.
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ",
    "ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS lost_lease BOOLEAN NOT NULL DEFAULT FALSE",
    "DROP INDEX IF EXISTS events_stale_claim_idx",
    """
    CREATE INDEX IF NOT EXISTS events_lease_until_idx
        ON deliveries (lease_until)
        WHERE status = 'in_flight'
    """,
    """
    CREATE INDEX IF NOT EXISTS destinations_recovery_idx
        ON destinations (recoverable_at)
        WHERE status = 'isolated'
    """,
    # Idempotent upgrades for databases created before pub/sub fan-out.
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS event_id UUID",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS event_type TEXT",
    # Legacy attempt rows referenced the per-destination copy as event_id;
    # that copy is now a delivery, so the column becomes delivery_id.
    """
    DO $$
    BEGIN
        IF EXISTS (
               SELECT 1
               FROM information_schema.columns
               WHERE table_name = 'delivery_attempts' AND column_name = 'event_id'
           )
           AND NOT EXISTS (
               SELECT 1
               FROM information_schema.columns
               WHERE table_name = 'delivery_attempts' AND column_name = 'delivery_id'
           ) THEN
            ALTER TABLE delivery_attempts RENAME COLUMN event_id TO delivery_id;
        END IF;
    END $$
    """,
    "ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS delivery_id UUID",
    "ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS event_id UUID",
    """
    CREATE INDEX IF NOT EXISTS deliveries_event_idx
        ON deliveries (event_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS delivery_attempts_event_idx
        ON delivery_attempts (event_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS destination_subscriptions_type_idx
        ON destination_subscriptions (event_type, destination_id)
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
