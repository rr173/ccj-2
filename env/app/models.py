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
        -- Activation handshake. A freshly registered (or re-located)
        -- destination starts 'pending': the confirmer sends it a one-time
        -- challenge and it only becomes 'confirmed' once the challenge is
        -- echoed back correctly before the round deadline. No event is ever
        -- fanned out to, or claimed for, a destination that is not confirmed.
        confirmation_state TEXT NOT NULL DEFAULT 'pending',
        challenge_token TEXT,
        challenge_expires_at TIMESTAMPTZ,
        confirmed_at TIMESTAMPTZ,
        -- Bumped on every re-arm (registration / URL change). Deliveries copy
        -- the generation they were fanned out under; after a URL change the
        -- claim gate refuses older-generation copies, so queued events never
        -- move to the new location and a failed in-flight copy is not retried.
        confirmation_generation BIGINT NOT NULL DEFAULT 1,
        -- Round number increments each time a new challenge is issued; the
        -- per-round attempt count drives probe backoff within one round.
        confirmation_round BIGINT NOT NULL DEFAULT 1,
        confirmation_attempt_count INTEGER NOT NULL DEFAULT 0,
        next_probe_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        -- Observe-only ("shadow") destinations still get their own copy of
        -- every subscribed event, with their own delivery/retries/dead-letter
        -- lifecycle, but their receipts never count toward the whole event's
        -- acknowledgement and event-level requeue never selects their copies.
        -- The flag is snapshotted onto each delivery at fan-out time, so a
        -- later toggle never rewrites the standing of already-created copies.
        observe_only BOOLEAN NOT NULL DEFAULT FALSE,
        CHECK (status IN ('active', 'isolated')),
        CHECK (failure_count >= 0),
        CHECK (next_event_seq >= 0),
        CHECK (confirmation_state IN ('pending', 'confirmed')),
        CHECK (confirmation_generation >= 1),
        CHECK (confirmation_round >= 1),
        CHECK (confirmation_attempt_count >= 0)
    )
    """,
    # Audit trail of the activation handshake for each destination:
    # every challenge probe sent, every echo received (correct or not), and
    # every round that expired unanswered. Nothing here creates deliveries.
    """
    CREATE TABLE IF NOT EXISTS confirmation_attempts (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        destination_id UUID NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
        confirmation_round BIGINT NOT NULL,
        kind TEXT NOT NULL,
        result TEXT NOT NULL,
        status_code INTEGER,
        response_excerpt TEXT,
        error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CHECK (kind IN ('challenge', 'echo', 'expired')),
        CHECK (result IN (
            'sent', 'confirmed', 'failed', 'invalid', 'expired'
        ))
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS confirmation_attempts_destination_idx
        ON confirmation_attempts (destination_id, created_at DESC, id DESC)
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
        not_before TIMESTAMPTZ,
        cancelled_at TIMESTAMPTZ,
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
        not_before TIMESTAMPTZ,
        last_error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        delivered_at TIMESTAMPTZ,
        reconcile_state TEXT NOT NULL DEFAULT 'none',
        reconcile_deadline TIMESTAMPTZ,
        reconciled_at TIMESTAMPTZ,
        receipt_result TEXT,
        receipt_id UUID,
        requeue_count INTEGER NOT NULL DEFAULT 0,
        -- Consecutive transport failures for this copy; reset to 0 by a 2xx
        -- or by a manual revive from the dead-letter area. attempts (the
        -- total try count used in the audit trail) keeps growing.
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        -- Dead-letter area. A copy becomes terminally 'dead_lettered' when its
        -- own consecutive transport failures reach the limit
        -- (dead_letter_reason 'delivery_attempts_exhausted') or when it has
        -- been requeued the configured number of times and still does not get
        -- a matching receipt ('receipt_timeout_exhausted' /
        -- 'receipt_failure_exhausted'). Dead copies are never claimed again,
        -- never block later copies of the same destination, and only leave the
        -- area via an explicit manual revive.
        dead_letter_reason TEXT,
        dead_lettered_at TIMESTAMPTZ,
        -- Generation of the destination's activation handshake this copy was
        -- fanned out under. A URL change re-arms confirmation and bumps the
        -- destination generation; older copies then fail the claim gate and
        -- queued ones are moved to the terminal 'superseded' state so nothing
        -- more is sent to the old location and nothing old is backfilled.
        confirmation_generation BIGINT NOT NULL DEFAULT 1,
        -- Snapshot of destinations.observe_only taken at fan-out. Observe-only
        -- copies go out and reconcile entirely on their own, but neither their
        -- receipts (nor their timeouts/dead-letters) ever change the whole
        -- event's acknowledgement standing, are targeted by event-level
        -- requeue, or are counted among the event's for-real copies.
        observe_only BOOLEAN NOT NULL DEFAULT FALSE,
        UNIQUE (destination_id, destination_seq),
        UNIQUE (destination_id, dedupe_key),
        -- superseded: the destination changed location before this copy was
        -- sent; it never goes out and is not retried or backfilled.
        -- dead_lettered: giving up on this one copy after repeated transport
        -- failures or unreconciled requeue cycles; it is parked, never sent
        -- again automatically, and no longer blocks later copies of this
        -- destination.
        CHECK (status IN ('pending', 'in_flight', 'delivered', 'cancelled', 'superseded', 'dead_lettered')),
        CHECK (attempts >= 0),
        CHECK (destination_seq >= 0),
        CHECK (reconcile_state IN ('none', 'awaiting', 'acknowledged', 'receipt_failed', 'timed_out')),
        CHECK (requeue_count >= 0),
        CHECK (consecutive_failures >= 0),
        CHECK (
            (status = 'dead_lettered') = (dead_letter_reason IS NOT NULL)
        ),
        CHECK (
            dead_letter_reason IS NULL OR dead_letter_reason IN (
                'delivery_attempts_exhausted',
                'receipt_timeout_exhausted',
                'receipt_failure_exhausted'
            )
        )
    )
    """,
    # One row per callback receipt sent by a receiver. The disposition records
    # how the receipt was judged at ingestion time, so late/duplicate/orphan
    # receipts stay visible instead of being silently dropped.
    """
    CREATE TABLE IF NOT EXISTS receipts (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        destination_id UUID NOT NULL REFERENCES destinations(id),
        dedupe_key TEXT NOT NULL,
        result TEXT NOT NULL,
        delivery_id UUID REFERENCES deliveries(id),
        disposition TEXT NOT NULL,
        received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CHECK (result IN ('success', 'failure')),
        CHECK (disposition IN ('applied', 'duplicate', 'late', 'orphan', 'premature'))
    )
    """,
    # One row per registered external system allowed to push events in. The
    # secret authenticates that system's events (HMAC signature); rotating it
    # overwrites secret_hmac so old signatures stop matching immediately.
    # disabled_at freezes admission: new events from the source are rejected,
    # while events already accepted keep draining through their queues.
    """
    CREATE TABLE IF NOT EXISTS event_sources (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name TEXT NOT NULL UNIQUE,
        secret TEXT NOT NULL,
        key_rotated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        disabled_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    # One row per inbound event attempt, including every rejected one. A
    # rejected event is visible here with an explicit disposition; it never
    # creates an event/delivery row, so it can never be reported as accepted
    # or sent out. accepted rows link to the event they produced.
    """
    CREATE TABLE IF NOT EXISTS ingestion_attempts (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        -- No FK: a rejected attempt may name a source id that was never
        -- registered, and it must still be recorded as source_unknown.
        source_id UUID,
        source_name TEXT,
        event_id UUID REFERENCES events(id),
        dedupe_key TEXT,
        event_type TEXT,
        signed_at TIMESTAMPTZ,
        disposition TEXT NOT NULL,
        reason TEXT,
        remote_addr TEXT,
        received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CHECK (disposition IN (
            'accepted',
            'unrouted',
            'pending_confirmation',
            'duplicate',
            'source_unknown',
            'source_disabled',
            'bad_signature',
            'stale_timestamp',
            'future_timestamp',
            'invalid_timestamp',
            'invalid_body'
        ))
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS ingestion_attempts_received_idx
        ON ingestion_attempts (received_at DESC, id DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS ingestion_attempts_disposition_idx
        ON ingestion_attempts (disposition, received_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS ingestion_attempts_source_idx
        ON ingestion_attempts (source_id, received_at DESC)
    """,
    # ingestion_attempts.source_id must stay FK-free so attempts from
    # unregistered source ids can be recorded.
    "ALTER TABLE ingestion_attempts DROP CONSTRAINT IF EXISTS ingestion_attempts_source_id_fkey",
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
    # Idempotent upgrades for databases created before receipt reconciliation.
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS reconcile_state TEXT NOT NULL DEFAULT 'none'",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS reconcile_deadline TIMESTAMPTZ",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS reconciled_at TIMESTAMPTZ",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS receipt_result TEXT",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS receipt_id UUID",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS requeue_count INTEGER NOT NULL DEFAULT 0",
    """
    CREATE INDEX IF NOT EXISTS deliveries_reconcile_due_idx
        ON deliveries (reconcile_deadline)
        WHERE reconcile_state = 'awaiting'
    """,
    """
    CREATE INDEX IF NOT EXISTS deliveries_reconcile_state_idx
        ON deliveries (reconcile_state, destination_id)
        WHERE reconcile_state <> 'none'
    """,
    """
    CREATE INDEX IF NOT EXISTS receipts_destination_dedupe_idx
        ON receipts (destination_id, dedupe_key)
    """,
    """
    CREATE INDEX IF NOT EXISTS receipts_disposition_idx
        ON receipts (disposition, received_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS receipts_delivery_idx
        ON receipts (delivery_id)
    """,
    # Idempotent upgrades for databases created before scheduled delivery
    # (not_before) and event cancellation.
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS not_before TIMESTAMPTZ",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS not_before TIMESTAMPTZ",
    # 'cancelled' is the terminal state of copies belonging to a cancelled
    # event. Fresh databases get the widened CHECK from CREATE TABLE above;
    # existing ones need it replaced. The constraint keeps whatever name it
    # was auto-created with, which is "deliveries_status_check" — or
    # "events_status_check" on databases whose deliveries table was renamed
    # from the legacy per-destination "events" table (renames keep
    # constraint names), so drop both before re-adding.
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_status_check",
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS events_status_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
        CHECK (status IN ('pending', 'in_flight', 'delivered', 'cancelled', 'superseded', 'dead_lettered'))
    """,
    # Idempotent upgrades for databases created before inbound source auth.
    # Every newly accepted event belongs to the registered source that pushed
    # it; the column is nullable so pre-upgrade events remain valid.
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS source_id UUID",
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = 'events_source_id_fkey'
        ) THEN
            ALTER TABLE events
                ADD CONSTRAINT events_source_id_fkey
                FOREIGN KEY (source_id) REFERENCES event_sources(id);
        END IF;
    END $$
    """,
    """
    CREATE INDEX IF NOT EXISTS events_source_idx ON events (source_id)
    """,
    # Idempotent upgrades for databases created before destination activation
    # handshakes. The detection, column additions and backfill live in one
    # DO block: only when the columns do not yet exist do we add them, and any
    # destination rows present at that moment predate the handshake feature —
    # they have already been receiving events, so they are treated as
    # confirmed and keep their queue. On later service restarts the columns
    # already exist, so freshly registered 'pending' destinations are never
    # flipped to confirmed by a migration. (Fresh databases get the columns
    # straight from CREATE TABLE above and take no branch here.)
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'destinations' AND column_name = 'confirmation_state'
        ) THEN
            ALTER TABLE destinations
                ADD COLUMN confirmation_state TEXT NOT NULL DEFAULT 'pending';
            ALTER TABLE destinations ADD COLUMN challenge_token TEXT;
            ALTER TABLE destinations ADD COLUMN challenge_expires_at TIMESTAMPTZ;
            ALTER TABLE destinations ADD COLUMN confirmed_at TIMESTAMPTZ;
            ALTER TABLE destinations
                ADD COLUMN confirmation_generation BIGINT NOT NULL DEFAULT 1;
            ALTER TABLE destinations
                ADD COLUMN confirmation_round BIGINT NOT NULL DEFAULT 1;
            ALTER TABLE destinations
                ADD COLUMN confirmation_attempt_count INTEGER NOT NULL DEFAULT 0;
            ALTER TABLE destinations
                ADD COLUMN next_probe_at TIMESTAMPTZ NOT NULL DEFAULT now();

            UPDATE destinations
            SET confirmation_state = 'confirmed',
                confirmed_at = now(),
                challenge_token = NULL,
                challenge_expires_at = NULL;
        END IF;
    END $$
    """,
    "ALTER TABLE destinations DROP CONSTRAINT IF EXISTS destinations_confirmation_state_check",
    """
    ALTER TABLE destinations ADD CONSTRAINT destinations_confirmation_state_check
        CHECK (confirmation_state IN ('pending', 'confirmed'))
    """,
    """
    CREATE INDEX IF NOT EXISTS destinations_probe_idx
        ON destinations (next_probe_at)
        WHERE confirmation_state = 'pending'
    """,
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS confirmation_generation BIGINT NOT NULL DEFAULT 1",
    # Widen the status check on upgraded databases (the constraint may carry
    # either auto-generated name, including one inherited from the legacy
    # events table rename). The dead-letter migration below widens it once
    # more to include 'dead_lettered'; keep this set aligned so re-running
    # init_db on a database that already has dead-lettered rows validates.
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_status_check",
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS events_status_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
        CHECK (status IN ('pending', 'in_flight', 'delivered', 'cancelled', 'superseded', 'dead_lettered'))
    """,
    # Widen the ingestion disposition check to include pending_confirmation.
    "ALTER TABLE ingestion_attempts DROP CONSTRAINT IF EXISTS ingestion_attempts_disposition_check",
    """
    ALTER TABLE ingestion_attempts ADD CONSTRAINT ingestion_attempts_disposition_check
        CHECK (disposition IN (
            'accepted', 'unrouted', 'pending_confirmation', 'duplicate',
            'source_unknown', 'source_disabled', 'bad_signature',
            'stale_timestamp', 'future_timestamp', 'invalid_timestamp',
            'invalid_body'
        ))
    """,
    # Idempotent upgrades for the dead-letter area. A copy that keeps failing
    # transport past its own attempt budget, or whose receipt keeps not
    # matching after the allowed requeue cycles, becomes terminally
    # 'dead_lettered': it is kept for inspection (which copy, which address,
    # why, when), is never claimed by the worker again, and — because the
    # worker only takes the smallest pending/in_flight seq — no longer blocks
    # later copies of the same destination.
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS consecutive_failures INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS dead_letter_reason TEXT",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS dead_lettered_at TIMESTAMPTZ",
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_status_check",
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS events_status_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
        CHECK (status IN ('pending', 'in_flight', 'delivered', 'cancelled', 'superseded', 'dead_lettered'))
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_consecutive_failures_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_consecutive_failures_check
        CHECK (consecutive_failures >= 0)
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_dead_letter_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_dead_letter_check
        CHECK (
            (status = 'dead_lettered') = (dead_letter_reason IS NOT NULL)
        )
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_dead_letter_reason_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_dead_letter_reason_check
        CHECK (
            dead_letter_reason IS NULL OR dead_letter_reason IN (
                'delivery_attempts_exhausted',
                'receipt_timeout_exhausted',
                'receipt_failure_exhausted'
            )
        )
    """,
    """
    CREATE INDEX IF NOT EXISTS deliveries_dead_letter_idx
        ON deliveries (dead_lettered_at DESC, id DESC)
        WHERE status = 'dead_lettered'
    """,
    # Idempotent upgrades for the observe-only ("shadow") subscriber mode.
    # A shadow destination still receives its own copy of every subscribed
    # event and runs its own delivery/retry/quarantine/dead-letter lifecycle,
    # but its copies never count toward the whole event's acknowledgement, are
    # skipped by event-level unreconciled requeue and are reported separately.
    # Existing destinations/copies predate the feature and default to for-real.
    "ALTER TABLE destinations ADD COLUMN IF NOT EXISTS observe_only BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS observe_only BOOLEAN NOT NULL DEFAULT FALSE",
    """
    CREATE INDEX IF NOT EXISTS deliveries_event_observe_idx
        ON deliveries (event_id)
        WHERE observe_only = FALSE
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
