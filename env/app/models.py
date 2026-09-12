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
        -- Operator-marked "not receiving" window [paused_from, paused_until).
        -- While now() is inside the window the worker never claims this
        -- destination's copies: they wait in their original queue positions,
        -- no attempt is made (so the pause can never be charged as
        -- consecutive failures or trigger isolation), no reconcile countdown
        -- runs for them (it only starts when a copy is really sent), and
        -- other destinations subscribed to the same types keep draining.
        -- paused_until NULL with paused_from set means "not receiving until
        -- explicitly resumed"; both NULL means no window.
        paused_from TIMESTAMPTZ,
        paused_until TIMESTAMPTZ,
        CHECK (status IN ('active', 'isolated')),
        CHECK (failure_count >= 0),
        CHECK (next_event_seq >= 0),
        CHECK (confirmation_state IN ('pending', 'confirmed')),
        CHECK (confirmation_generation >= 1),
        CHECK (confirmation_round >= 1),
        CHECK (confirmation_attempt_count >= 0),
        CONSTRAINT destinations_pause_window_check CHECK (
            (paused_until IS NULL OR paused_from IS NOT NULL)
            AND (paused_until IS NULL OR paused_until > paused_from)
        )
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
    # Per-event-type acknowledgement threshold ("认完门槛"): at most one row
    # per event type. A non-null threshold means an event of this type is
    # whole-event acknowledged once that many FOR-REAL copies carry a matching
    # success receipt; observe-only ("shadow") copies never count. No row (or a
    # null threshold) keeps the default rule: every fanned-out for-real copy
    # must be acknowledged. The effective required number is capped at the
    # number of for-real confirmed subscribers present at ingest time and is
    # snapshotted onto the event row, so later configuration/subscription
    # changes never rewrite the standing of an already-accepted event.
    """
    CREATE TABLE IF NOT EXISTS event_type_ack_thresholds (
        event_type TEXT PRIMARY KEY,
        ack_threshold INTEGER NOT NULL CHECK (ack_threshold >= 1),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
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
        -- Latest transport time chosen at submission ("最晚送到"). Null means
        -- no such promise; deliveries then proceed normally. The value is
        -- snapshotted onto undelivered copies, and changing it later only
        -- rewrites copies that have not completed transport.
        deliver_by TIMESTAMPTZ,
        cancelled_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        -- Optional short text a gated event's preview is allowed to show.
        -- The real content stays in payload and only goes out with the body,
        -- after this destination nods. Null on ordinary events.
        preview_payload JSONB,
        -- Number of for-real copies whose success receipts are required to
        -- mark the whole event acknowledged. Snapshot taken at fan-out time:
        -- all for-real confirmed subscribers by default, or the configured
        -- type threshold capped at that subscriber count. Null on legacy rows
        -- (treated as "every live for-real copy must acknowledge").
        required_ack_count INTEGER,
        CHECK (required_ack_count IS NULL OR required_ack_count >= 0)
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
        -- Snapshot of events.deliver_by for this copy. It is copied/rewritten
        -- only while transport has not completed: once delivered_at is set a
        -- later deadline change can never touch the copy. Null copies ignore
        -- the cutoff and keep their ordinary delivery lifecycle.
        deliver_by TIMESTAMPTZ,
        last_error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        delivered_at TIMESTAMPTZ,
        -- When an undelivered copy was closed because its deliver_by cutoff
        -- had passed. Terminal deadline_expired rows never go out and must be
        -- distinguishable from delivered, filtered, and unrouted outcomes.
        deliver_by_expired_at TIMESTAMPTZ,
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
        -- Snapshot of the subscription condition this copy was fanned out
        -- under (destination_subscriptions.filter_spec at fan-out). NULL means
        -- the subscription had no condition, so every body was delivered; a
        -- non-null condition matched the body (a non-matching body creates no
        -- row at all — see subscription_filter_evaluations). The snapshot is
        -- never rewritten, so later condition edits only affect later events.
        filter_spec JSONB,
        -- Preview-consent gate ("预告 + 点头才给正文"). Gated events fan out
        -- into two copies per subscriber: phase 'preview' (the notice, queued
        -- first, no real payload, no receipt reconciliation) and phase 'body'
        -- (the content, queued behind it). A body is claimable only when its
        -- own preview is delivered and release_state = 'released'. Ordinary
        -- (non-gated) events and corrections are a single phase 'body' copy
        -- with a null release_state and behave exactly as before.
        phase TEXT NOT NULL DEFAULT 'body',
        release_state TEXT,
        -- preview copy points at its body (preview_delivery_id); a gated body
        -- points back at its preview (body_delivery_id). The self-reference is
        -- kept loose (no FK) so the pair inserts in either order inside one
        -- transaction.
        preview_delivery_id UUID,
        body_delivery_id UUID,
        -- From when the address may decide, and until when. The deadline is
        -- written when the preview really completes transport, snapshotting
        -- delivered_at + the per-event consent timeout; after it, a missing
        -- nod voids the body terminally and a late nod is never applied.
        consent_deadline TIMESTAMPTZ,
        consent_timeout_seconds BIGINT,
        released_at TIMESTAMPTZ,
        voided_at TIMESTAMPTZ,
        void_reason TEXT,
        -- Relay chain ("接力"): when this copy is a station of an
        -- event-type relay chain, relay_chain_id is the chain VERSION the
        -- event set out under and relay_station_no is this station's
        -- position in it (1-based). Both are snapshotted onto the copy at
        -- fan-out: re-defining the chain order only applies to events
        -- accepted afterwards; an event already on its way keeps walking
        -- exactly the stations it set out with. Null on every ordinary
        -- (non-relay) copy and on corrections.
        relay_chain_id UUID,
        relay_station_no INTEGER,
        -- Set only on stations permanently skipped ("不再补") because an
        -- earlier station of the same relay run stopped; relay_skipped_at
        -- says when and relay_skip_reason why (copied from the triggering
        -- station's stop reason). relay_stopped_by_delivery_id points at the
        -- station that triggered the cascade. A skipped row is terminal,
        -- never sent and never backfilled, so it can never be written as
        -- "sent to a later station".
        relay_skipped_at TIMESTAMPTZ,
        relay_skip_reason TEXT,
        relay_stopped_by_delivery_id UUID,
        UNIQUE (destination_id, destination_seq),
        UNIQUE (destination_id, dedupe_key),
        -- superseded: the destination changed location before this copy was
        -- sent; it never goes out and is not retried or backfilled.
        -- dead_lettered: giving up on this one copy after repeated transport
        -- failures or unreconciled requeue cycles; it is parked, never sent
        -- again automatically, and no longer blocks later copies of this
        -- destination.
        -- failed: a correction copy whose send attempt failed. Terminal for
        -- that copy only: corrections are not retried in place, the failure
        -- is never charged to the destination's consecutive-failure tally
        -- (no isolation), and the copy no longer blocks later copies.
        -- release_denied / release_expired / release_voided: terminal states
        -- of a gated body that never went out — the address answered "no",
        -- did not nod before the agreed deadline, or the not-yet-out body was
        -- voided manually / because its preview never made it through.
        -- deadline_expired: still undelivered when its deliver_by cutoff
        -- passed; terminal, it never goes out and is not marked delivered.
        CHECK (status IN ('pending', 'in_flight', 'delivered', 'cancelled', 'superseded', 'dead_lettered', 'failed',
                          'release_denied', 'release_expired', 'release_voided', 'relay_skipped',
                          'deadline_expired')),
        CHECK (phase IN ('preview', 'body')),
        CHECK (
            -- Relay-chain snapshots come as a pair and only ever appear on
            -- body copies (relay types are never preview-gated); station
            -- numbers start at 1.
            (relay_chain_id IS NULL) = (relay_station_no IS NULL)
        ),
        CHECK (relay_station_no IS NULL OR relay_station_no >= 1),
        CHECK (
            -- The skip audit columns exist only together and only on a
            -- terminal relay_skipped row.
            (status = 'relay_skipped') = (relay_skip_reason IS NOT NULL)
        ),
        CHECK (
            relay_skip_reason IS NULL OR relay_skip_reason IN (
                -- The previous station exhausted its transport retries /
                -- dead-lettered, or dead-lettered after receipt cycles.
                'delivery_attempts_exhausted',
                'receipt_timeout_exhausted',
                'receipt_failure_exhausted',
                -- The previous station answered with a failure receipt while
                -- still inside its requeue budget.
                'receipt_failed',
                -- Reconciliation timed out for the previous station while it
                -- still had requeue budget.
                'receipt_timeout',
                -- The previous station's destination changed location before
                -- the copy could be sent.
                'superseded',
                -- The previous station missed its promised deliver-by cutoff.
                'deliver_by_expired'
            )
        ),
        CHECK (
            release_state IS NULL
            OR release_state IN (
                'held', 'released', 'release_denied',
                'release_expired', 'release_voided', 'deadline_expired'
            )
        ),
        -- Only preview rows self-mark body_delivery_id; bodies leave it null.
        -- A preview points at its body, a body back at its preview.
        CHECK (
            (phase = 'preview' AND body_delivery_id IS NOT NULL
                 AND preview_delivery_id IS NULL)
            OR
            (phase = 'body' AND body_delivery_id IS NULL)
        ),
        CHECK (
            void_reason IS NULL OR void_reason IN (
                'manual', 'preview_dead_lettered', 'preview_superseded',
                'preview_deadline_expired', 'deliver_by_expired'
            )
        ),
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

    # --- Preview-consent gate ("预告 + 点头才给正文"), idempotent -----
    # Placed after the deliveries/events tables exist (a fresh database
    # already declares these columns/constraints inline, so every
    # statement here is IF NOT EXISTS / drop-and-readd and is a no-op there;
    # upgraded databases gain the new tables, columns and checks here.
    # Copies of gated events come in two phases:
    #  * 'preview' — the notice, queued first. It carries no business payload,
    #    only preview_payload (which may be empty), and never enters receipt
    #    reconciliation.
    #  * 'body'    — the actual content, queued behind the preview. It stays
    #    pending with release_state 'held' until this same destination's own
    #    preview is delivered and that destination approves before the
    #    consent deadline; the worker claim gate refuses every other body.
    # Ordinary (non-gated) events and corrections have phase 'body' and a null
    # release_state, so they behave exactly as before.
    #
    # release_state lifecycle of a gated body:
    #   held -> released   (this destination nodded yes, in time)
    #   held -> release_denied    (this destination answered "no")
    #   held -> release_expired   (deadline passed with no nod; a later nod
    #                              can never revive it)
    #   held -> release_voided    (manual void of a not-yet-out body, or the
    #                              preview itself never made it through)
    # All four non-held states are terminal: such a body never goes out.
    """
    ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS phase TEXT NOT NULL DEFAULT 'body'
    """,
    """
    ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS release_state TEXT
    """,
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS preview_delivery_id UUID",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS body_delivery_id UUID",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS consent_deadline TIMESTAMPTZ",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS consent_timeout_seconds BIGINT",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS released_at TIMESTAMPTZ",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS voided_at TIMESTAMPTZ",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS void_reason TEXT",
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_phase_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_phase_check
        CHECK (phase IN ('preview', 'body'))
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_release_state_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_release_state_check
        CHECK (
            release_state IS NULL
            OR release_state IN (
                'held', 'released', 'release_denied',
                'release_expired', 'release_voided', 'deadline_expired'
            )
        )
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_release_shape_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_release_shape_check
        CHECK (
            -- preview rows point at their body via body_delivery_id; body
            -- rows point back at their preview via preview_delivery_id.
            (phase = 'preview' AND body_delivery_id IS NOT NULL
                 AND preview_delivery_id IS NULL)
            OR
            (phase = 'body' AND body_delivery_id IS NULL)
        )
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_void_reason_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_void_reason_check
        CHECK (
            void_reason IS NULL OR void_reason IN (
                'manual', 'preview_dead_lettered', 'preview_superseded',
                'preview_deadline_expired', 'deliver_by_expired'
            )
        )
    """,
    """
    CREATE INDEX IF NOT EXISTS deliveries_release_due_idx
        ON deliveries (consent_deadline)
        WHERE phase = 'body'
          AND release_state = 'held'
          AND status = 'pending'
    """,
    # Optional short text for a gated event: what the notice is allowed to
    # show. The real content stays in payload and only goes out with the body.
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS preview_payload JSONB",
    # Preview-consent gate ("预告 + 点头才给正文"). One row per event type that
    # is *gated*: instead of one copy per subscriber, an event of a gated type
    # fans out into two sequential copies per subscriber — a preview first and,
    # only after that address itself nods yes before the agreed deadline, the
    # body. The deadline (preview_consent_timeout_seconds) is snapshotted onto
    # every pair at fan-out (deliveries.consent_timeout_seconds), so changing
    # or removing the policy here only ever affects later events. A gated type
    # with no row behaves like every ordinary type (body straight away).
    """
    CREATE TABLE IF NOT EXISTS event_type_preview_policies (
        event_type TEXT PRIMARY KEY,
        consent_timeout_seconds BIGINT NOT NULL CHECK (consent_timeout_seconds >= 1),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    # One row per consent decision ("点头/不要") returned by a destination
    # against a gated event's preview. This is the audit trail: it records
    # *whose* answer it was (destination + event — never another address's),
    # when it arrived and how it was judged. A nod counts exactly once: the
    # row key is (delivery_id, decision) plus a unique delivery_id for the
    # applied answer, so a second nod on the same gate is logged 'duplicate'
    # and never re-applies. A row whose delivery_id stays null is an 'orphan'
    # (no outstanding gate for that address/event), kept for inspection.
    """
    CREATE TABLE IF NOT EXISTS release_gate_decisions (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        destination_id UUID NOT NULL,
        event_id UUID,
        delivery_id UUID REFERENCES deliveries(id),
        decision TEXT NOT NULL,
        disposition TEXT NOT NULL,
        reason TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CHECK (decision IN ('approve', 'deny')),
        CHECK (disposition IN (
            'released', 'denied', 'duplicate', 'late_ignored',
            'conflict', 'preview_not_delivered', 'not_gated', 'orphan'
        ))
    )
    """,
    # Exactly one effective answer (released / denied) per gated body. Audit
    # rows for duplicate/conflict/late/pre-delivery/orphan decisions carry a
    # null delivery_id and are outside this index.
    """
    CREATE UNIQUE INDEX IF NOT EXISTS release_gate_decisions_effective_uniq
        ON release_gate_decisions (delivery_id)
        WHERE delivery_id IS NOT NULL
    """,
    """
    CREATE INDEX IF NOT EXISTS release_gate_decisions_event_idx
        ON release_gate_decisions (event_id, created_at DESC, id DESC)
        WHERE event_id IS NOT NULL
    """,
    """
    CREATE INDEX IF NOT EXISTS release_gate_decisions_destination_idx
        ON release_gate_decisions (destination_id, created_at DESC, id DESC)
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
            -- accepted/stored, but every confirmed subscriber's own
            -- subscription condition withheld its copy (no deliveries); the
            -- per-address judgements are in subscription_filter_evaluations.
            'filtered',
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
        -- Optional per-(destination, event type) condition over the event
        -- body. NULL means "receive every event of this type"; a non-null
        -- condition is evaluated once at ingest against the exact body, and a
        -- non-matching body creates no delivery at all (its outcome is
        -- recorded in subscription_filter_evaluations). Conditions are
        -- evaluated at fan-out time, so replacing one here only ever affects
        -- later events; copies already fanned out keep the snapshot on the
        -- delivery row (deliveries.filter_spec).
        filter_spec JSONB,
        PRIMARY KEY (destination_id, event_type)
    )
    """,
    # One row per fan-out evaluation of a destination's own subscription
    # condition (only rows for subscriptions that actually carry a condition
    # appear here). matched=true: the condition held and a delivery was
    # created; matched=false: the body did not satisfy THIS destination's own
    # condition, so no delivery was created and nothing was sent to it. This
    # is deliberately separate from an event being "unrouted" (nobody
    # subscribes at all) and from "pending confirmation" (the subscriber has
    # not finished the handshake; the condition is not even evaluated). The
    # exact condition is snapshotted, so re-inspecting the row shows precisely
    # what was judged; the pure evaluator gives the same answer again.
    """
    CREATE TABLE IF NOT EXISTS subscription_filter_evaluations (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        event_id UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
        destination_id UUID NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
        event_type TEXT NOT NULL,
        matched BOOLEAN NOT NULL,
        filter_spec JSONB NOT NULL,
        observe_only BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        -- At most one judgement per (event, destination): the same body is
        -- evaluated exactly once against the same condition snapshot.
        UNIQUE (event_id, destination_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS subscription_filter_evaluations_event_idx
        ON subscription_filter_evaluations (event_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS subscription_filter_evaluations_destination_idx
        ON subscription_filter_evaluations (destination_id, created_at DESC, id DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS subscription_filter_evaluations_unmatched_idx
        ON subscription_filter_evaluations (created_at DESC, id DESC)
        WHERE matched = FALSE
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
        CHECK (status IN ('pending', 'in_flight', 'delivered', 'cancelled', 'superseded', 'dead_lettered', 'failed',
                          'release_denied', 'release_expired', 'release_voided', 'relay_skipped',
                          'deadline_expired'))
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
        CHECK (status IN ('pending', 'in_flight', 'delivered', 'cancelled', 'superseded', 'dead_lettered', 'failed',
                          'release_denied', 'release_expired', 'release_voided', 'relay_skipped',
                          'deadline_expired'))
    """,
    # Widen the ingestion disposition check to include pending_confirmation.
    "ALTER TABLE ingestion_attempts DROP CONSTRAINT IF EXISTS ingestion_attempts_disposition_check",
    """
    ALTER TABLE ingestion_attempts ADD CONSTRAINT ingestion_attempts_disposition_check
        CHECK (disposition IN (
            'accepted', 'unrouted', 'pending_confirmation', 'filtered', 'duplicate',
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
        CHECK (status IN ('pending', 'in_flight', 'delivered', 'cancelled', 'superseded', 'dead_lettered', 'failed',
                          'release_denied', 'release_expired', 'release_voided', 'relay_skipped',
                          'deadline_expired'))
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
    # Idempotent upgrades for per-event-type acknowledgement thresholds
    # ("认完门槛"). The type table is created above for fresh databases; the
    # events column snapshots how many for-real success receipts are required
    # to mark each ingested event acknowledged (null = every live for-real
    # copy). Legacy events keep the old all-for-real rule.
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS required_ack_count INTEGER",
    "ALTER TABLE events DROP CONSTRAINT IF EXISTS events_required_ack_count_check",
    """
    ALTER TABLE events ADD CONSTRAINT events_required_ack_count_check
        CHECK (required_ack_count IS NULL OR required_ack_count >= 0)
    """,
    # Idempotent upgrades for per-destination, per-event-type subscription
    # filters ("订阅条件"). A subscription may carry a JSON condition over the
    # event body: at fan-out the condition is evaluated once by a pure
    # evaluator, a matching body is fanned out (with the condition snapshotted
    # onto deliveries.filter_spec) and a non-matching body creates no delivery
    # at all — its judgement is recorded in
    # subscription_filter_evaluations, so it never reads as "sent" and stays
    # distinct from an event with no subscribers ('unrouted'). Existing
    # subscriptions have no condition (NULL) and behave exactly as before.
    "ALTER TABLE destination_subscriptions ADD COLUMN IF NOT EXISTS filter_spec JSONB",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS filter_spec JSONB",
    "ALTER TABLE subscription_filter_evaluations ADD COLUMN IF NOT EXISTS observe_only BOOLEAN NOT NULL DEFAULT FALSE",
    # Widen the ingestion disposition check to include 'filtered': accepted
    # and stored, but every confirmed subscriber's own subscription condition
    # withheld its copy.
    "ALTER TABLE ingestion_attempts DROP CONSTRAINT IF EXISTS ingestion_attempts_disposition_check",
    """
    ALTER TABLE ingestion_attempts ADD CONSTRAINT ingestion_attempts_disposition_check
        CHECK (disposition IN (
            'accepted', 'unrouted', 'pending_confirmation', 'filtered', 'duplicate',
            'source_unknown', 'source_disabled', 'bad_signature',
            'stale_timestamp', 'future_timestamp', 'invalid_timestamp',
            'invalid_body'
        ))
    """,
    # window is a claim-time gate only: while it is in effect the destination
    # is never picked, its copies keep their queue positions, nothing is
    # attempted (nothing counts toward failure isolation) and no reconcile
    # countdown runs. Existing destinations have no window (both columns
    # null) and behave exactly as before.
    "ALTER TABLE destinations ADD COLUMN IF NOT EXISTS paused_from TIMESTAMPTZ",
    "ALTER TABLE destinations ADD COLUMN IF NOT EXISTS paused_until TIMESTAMPTZ",
    "ALTER TABLE destinations DROP CONSTRAINT IF EXISTS destinations_pause_window_check",
    """
    ALTER TABLE destinations ADD CONSTRAINT destinations_pause_window_check
        CHECK (
            (paused_until IS NULL OR paused_from IS NOT NULL)
            AND (paused_until IS NULL OR paused_until > paused_from)
        )
    """,
    # Idempotent upgrades for event corrections ("补一笔更正"). A correction is
    # itself an event row whose corrects_event_id points at the original
    # event; its copies are additional deliveries fanned out only to the
    # destinations the original event was really delivered to. The original
    # event's own rows are never rewritten by a correction.
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS corrects_event_id UUID",
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = 'events_corrects_event_id_fkey'
        ) THEN
            ALTER TABLE events
                ADD CONSTRAINT events_corrects_event_id_fkey
                FOREIGN KEY (corrects_event_id) REFERENCES events(id);
        END IF;
    END $$
    """,
    """
    CREATE INDEX IF NOT EXISTS events_corrects_event_idx
        ON events (corrects_event_id)
        WHERE corrects_event_id IS NOT NULL
    """,
    # A correction copy that fails its send attempt ends in the terminal
    # 'failed' state (see the deliveries table comment above). Gated bodies
    # that never go out end in release_denied / release_expired /
    # release_voided. Widen the status check everywhere it is re-asserted so
    # re-running init_db on a database that already has such rows validates.
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_status_check",
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS events_status_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
        CHECK (status IN ('pending', 'in_flight', 'delivered', 'cancelled', 'superseded', 'dead_lettered', 'failed',
                          'release_denied', 'release_expired', 'release_voided', 'relay_skipped',
                          'deadline_expired'))
    """,
    # Idempotent upgrades for per-event-type relay chains ("接力"). One
    # ACTIVE chain per event type; stations are an ordered list of
    # destinations (no destination may occupy two stations of one chain).
    # Events of that type accepted AFTER the chain is defined go only to its
    # stations, one station at a time, each later station created pending and
    # withheld until the previous station carries a matching success receipt.
    # Re-defining a chain never mutates a version already in flight: every
    # PUT creates a new version row, and each delivery snapshots
    # (relay_chain_id, relay_station_no) at fan-out, so an event already
    # walking a chain keeps the stations it set out with while only later
    # accepted events follow the new order.
    """
    CREATE TABLE IF NOT EXISTS relay_chains (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        event_type TEXT NOT NULL,
        version INTEGER NOT NULL,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (event_type, version)
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS relay_chains_active_uniq
        ON relay_chains (event_type) WHERE active = TRUE
    """,
    """
    CREATE TABLE IF NOT EXISTS relay_chain_stations (
        chain_id UUID NOT NULL REFERENCES relay_chains(id),
        station_no INTEGER NOT NULL,
        destination_id UUID NOT NULL REFERENCES destinations(id),
        PRIMARY KEY (chain_id, station_no),
        -- The same destination can never occupy two stations of one chain.
        UNIQUE (chain_id, destination_id),
        CHECK (station_no >= 1)
    )
    """,
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS relay_chain_id UUID",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS relay_station_no INTEGER",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS relay_skipped_at TIMESTAMPTZ",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS relay_skip_reason TEXT",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS relay_stopped_by_delivery_id UUID",
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = 'deliveries_relay_chain_fkey'
        ) THEN
            ALTER TABLE deliveries
                ADD CONSTRAINT deliveries_relay_chain_fkey
                FOREIGN KEY (relay_chain_id) REFERENCES relay_chains(id);
        END IF;
    END $$
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_relay_snapshot_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_relay_snapshot_check
        CHECK ((relay_chain_id IS NULL) = (relay_station_no IS NULL))
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_relay_station_no_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_relay_station_no_check
        CHECK (relay_station_no IS NULL OR relay_station_no >= 1)
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_relay_skip_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_relay_skip_check
        CHECK (
            (status = 'relay_skipped') = (relay_skip_reason IS NOT NULL)
        )
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_relay_skip_reason_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_relay_skip_reason_check
        CHECK (
            relay_skip_reason IS NULL OR relay_skip_reason IN (
                'delivery_attempts_exhausted',
                'receipt_timeout_exhausted',
                'receipt_failure_exhausted',
                'receipt_failed',
                'receipt_timeout',
                'superseded',
                'deliver_by_expired'
            )
        )
    """,
    # Worker claim gate: relay stations are selected but their pending rows
    # only become deliverable once the previous station of the same run is
    # acknowledged; relay_skipped rows are terminal and never selected.
    """
    CREATE INDEX IF NOT EXISTS deliveries_relay_gate_idx
        ON deliveries (event_id, relay_chain_id, relay_station_no)
        WHERE relay_chain_id IS NOT NULL
    """,
    # Idempotent upgrades for an event-wide latest delivery promise
    # ("最晚送到"). The event timestamp is copied onto copies while they are
    # still undelivered; a later change rewrites only status='pending' and
    # in-flight copies. The sweep moves still-pending copies with a missed
    # cutoff to the terminal deadline_expired state; delivered copies remain
    # exactly as they were and are never recalled.
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS deliver_by TIMESTAMPTZ",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS deliver_by TIMESTAMPTZ",
    "ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS deliver_by_expired_at TIMESTAMPTZ",
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_status_check",
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS events_status_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
        CHECK (status IN ('pending', 'in_flight', 'delivered', 'cancelled', 'superseded', 'dead_lettered', 'failed',
                          'release_denied', 'release_expired', 'release_voided', 'relay_skipped',
                          'deadline_expired'))
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_release_state_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_release_state_check
        CHECK (
            release_state IS NULL
            OR release_state IN (
                'held', 'released', 'release_denied',
                'release_expired', 'release_voided', 'deadline_expired'
            )
        )
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_void_reason_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_void_reason_check
        CHECK (
            void_reason IS NULL OR void_reason IN (
                'manual', 'preview_dead_lettered', 'preview_superseded',
                'preview_deadline_expired', 'deliver_by_expired'
            )
        )
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_relay_skip_reason_check",
    """
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_relay_skip_reason_check
        CHECK (
            relay_skip_reason IS NULL OR relay_skip_reason IN (
                'delivery_attempts_exhausted',
                'receipt_timeout_exhausted',
                'receipt_failure_exhausted',
                'receipt_failed',
                'receipt_timeout',
                'superseded',
                'deliver_by_expired'
            )
        )
    """,
    """
    CREATE INDEX IF NOT EXISTS deliveries_deliver_by_due_idx
        ON deliveries (deliver_by)
        WHERE status = 'pending'
          AND deliver_by IS NOT NULL
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
