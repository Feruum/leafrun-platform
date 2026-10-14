CREATE TABLE IF NOT EXISTS platform_event_outbox (
    event_id text PRIMARY KEY,
    subject text NOT NULL,
    encoded_envelope bytea NOT NULL,
    payload_hash bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_token text,
    lease_until timestamptz,
    sent_at timestamptz,
    last_error text,
    CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);
CREATE INDEX IF NOT EXISTS platform_event_outbox_ready_idx
    ON platform_event_outbox (available_at, created_at) WHERE sent_at IS NULL;

CREATE TABLE IF NOT EXISTS platform_event_inbox (
    consumer_name text NOT NULL,
    event_id text NOT NULL,
    encoded_envelope bytea NOT NULL,
    payload_hash bytea NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','applied','quarantined','resolved')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    replay_count integer NOT NULL DEFAULT 0 CHECK (replay_count >= 0),
    replay_requested boolean NOT NULL DEFAULT false,
    replay_available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_token text,
    lease_until timestamptz,
    last_error text,
    resolution_reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    applied_at timestamptz,
    quarantined_at timestamptz,
    resolved_at timestamptz,
    PRIMARY KEY (consumer_name, event_id),
    CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);
CREATE INDEX IF NOT EXISTS platform_event_inbox_lease_idx
    ON platform_event_inbox (consumer_name, lease_until) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS platform_event_inbox_replay_idx
    ON platform_event_inbox(consumer_name,replay_available_at) WHERE replay_requested;

CREATE TABLE IF NOT EXISTS platform_event_history (
    id bigserial PRIMARY KEY,
    consumer_name text NOT NULL,
    event_id text NOT NULL,
    action text NOT NULL,
    attempt integer NOT NULL DEFAULT 0,
    details text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS platform_event_history_lookup_idx
    ON platform_event_history (consumer_name, event_id, id);
