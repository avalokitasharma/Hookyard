CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    tenant_id UUID NOT NULL,

    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,

    occurred_at TIMESTAMPTZ NULL,
    source VARCHAR(255) NULL,
    metadata JSONB NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_events_tenant_created
    ON events (tenant_id, created_at DESC);

CREATE INDEX idx_events_tenant_type_created
    ON events (tenant_id, event_type, created_at DESC);


CREATE TABLE event_idempotency (
    tenant_id UUID NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,

    event_id UUID NOT NULL REFERENCES events(id),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (tenant_id, idempotency_key)
);


CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,

    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    published_at TIMESTAMPTZ NULL,

    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL
);

CREATE INDEX idx_outbox_unpublished
    ON outbox (created_at)
    WHERE published_at IS NULL;