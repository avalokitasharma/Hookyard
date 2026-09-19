CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE endpoint_projection (
    endpoint_id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    url TEXT NOT NULL,
    secret_encrypted BYTEA NOT NULL,
    status VARCHAR(32) NOT NULL,
    connect_timeout_ms INTEGER NOT NULL DEFAULT 5000,
    request_timeout_ms INTEGER NOT NULL DEFAULT 10000,
    retry_policy JSONB NOT NULL,
    version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_endpoint_projection_tenant
    ON endpoint_projection (tenant_id);

CREATE TABLE endpoint_subscription_projection (
    endpoint_id UUID NOT NULL,
    tenant_id UUID NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    PRIMARY KEY (endpoint_id, event_type)
);

CREATE INDEX idx_subscription_projection_event_type
    ON endpoint_subscription_projection (tenant_id, event_type, endpoint_id);

CREATE TABLE deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    event_id UUID NOT NULL,
    endpoint_id UUID NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ NULL,
    last_status_code INTEGER NULL,
    last_error_code VARCHAR(100) NULL,
    last_error_message TEXT NULL,
    lease_owner VARCHAR(255) NULL,
    lease_expires_at TIMESTAMPTZ NULL,
    replay_of_delivery_id UUID NULL,
    endpoint_url TEXT NOT NULL,
    endpoint_secret_encrypted BYTEA NOT NULL,
    connect_timeout_ms INTEGER NOT NULL,
    request_timeout_ms INTEGER NOT NULL,
    retry_policy JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL
);

CREATE INDEX idx_deliveries_ready
    ON deliveries (next_attempt_at)
    WHERE status IN ('PENDING', 'RETRYING');

CREATE INDEX idx_deliveries_lease_expired
    ON deliveries (lease_expires_at)
    WHERE status = 'IN_PROGRESS';

CREATE INDEX idx_deliveries_tenant_created
    ON deliveries (tenant_id, created_at DESC);

CREATE INDEX idx_deliveries_endpoint_created
    ON deliveries (endpoint_id, created_at DESC);

CREATE INDEX idx_deliveries_event
    ON deliveries (event_id);

CREATE TABLE delivery_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id UUID NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(32) NOT NULL,
    http_status_code INTEGER NULL,
    latency_ms INTEGER NULL,
    error_code VARCHAR(100) NULL,
    error_message TEXT NULL,
    response_headers JSONB NULL,
    response_body TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (delivery_id, attempt_number)
);


CREATE INDEX idx_delivery_attempts_delivery
    ON delivery_attempts (delivery_id, attempt_number);

CREATE TABLE processed_messages (
    consumer_name VARCHAR(100) NOT NULL,
    message_id VARCHAR(255) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_name, message_id)
);
