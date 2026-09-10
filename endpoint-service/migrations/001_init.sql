CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE retry_policies (
    retry_policy_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    backoff_type VARCHAR(32) NOT NULL CHECK (backoff_type IN ('fixed', 'exponential')),
    initial_delay_ms INTEGER NOT NULL CHECK (initial_delay_ms BETWEEN 100 AND 86400000),
    max_delay_ms INTEGER NOT NULL CHECK (max_delay_ms >= initial_delay_ms AND max_delay_ms <= 604800000),
    jitter_percent INTEGER NOT NULL CHECK (jitter_percent BETWEEN 0 AND 100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, name),
    UNIQUE (retry_policy_id, tenant_id)
);

CREATE INDEX idx_retry_policies_tenant ON retry_policies(tenant_id);


CREATE TABLE endpoints (
    endpoint_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    url TEXT NOT NULL,
    secret_encrypted BYTEA NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('ACTIVE', 'DELETED')),
    connect_timeout_ms INTEGER NOT NULL DEFAULT 5000 CHECK (connect_timeout_ms BETWEEN 100 AND 120000),
    request_timeout_ms INTEGER NOT NULL DEFAULT 10000 CHECK (request_timeout_ms BETWEEN 100 AND 120000),
    retry_policy_id UUID NOT NULL,
    CONSTRAINT fk_endpoint_retry_policy_tenant FOREIGN KEY (retry_policy_id, tenant_id) REFERENCES retry_policies(retry_policy_id, tenant_id),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ NULL
);

CREATE UNIQUE INDEX idx_endpoints_active_name ON endpoints(tenant_id, lower(name)) WHERE deleted_at IS NULL;
CREATE INDEX idx_endpoints_tenant_status ON endpoints(tenant_id, status, endpoint_id);
CREATE INDEX idx_endpoints_updated ON endpoints(tenant_id, updated_at DESC);

CREATE TABLE endpoint_subscriptions (
    endpoint_id UUID NOT NULL REFERENCES endpoints(endpoint_id) ON DELETE CASCADE,
    event_type VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (endpoint_id, event_type)
);

CREATE INDEX idx_endpoint_subscriptions_event ON endpoint_subscriptions(event_type, endpoint_id);

CREATE TABLE outbox_messages (
    id UUID PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    message_key VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_by VARCHAR(255) NULL,
    locked_until TIMESTAMPTZ NULL,
    published_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_outbox_ready ON outbox_messages(available_at, created_at, id)
WHERE published_at IS NULL;

CREATE INDEX idx_outbox_locked ON outbox_messages(locked_until)
WHERE published_at IS NULL;
