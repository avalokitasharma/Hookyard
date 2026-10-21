CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE tenants (
    tenant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 255),
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','SUSPENDED','DELETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX tenants_name_active_uq ON tenants (lower(name)) WHERE deleted_at IS NULL;

-- Only a hash of the secret is persisted. The complete key is shown once at creation.
CREATE TABLE api_keys (
    key_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id),
    name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 120),
    key_prefix TEXT NOT NULL UNIQUE,
    secret_hash BYTEA NOT NULL,
    scopes TEXT[] NOT NULL DEFAULT ARRAY['events:write','events:read','endpoints:manage','deliveries:read','deliveries:replay']::TEXT[],
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','REVOKED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_by TEXT
);

CREATE INDEX api_keys_tenant_created_idx ON api_keys(tenant_id, created_at DESC);
CREATE INDEX api_keys_active_tenant_idx ON api_keys(tenant_id) WHERE status='ACTIVE';

CREATE TABLE auth_audit_log (
    audit_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID REFERENCES tenants(tenant_id),
    key_id UUID REFERENCES api_keys(key_id),
    action TEXT NOT NULL,
    actor TEXT NOT NULL DEFAULT 'system',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX auth_audit_tenant_created_idx ON auth_audit_log(tenant_id, created_at DESC);



