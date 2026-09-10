CREATE TABLE tenants (
    tenant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('ACTIVE', 'DELETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ NULL
);

CREATE UNIQUE INDEX idx_tenants_active_name
    ON tenants(lower(name))
    WHERE deleted_at IS NULL;

CREATE INDEX idx_tenants_status
    ON tenants(status, tenant_id);

-- Enforce tenant ownership at the database boundary. Existing data must have
-- corresponding tenants before this migration can be applied.
ALTER TABLE retry_policies
    ADD CONSTRAINT fk_retry_policies_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id);

ALTER TABLE endpoints
    ADD CONSTRAINT fk_endpoints_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id);

