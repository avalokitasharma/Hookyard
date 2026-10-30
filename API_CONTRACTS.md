# Tenant API additions

These routes are tenant-scoped. The current services still read `X-Tenant-ID`; this is a development integration point, not a production trust boundary. Before production, only the authenticated gateway may set this identity context and direct untrusted access to the services must be blocked.

## Endpoint service

- `GET /v1/endpoints?limit=50&cursor=<opaque>&status=ACTIVE`
- `status` accepts `ACTIVE`, `DELETED`, or `ALL`; it defaults to `ACTIVE`.
- `limit` defaults to 50 and must be between 1 and 100.
- Response: `{ "items": [...], "next_cursor": "..." }`.
- Endpoint output uses the existing public view and never returns the signing secret.

## Delivery service

- `GET /v1/deliveries?limit=50&cursor=<opaque>&status=FAILED&endpoint_id=<uuid>&event_id=<uuid>`
- `GET /v1/deliveries/{id}/attempts?limit=50&cursor=<opaque>`
- `GET /v1/dlq?limit=50&cursor=<opaque>&endpoint_id=<uuid>&event_id=<uuid>`
- `POST /v1/dlq/{id}/replay`
- Delivery and DLQ list responses use `{ "items": [...], "next_cursor": "..." }`.
- Delivery status filters accept `PENDING`, `IN_PROGRESS`, `RETRYING`, `DELIVERED`, `FAILED`, `DLQ`, and `CANCELLED`.
- DLQ listing is always constrained to `status = 'DLQ'`.
- Attempt history exposes attempt number, status, HTTP status, latency, error code and timestamps. It intentionally excludes response bodies, response headers, raw error messages and internal traces.
- Delivery list/get responses intentionally omit raw `last_error_message` for the same reason.

Cursors are opaque Base64URL-encoded keyset positions. They are pagination state only; they do not grant access. Every repository query still requires the authenticated tenant ID.

## Schema note

The delivery repository already reads/writes `event_type` and `event_payload`, but the original delivery migration omitted both columns. The corrected initial migration includes them. It also uses a partial unique index for original (non-replay) event/endpoint deliveries so replay rows can coexist with the original delivery. Fresh development databases should be created from the corrected migration. Existing databases need a data-aware backfill of event payloads from the Event Service before these columns can safely be made `NOT NULL`; do not fill historical payloads with `{}`.
