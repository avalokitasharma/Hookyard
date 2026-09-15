package endpoint

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/avalokitasharma/HookYard/endpoint-service/internal/retrypolicy"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("endpoint not found")
var ErrConflict = errors.New("endpoint conflict")
var ErrInvalid = errors.New("invalid endpoint request")

type Repository struct {
	db                *pgxpool.Pool
	endpointTopic     string
	subscriptionTopic string
}

func NewRepository(db *pgxpool.Pool, endpointTopic, subscriptionTopic string) *Repository {
	return &Repository{
		db:                db,
		endpointTopic:     endpointTopic,
		subscriptionTopic: subscriptionTopic,
	}
}

func (r *Repository) CreateEndpoint(ctx context.Context, tenantID uuid.UUID, req CreateRequest, encryptedSecret []byte, policy retrypolicy.Policy) (Endpoint, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return Endpoint{}, fmt.Errorf("begin create endpoint: %w", err)
	}
	defer tx.Rollback(ctx)

	// Lock the tenant row for the duration of endpoint creation. Tenant deletion
	// takes the same row lock, preventing a race where a tenant is deleted
	// between service-level validation and this INSERT.
	var tenantStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM tenants WHERE tenant_id=$1 FOR SHARE`, tenantID).Scan(&tenantStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Endpoint{}, fmt.Errorf("%w: tenant not found", ErrInvalid)
		}
		return Endpoint{}, fmt.Errorf("check tenant: %w", err)
	}
	if tenantStatus != "ACTIVE" {
		return Endpoint{}, fmt.Errorf("%w: tenant is not active", ErrInvalid)
	}

	id := uuid.New()
	version := int64(1)
	_, err = tx.Exec(ctx, `INSERT INTO endpoints(endpoint_id,tenant_id,name,url,secret_encrypted,status,connect_timeout_ms,request_timeout_ms,retry_policy_id,version,created_at,updated_at)
	VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW(),NOW())`, id, tenantID, req.Name, req.URL, encryptedSecret, StatusActive, req.ConnectTimeoutMS, req.RequestTimeoutMS, policy.ID, version)
	if err != nil {
		return Endpoint{}, mapDBError(err)
	}
	for _, eventType := range req.EventTypes {
		if _, err = tx.Exec(ctx, `INSERT INTO endpoint_subscriptions(endpoint_id,event_type) VALUES($1,$2)`, id, eventType); err != nil {
			return Endpoint{}, fmt.Errorf("insert subscription: %w", err)
		}
	}
	var e Endpoint
	e, err = r.scanTx(ctx, tx, id)
	if err != nil {
		return Endpoint{}, err
	}
	// if err = r.enqueueChanged(ctx, tx, e, policy); err != nil {
	// 	return Endpoint{}, err
	// }
	if err = tx.Commit(ctx); err != nil {
		return Endpoint{}, fmt.Errorf("commit create endpoint: %w", err)
	}
	return e, nil
}

func (r *Repository) scanTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) (Endpoint, error) {
	row := tx.QueryRow(ctx, `SELECT endpoint_id,tenant_id,name,url,secret_encrypted,status,connect_timeout_ms,request_timeout_ms,retry_policy_id,version,created_at,updated_at,deleted_at FROM endpoints WHERE endpoint_id=$1`, id)

	e, err := scanEndpoint(row)
	if err != nil {
		return Endpoint{}, err
	}
	if err = r.loadSubscriptionsTx(ctx, tx, &e); err != nil {
		return Endpoint{}, err
	}
	return e, nil
}
func (r *Repository) loadSubscriptionsTx(ctx context.Context, tx pgx.Tx, e *Endpoint) error {
	rows, err := tx.Query(ctx, `SELECT event_type FROM endpoint_subscriptions WHERE endpoint_id=$1 ORDER BY event_type`, e.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return err
		}
		e.EventTypes = append(e.EventTypes, s)
	}
	return rows.Err()
}

func scanEndpoint(row interface{ Scan(...any) error }) (Endpoint, error) {
	var e Endpoint
	err := row.Scan(&e.ID,
		&e.TenantID,
		&e.Name,
		&e.URL,
		&e.SecretEncrypted,
		&e.Status,
		&e.ConnectTimeoutMS,
		&e.RequestTimeoutMS,
		&e.RetryPolicyID,
		&e.Version,
		&e.CreatedAt,
		&e.UpdatedAt,
		&e.DeletedAt)
	return e, err
}
func mapDBError(err error) error {
	if strings.Contains(strings.ToLower(err.Error()), "duplicate key") {
		return fmt.Errorf("%w: duplicate endpoint data", ErrConflict)
	}
	return fmt.Errorf("database error: %w", err)
}

// Get endpoint

func (r *Repository) GetEndpoint(ctx context.Context, tenantID, id uuid.UUID) (Endpoint, retrypolicy.Policy, error) {
	row := r.db.QueryRow(ctx, `SELECT endpoint_id,tenant_id,name,url,secret_encrypted,status,connect_timeout_ms,request_timeout_ms,retry_policy_id,version,created_at,updated_at,deleted_at FROM endpoints WHERE tenant_id=$1 AND endpoint_id=$2`, tenantID, id)
	e, err := scanEndpoint(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Endpoint{}, retrypolicy.Policy{}, ErrNotFound
	}
	if err != nil {
		return Endpoint{}, retrypolicy.Policy{}, fmt.Errorf("get endpoint: %w", err)
	}
	if err = r.loadSubscriptions(ctx, &e); err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	p, err := r.getPolicy(ctx, tenantID, e.RetryPolicyID)
	if err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	return e, p, nil
}
func (r *Repository) loadSubscriptions(ctx context.Context, e *Endpoint) error {
	rows, err := r.db.Query(ctx, `SELECT event_type FROM endpoint_subscriptions WHERE endpoint_id=$1 ORDER BY event_type`, e.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return err
		}
		e.EventTypes = append(e.EventTypes, s)
	}
	return rows.Err()
}

// Get policy

func (r *Repository) getPolicy(ctx context.Context, tenantID, id uuid.UUID) (retrypolicy.Policy, error) {
	var p retrypolicy.Policy
	err := r.db.QueryRow(ctx, `SELECT retry_policy_id,tenant_id,name,max_attempts,backoff_type,initial_delay_ms,max_delay_ms,jitter_percent FROM retry_policies WHERE tenant_id=$1 AND retry_policy_id=$2`, tenantID, id).Scan(&p.ID, &p.TenantID, &p.Name, &p.MaxAttempts, &p.BackoffType, &p.InitialDelay, &p.MaxDelay, &p.JitterPercent)
	if err != nil {
		return p, fmt.Errorf("get retry policy: %w", err)
	}
	return p, nil
}
func (r *Repository) getPolicyTx(ctx context.Context, tx pgx.Tx, tenantID, id uuid.UUID) (retrypolicy.Policy, error) {
	var p retrypolicy.Policy
	err := tx.QueryRow(ctx, `SELECT retry_policy_id,tenant_id,name,max_attempts,backoff_type,initial_delay_ms,max_delay_ms,jitter_percent FROM retry_policies WHERE tenant_id=$1 AND retry_policy_id=$2`, tenantID, id).Scan(&p.ID, &p.TenantID, &p.Name, &p.MaxAttempts, &p.BackoffType, &p.InitialDelay, &p.MaxDelay, &p.JitterPercent)
	if err != nil {
		return p, fmt.Errorf("get retry policy: %w", err)
	}
	return p, nil
}

// Patch endpoint
func (r *Repository) Patch(ctx context.Context, tenantID, id uuid.UUID, req PatchRequest, encryptedSecret []byte, policy *retrypolicy.Policy) (Endpoint, retrypolicy.Policy, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return Endpoint{}, retrypolicy.Policy{}, fmt.Errorf("begin patch endpoint: %w", err)
	}
	defer tx.Rollback(ctx)
	var e Endpoint
	e, err = r.scanTx(ctx, tx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Endpoint{}, retrypolicy.Policy{}, ErrNotFound
	}
	if err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	if e.TenantID != tenantID {
		return Endpoint{}, retrypolicy.Policy{}, ErrNotFound
	}
	if e.Status == StatusDeleted {
		return Endpoint{}, retrypolicy.Policy{}, fmt.Errorf("%w: deleted endpoint cannot be patched", ErrConflict)
	}

	name, url := e.Name, e.URL
	connect, request := e.ConnectTimeoutMS, e.RequestTimeoutMS
	policyID := e.RetryPolicyID
	if req.Name != nil {
		name = *req.Name
	}
	if req.URL != nil {
		url = *req.URL
	}
	if req.ConnectTimeoutMS != nil {
		connect = *req.ConnectTimeoutMS
	}
	if req.RequestTimeoutMS != nil {
		request = *req.RequestTimeoutMS
	}
	if req.RetryPolicyID != nil {
		policyID = *req.RetryPolicyID
	}
	var secret = e.SecretEncrypted
	if req.Secret != nil {
		secret = encryptedSecret
	}
	if policy == nil || policy.ID != policyID {
		p, err := r.getPolicyTx(ctx, tx, tenantID, policyID)
		if err != nil {
			return Endpoint{}, retrypolicy.Policy{}, err
		}
		policy = &p
	}
	version := e.Version + 1
	_, err = tx.Exec(ctx, `UPDATE endpoints SET name=$3,url=$4,secret_encrypted=$5,connect_timeout_ms=$6,request_timeout_ms=$7,retry_policy_id=$8,version=$9,updated_at=NOW() WHERE tenant_id=$1 AND endpoint_id=$2`, tenantID, id, name, url, secret, connect, request, policyID, version)
	if err != nil {
		return Endpoint{}, retrypolicy.Policy{}, mapDBError(err)
	}
	if req.EventTypes != nil {
		if _, err = tx.Exec(ctx, `DELETE FROM endpoint_subscriptions WHERE endpoint_id=$1`, id); err != nil {
			return Endpoint{}, retrypolicy.Policy{}, err
		}
		for _, eventType := range *req.EventTypes {
			if _, err = tx.Exec(ctx, `INSERT INTO endpoint_subscriptions(endpoint_id,event_type) VALUES($1,$2)`, id, eventType); err != nil {
				return Endpoint{}, retrypolicy.Policy{}, fmt.Errorf("insert subscription: %w", err)
			}
		}
	}
	e, err = r.scanTx(ctx, tx, id)
	if err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	// if err = r.enqueueChanged(ctx, tx, e, *policy); err != nil {
	// 	return Endpoint{}, retrypolicy.Policy{}, err
	// }
	if err = tx.Commit(ctx); err != nil {
		return Endpoint{}, retrypolicy.Policy{}, fmt.Errorf("commit patch endpoint: %w", err)
	}
	return e, *policy, nil
}

// delete endpoint

func (r *Repository) Delete(ctx context.Context, tenantID, id uuid.UUID) (Endpoint, retrypolicy.Policy, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	defer tx.Rollback(ctx)
	var e Endpoint
	e, err = r.scanTx(ctx, tx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Endpoint{}, retrypolicy.Policy{}, ErrNotFound
	}
	if err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	if e.TenantID != tenantID {
		return Endpoint{}, retrypolicy.Policy{}, ErrNotFound
	}
	if e.Status == StatusDeleted {
		p, pe := r.getPolicyTx(ctx, tx, tenantID, e.RetryPolicyID)
		if pe != nil {
			return Endpoint{}, retrypolicy.Policy{}, pe
		}
		return e, p, nil
	}
	p, err := r.getPolicyTx(ctx, tx, tenantID, e.RetryPolicyID)
	if err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	e.Version++
	e.Status = StatusDeleted
	now := time.Now().UTC()
	e.DeletedAt = &now
	e.UpdatedAt = now
	e.EventTypes = nil
	_, err = tx.Exec(ctx, `UPDATE endpoints SET status=$3,deleted_at=$4,version=$5,updated_at=$4 WHERE tenant_id=$1 AND endpoint_id=$2`, tenantID, id, StatusDeleted, now, e.Version)
	if err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM endpoint_subscriptions WHERE endpoint_id=$1`, id); err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	// if err = r.enqueueChanged(ctx, tx, e, p); err != nil {
	// 	return Endpoint{}, retrypolicy.Policy{}, err
	// }
	if err = tx.Commit(ctx); err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	return e, p, nil
}

func (r *Repository) GetSubscriptions(ctx context.Context, tenantID uuid.UUID, eventType string) ([]Endpoint, error) {
	rows, err := r.db.Query(ctx,
		`SELECT e.endpoint_id,e.tenant_id,e.name,e.url,e.secret_encrypted,e.status,e.connect_timeout_ms,e.request_timeout_ms,e.retry_policy_id,e.version,e.created_at,e.updated_at,e.deleted_at 
		FROM endpoint_subscriptions s 
		JOIN endpoints e ON e.endpoint_id=s.endpoint_id 
		WHERE e.tenant_id=$1 AND s.event_type=$2 AND e.status=$3 AND e.deleted_at IS NULL ORDER BY e.endpoint_id`,
		tenantID, eventType, StatusActive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Endpoint
	for rows.Next() {
		e, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
