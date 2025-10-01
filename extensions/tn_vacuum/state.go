package tn_vacuum

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/trufnetwork/kwil-db/core/log"
)

const (
	// vacuumSchemaName is the private Postgres schema used to persist the
	// extension's local bookkeeping. Schemas prefixed with ext_ are ignored by
	// consensus hashing, keeping this purely node-local state.
	vacuumSchemaName = "ext_tn_vacuum"
)

// runState is the minimal persisted view of the extension. It tracks the block
// height and timestamp of the last successful vacuum run.
type runState struct {
	LastRunHeight int64
	LastRunAt     time.Time
}

// stateStore represents a persistence backend capable of storing and loading
// runState snapshots.
type stateStore interface {
	Ensure(ctx context.Context) error
	Load(ctx context.Context) (runState, bool, error)
	Save(ctx context.Context, state runState) error
	Close()
}

// pgStateStore implements stateStore using a dedicated pgx connection pool.
type pgStateStore struct {
	pool   *pgxpool.Pool
	logger log.Logger
}

// newPGStateStore constructs a Postgres-backed store with its own pool.
func newPGStateStore(ctx context.Context, cfg DBConnConfig, logger log.Logger) (stateStore, error) {
	connStr := buildConnString(cfg)
	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse connection config: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}
	return &pgStateStore{pool: pool, logger: logger}, nil
}

func (s *pgStateStore) Ensure(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", vacuumSchemaName)); err != nil {
		return fmt.Errorf("create tn_vacuum schema: %w", err)
	}

	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s.run_state (
            id INT PRIMARY KEY,
            last_run_height BIGINT NOT NULL,
            last_run_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )`, vacuumSchemaName)); err != nil {
		return fmt.Errorf("create run_state table: %w", err)
	}
	return nil
}

func (s *pgStateStore) Load(ctx context.Context) (runState, bool, error) {
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`
        SELECT last_run_height, last_run_at
        FROM %s.run_state
        WHERE id = 1
    `, vacuumSchemaName))

	var state runState
	if err := row.Scan(&state.LastRunHeight, &state.LastRunAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return runState{}, false, nil
		}
		return runState{}, false, fmt.Errorf("load run_state: %w", err)
	}
	state.LastRunAt = state.LastRunAt.UTC()
	return state, true, nil
}

func (s *pgStateStore) Save(ctx context.Context, state runState) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
        INSERT INTO %s.run_state (id, last_run_height, last_run_at, updated_at)
        VALUES (1, $1, $2, NOW())
        ON CONFLICT (id)
        DO UPDATE SET
            last_run_height = EXCLUDED.last_run_height,
            last_run_at = EXCLUDED.last_run_at,
            updated_at = NOW()
    `, vacuumSchemaName), state.LastRunHeight, state.LastRunAt)
	if err != nil {
		return fmt.Errorf("persist run_state: %w", err)
	}
	return nil
}

func (s *pgStateStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// noopStateStore is used when persistence is disabled or misconfigured.
type noopStateStore struct{}

func (noopStateStore) Ensure(ctx context.Context) error { return nil }
func (noopStateStore) Load(ctx context.Context) (runState, bool, error) {
	return runState{}, false, nil
}
func (noopStateStore) Save(ctx context.Context, state runState) error { return nil }
func (noopStateStore) Close()                                         {}
