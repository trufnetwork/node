package tn_vacuum

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/kwil-db/core/log"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
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
}

// pgStateStore implements stateStore using the node's Postgres connection.
type pgStateStore struct {
	db     sql.DB
	logger log.Logger
}

// newPGStateStore constructs a Postgres-backed store.
func newPGStateStore(db sql.DB, logger log.Logger) stateStore {
	return &pgStateStore{db: db, logger: logger}
}

// Ensure creates the schema and table necessary for persistence. The
// operations are idempotent so the method can be invoked on every start.
func (s *pgStateStore) Ensure(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin state setup tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Execute(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", vacuumSchemaName)); err != nil {
		return fmt.Errorf("create tn_vacuum schema: %w", err)
	}

	if _, err := tx.Execute(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.run_state (
			id INT PRIMARY KEY,
			last_run_height BIGINT NOT NULL,
			last_run_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`, vacuumSchemaName)); err != nil {
		return fmt.Errorf("create run_state table: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit state setup tx: %w", err)
	}
	return nil
}

// Load returns the previously persisted runState. The boolean indicates
// whether a record exists.
func (s *pgStateStore) Load(ctx context.Context) (runState, bool, error) {
	rs, err := s.db.Execute(ctx, fmt.Sprintf(`
		SELECT last_run_height, last_run_at
		FROM %s.run_state
		WHERE id = 1
	`, vacuumSchemaName))
	if err != nil {
		return runState{}, false, fmt.Errorf("load run_state: %w", err)
	}

	if len(rs.Rows) == 0 {
		return runState{}, false, nil
	}

	row := rs.Rows[0]
	height, ok := row[0].(int64)
	if !ok {
		return runState{}, false, fmt.Errorf("unexpected type for last_run_height: %T", row[0])
	}
	ts, ok := row[1].(time.Time)
	if !ok {
		return runState{}, false, fmt.Errorf("unexpected type for last_run_at: %T", row[1])
	}
	return runState{LastRunHeight: height, LastRunAt: ts.UTC()}, true, nil
}

// Save upserts the supplied runState. The extension only stores a single row,
// so the implementation uses a fixed primary key.
func (s *pgStateStore) Save(ctx context.Context, state runState) error {
	_, err := s.db.Execute(ctx, fmt.Sprintf(`
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
