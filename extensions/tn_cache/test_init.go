package tn_cache

import (
	"context"
	"fmt"
	
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	kwilconfig "github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
)

// testConfig holds all test-related configuration in one place
type testConfig struct {
	config   map[string]string
	dbConfig *kwilconfig.DBConfig
	dbPool   internal.DBPool
	sqlDB    sql.DB
}

var testOverrides = &testConfig{}

// SetTestConfiguration sets configuration for testing purposes
// This bypasses the normal service.LocalConfig.Extensions mechanism
func SetTestConfiguration(extConfig map[string]string) {
	testOverrides.config = extConfig
}

// SetTestDBConfiguration sets database configuration for testing
func SetTestDBConfiguration(dbConfig kwilconfig.DBConfig) {
	testOverrides.dbConfig = &dbConfig
}

// SetTestDB allows tests to inject their own database connection
// that will be used instead of creating a new pool
func SetTestDB(db sql.DB) {
	if db == nil {
		testOverrides.sqlDB = nil
		testOverrides.dbPool = nil
		return
	}
	
	testOverrides.sqlDB = db
	testOverrides.dbPool = newSimpleDBPoolAdapter(db)
}

// getTestConfig returns test configuration if set
func getTestConfig() map[string]string {
	return testOverrides.config
}

// getTestDBConfig returns test DB configuration if set
func getTestDBConfig() *kwilconfig.DBConfig {
	return testOverrides.dbConfig
}

// getTestDB returns the injected test database if set
func getTestDB() sql.DB {
	return testOverrides.sqlDB
}

// getTestDBPool returns the test DB pool adapter if set
func getTestDBPool() internal.DBPool {
	return testOverrides.dbPool
}

// simpleDBPoolAdapter is a minimal adapter for tests
// It only implements the methods actually used by CacheDB
type simpleDBPoolAdapter struct {
	db sql.DB
}

func newSimpleDBPoolAdapter(db sql.DB) internal.DBPool {
	return &simpleDBPoolAdapter{db: db}
}

func (a *simpleDBPoolAdapter) Begin(ctx context.Context) (pgx.Tx, error) {
	// For tests, we often want to use the existing transaction
	// Check if the db is already a transaction
	if tx, ok := a.db.(sql.Tx); ok {
		// Return a no-op transaction wrapper
		return &noOpTx{tx: tx}, nil
	}
	
	// Otherwise start a new transaction
	tx, err := a.db.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	return &simpleTxAdapter{tx: tx}, nil
}

func (a *simpleDBPoolAdapter) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	// CacheDB doesn't use Query directly, only through transactions
	return nil, fmt.Errorf("Query not implemented - use transaction")
}

func (a *simpleDBPoolAdapter) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	// CacheDB doesn't use QueryRow directly, only through transactions
	return &emptyRow{err: fmt.Errorf("QueryRow not implemented - use transaction")}
}

// noOpTx wraps an existing transaction (no commit/rollback needed)
type noOpTx struct {
	tx sql.Tx
}

func (t *noOpTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return nil, fmt.Errorf("nested transactions not supported")
}

func (t *noOpTx) Commit(ctx context.Context) error {
	// No-op: don't actually commit the test's transaction
	return nil
}

func (t *noOpTx) Rollback(ctx context.Context) error {
	// No-op: don't actually rollback the test's transaction
	return nil
}

func (t *noOpTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, fmt.Errorf("CopyFrom not implemented")
}

func (t *noOpTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return nil
}

func (t *noOpTx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (t *noOpTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, fmt.Errorf("Prepare not implemented")
}

func (t *noOpTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	result, err := t.tx.Execute(ctx, sql, arguments...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", len(result.Rows))), nil
}

func (t *noOpTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	result, err := t.tx.Execute(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return newSimpleRows(result), nil
}

func (t *noOpTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	result, err := t.tx.Execute(ctx, sql, args...)
	if err != nil {
		return &emptyRow{err: err}
	}
	if len(result.Rows) == 0 {
		return &emptyRow{err: pgx.ErrNoRows}
	}
	return &simpleRow{row: result.Rows[0]}
}

func (t *noOpTx) Conn() *pgx.Conn {
	return nil
}

// simpleTxAdapter wraps a new transaction
type simpleTxAdapter struct {
	noOpTx
	tx sql.Tx
}

func (t *simpleTxAdapter) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *simpleTxAdapter) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

// Minimal row implementations
type emptyRow struct {
	err error
}

func (r *emptyRow) Scan(dest ...any) error {
	return r.err
}

type simpleRow struct {
	row []any
}

func (r *simpleRow) Scan(dest ...any) error {
	if len(dest) != len(r.row) {
		return fmt.Errorf("scan dest count (%d) != column count (%d)", len(dest), len(r.row))
	}
	for i, val := range r.row {
		if err := scanValue(dest[i], val); err != nil {
			return err
		}
	}
	return nil
}

// simpleRows implements pgx.Rows minimally
type simpleRows struct {
	result *sql.ResultSet
	idx    int
}

func newSimpleRows(result *sql.ResultSet) pgx.Rows {
	return &simpleRows{result: result, idx: -1}
}

func (r *simpleRows) Close() {}
func (r *simpleRows) Err() error { return nil }
func (r *simpleRows) CommandTag() pgconn.CommandTag {
	return pgconn.NewCommandTag(fmt.Sprintf("SELECT %d", len(r.result.Rows)))
}
func (r *simpleRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *simpleRows) Next() bool {
	r.idx++
	return r.idx < len(r.result.Rows)
}
func (r *simpleRows) Scan(dest ...any) error {
	if r.idx < 0 || r.idx >= len(r.result.Rows) {
		return fmt.Errorf("no row to scan")
	}
	row := r.result.Rows[r.idx]
	if len(dest) != len(row) {
		return fmt.Errorf("scan dest count (%d) != column count (%d)", len(dest), len(row))
	}
	for i, val := range row {
		if err := scanValue(dest[i], val); err != nil {
			return err
		}
	}
	return nil
}
func (r *simpleRows) Values() ([]any, error) {
	if r.idx < 0 || r.idx >= len(r.result.Rows) {
		return nil, fmt.Errorf("no row")
	}
	return r.result.Rows[r.idx], nil
}
func (r *simpleRows) RawValues() [][]byte { return nil }
func (r *simpleRows) Conn() *pgx.Conn { return nil }

// scanValue handles type conversion for common cases
func scanValue(dest, src any) error {
	if src == nil {
		return nil
	}
	
	switch d := dest.(type) {
	case *string:
		*d = fmt.Sprintf("%v", src)
	case *int64:
		switch s := src.(type) {
		case int64:
			*d = s
		case int:
			*d = int64(s)
		default:
			return fmt.Errorf("cannot scan %T into *int64", src)
		}
	case *int:
		switch s := src.(type) {
		case int:
			*d = s
		case int64:
			*d = int(s)
		default:
			return fmt.Errorf("cannot scan %T into *int", src)
		}
	default:
		// For other types, try direct assignment
		if iface, ok := dest.(*interface{}); ok {
			*iface = src
			return nil
		}
		return fmt.Errorf("unsupported scan type: %T", dest)
	}
	
	return nil
}