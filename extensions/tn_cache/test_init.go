package tn_cache

import (
	"context"
	"fmt"
	"math"
	"strings"
	
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
	
	// For test purposes, return a simple command tag
	// The exact format is rarely checked in tests
	rowsAffected := len(result.Rows)
	return pgconn.NewCommandTag(fmt.Sprintf("OK %d", rowsAffected)), nil
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
		// Handle nil values for pointers
		switch d := dest.(type) {
		case *string:
			*d = ""
		case *int:
			*d = 0
		case *int64:
			*d = 0
		case *bool:
			*d = false
		case *[]byte:
			*d = nil
		case *interface{}:
			*d = nil
		}
		return nil
	}
	
	switch d := dest.(type) {
	case *string:
		switch s := src.(type) {
		case string:
			*d = s
		case []byte:
			*d = string(s)
		default:
			*d = fmt.Sprintf("%v", src)
		}
	case *[]byte:
		switch s := src.(type) {
		case []byte:
			*d = s
		case string:
			*d = []byte(s)
		default:
			return fmt.Errorf("cannot scan %T into *[]byte", src)
		}
	case *bool:
		switch s := src.(type) {
		case bool:
			*d = s
		case int:
			*d = s != 0
		case int64:
			*d = s != 0
		case string:
			// Handle common boolean string representations
			switch strings.ToLower(s) {
			case "true", "t", "yes", "y", "1":
				*d = true
			case "false", "f", "no", "n", "0":
				*d = false
			default:
				return fmt.Errorf("cannot scan string %q into *bool", s)
			}
		default:
			return fmt.Errorf("cannot scan %T into *bool", src)
		}
	case *int64:
		switch s := src.(type) {
		case int64:
			*d = s
		case int:
			*d = int64(s)
		case int32:
			*d = int64(s)
		case uint:
			*d = int64(s)
		case uint32:
			*d = int64(s)
		case uint64:
			// Check for overflow
			if s > math.MaxInt64 {
				return fmt.Errorf("uint64 value %d overflows int64", s)
			}
			*d = int64(s)
		default:
			return fmt.Errorf("cannot scan %T into *int64", src)
		}
	case *int:
		switch s := src.(type) {
		case int:
			*d = s
		case int64:
			// Check for overflow on 32-bit systems
			if s > math.MaxInt || s < math.MinInt {
				return fmt.Errorf("int64 value %d overflows int", s)
			}
			*d = int(s)
		case int32:
			*d = int(s)
		case uint:
			// Check for overflow
			if s > uint(math.MaxInt) {
				return fmt.Errorf("uint value %d overflows int", s)
			}
			*d = int(s)
		case uint32:
			// Check for overflow on 32-bit systems
			if s > math.MaxInt32 {
				return fmt.Errorf("uint32 value %d overflows int", s)
			}
			*d = int(s)
		default:
			return fmt.Errorf("cannot scan %T into *int", src)
		}
	case *uint:
		switch s := src.(type) {
		case uint:
			*d = s
		case uint64:
			// Check for overflow on 32-bit systems
			if s > uint64(^uint(0)) {
				return fmt.Errorf("uint64 value %d overflows uint", s)
			}
			*d = uint(s)
		case uint32:
			*d = uint(s)
		case int:
			if s < 0 {
				return fmt.Errorf("negative int value %d cannot be scanned into uint", s)
			}
			*d = uint(s)
		case int64:
			if s < 0 {
				return fmt.Errorf("negative int64 value %d cannot be scanned into uint", s)
			}
			*d = uint(s)
		default:
			return fmt.Errorf("cannot scan %T into *uint", src)
		}
	case *uint64:
		switch s := src.(type) {
		case uint64:
			*d = s
		case uint:
			*d = uint64(s)
		case uint32:
			*d = uint64(s)
		case int:
			if s < 0 {
				return fmt.Errorf("negative int value %d cannot be scanned into uint64", s)
			}
			*d = uint64(s)
		case int64:
			if s < 0 {
				return fmt.Errorf("negative int64 value %d cannot be scanned into uint64", s)
			}
			*d = uint64(s)
		default:
			return fmt.Errorf("cannot scan %T into *uint64", src)
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