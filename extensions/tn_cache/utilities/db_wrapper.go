package utilities

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/trufnetwork/kwil-db/node/pg"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// poolDBWrapper wraps a pgxpool.Pool to implement the sql.DB interface
// This allows us to use our independent connection pool with Engine.Call
//
// Why we need this wrapper:
// Kwil-db's app.DB manages transactions internally and extensions share
// the same transaction context. When multiple extensions or operations
// compete for write access, the shared transaction can be closed by one
// operation while another is still using it, causing "tx is closed" errors.
// By creating an independent connection pool, we isolate cache operations
// from kwil-db's transaction lifecycle.
//
// Note: All cache operations are READ-ONLY. We use regular transactions
// (BeginTx) instead of read-only transactions (BeginReadTx) because:
// 1. The cache extension only queries data, never modifies it
// 2. Using regular transactions simplifies the interface implementation
// 3. PostgreSQL will optimize read-only queries automatically
type poolDBWrapper struct {
	pool *pgxpool.Pool
}

var _ sql.DB = &poolDBWrapper{}
var _ sql.AccessModer = &poolDBWrapper{}
var _ sql.QueryScanner = &poolDBWrapper{}

// NewPoolDBWrapper creates a new wrapper around the pgxpool
func NewPoolDBWrapper(pool *pgxpool.Pool) sql.DB {
	return &poolDBWrapper{pool: pool}
}

// Execute implements sql.Executor
func (w *poolDBWrapper) Execute(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
	// Acquire a connection to get the type map
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	// Build the OID to datatype map using kwil-db's type system
	oidToDataType := pg.OidTypesMap(conn.Conn().TypeMap())

	// Get column names and OIDs
	fields := rows.FieldDescriptions()
	columns := make([]string, len(fields))
	oids := make([]uint32, len(fields))
	for i, field := range fields {
		columns[i] = string(field.Name)
		oids[i] = field.DataTypeOID
	}

	// Collect all rows using kwil-db's type decoding
	var resultRows [][]any
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("scan row values: %w", err)
		}

		// Use kwil-db's decoding logic
		decodedValues, err := pg.DecodeFromPG(values, oids, oidToDataType)
		if err != nil {
			return nil, fmt.Errorf("decode values: %w", err)
		}

		resultRows = append(resultRows, decodedValues)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	return &sql.ResultSet{
		Columns: columns,
		Rows:    resultRows,
		Status: sql.CommandTag{
			Text:         rows.CommandTag().String(),
			RowsAffected: rows.CommandTag().RowsAffected(),
		},
	}, nil
}

// BeginTx implements sql.TxMaker
func (w *poolDBWrapper) BeginTx(ctx context.Context) (sql.Tx, error) {
	pgxTx, err := w.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	return &poolTxWrapper{tx: pgxTx}, nil
}

// poolTxWrapper wraps a pgx.Tx to implement the sql.Tx interface
type poolTxWrapper struct {
	tx pgx.Tx
}

var _ sql.Tx = &poolTxWrapper{}
var _ sql.QueryScanner = &poolTxWrapper{}
var _ sql.AccessModer = &poolTxWrapper{}

// Execute implements sql.Executor for transactions
func (t *poolTxWrapper) Execute(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
	rows, err := t.tx.Query(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("execute query in tx: %w", err)
	}
	defer rows.Close()

	// Build the OID to datatype map using kwil-db's type system
	oidToDataType := pg.OidTypesMap(t.tx.Conn().TypeMap())

	// Get column names and OIDs
	fields := rows.FieldDescriptions()
	columns := make([]string, len(fields))
	oids := make([]uint32, len(fields))
	for i, field := range fields {
		columns[i] = string(field.Name)
		oids[i] = field.DataTypeOID
	}

	// Collect all rows using kwil-db's type decoding
	var resultRows [][]any
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("scan row values: %w", err)
		}

		// Use kwil-db's decoding logic
		decodedValues, err := pg.DecodeFromPG(values, oids, oidToDataType)
		if err != nil {
			return nil, fmt.Errorf("decode values: %w", err)
		}

		resultRows = append(resultRows, decodedValues)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	return &sql.ResultSet{
		Columns: columns,
		Rows:    resultRows,
		Status: sql.CommandTag{
			Text:         rows.CommandTag().String(),
			RowsAffected: rows.CommandTag().RowsAffected(),
		},
	}, nil
}

// BeginTx implements sql.TxMaker for nested transactions
func (t *poolTxWrapper) BeginTx(ctx context.Context) (sql.Tx, error) {
	// pgx supports savepoints for nested transactions
	nestedTx, err := t.tx.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin nested transaction: %w", err)
	}
	return &poolTxWrapper{tx: nestedTx}, nil
}

// Rollback implements sql.Tx
func (t *poolTxWrapper) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

// Commit implements sql.Tx
func (t *poolTxWrapper) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

// QueryScanFn implements sql.QueryScanner for transactions
func (t *poolTxWrapper) QueryScanFn(ctx context.Context, stmt string, scans []any, fn func() error, args ...any) error {
	// Execute the query on the transaction
	rows, err := t.tx.Query(ctx, stmt, args...)
	if err != nil {
		return fmt.Errorf("query execution: %w", err)
	}
	defer rows.Close()

	// Process each row
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		if err := fn(); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	return nil
}

// AccessMode implements sql.AccessModer
func (w *poolDBWrapper) AccessMode() sql.AccessMode {
	return sql.ReadOnly
}

// QueryScanFn implements sql.QueryScanner
// This is required for Engine.Call to work with row-by-row callbacks
func (w *poolDBWrapper) QueryScanFn(ctx context.Context, stmt string, scans []any, fn func() error, args ...any) error {
	// Acquire a connection from the pool
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	// Execute the query
	rows, err := conn.Query(ctx, stmt, args...)
	if err != nil {
		return fmt.Errorf("query execution: %w", err)
	}
	defer rows.Close()

	// Process each row
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		if err := fn(); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	return nil
}

// AccessMode implements sql.AccessModer
func (t *poolTxWrapper) AccessMode() sql.AccessMode {
	return sql.ReadOnly
}
