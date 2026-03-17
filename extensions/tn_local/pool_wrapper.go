package tn_local

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/trufnetwork/kwil-db/node/pg"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// PoolDBWrapper wraps a pgxpool.Pool to implement the sql.DB interface
// with ReadWrite access for local storage operations.
type PoolDBWrapper struct {
	Pool *pgxpool.Pool
}

var _ sql.DB = &PoolDBWrapper{}

// NewPoolDBWrapper creates a new wrapper around the pgxpool.
func NewPoolDBWrapper(pool *pgxpool.Pool) *PoolDBWrapper {
	return &PoolDBWrapper{Pool: pool}
}

// Execute implements sql.Executor.
func (w *PoolDBWrapper) Execute(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
	conn, err := w.Pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	oidToDataType := pg.OidTypesMap(conn.Conn().TypeMap())

	fields := rows.FieldDescriptions()
	columns := make([]string, len(fields))
	oids := make([]uint32, len(fields))
	for i, field := range fields {
		columns[i] = string(field.Name)
		oids[i] = field.DataTypeOID
	}

	var resultRows [][]any
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("scan row values: %w", err)
		}
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

// BeginTx implements sql.TxMaker.
func (w *PoolDBWrapper) BeginTx(ctx context.Context) (sql.Tx, error) {
	pgxTx, err := w.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	return &poolTxWrapper{tx: pgxTx}, nil
}

// Close closes the underlying connection pool.
func (w *PoolDBWrapper) Close() {
	w.Pool.Close()
}

// poolTxWrapper wraps a pgx.Tx to implement the sql.Tx interface.
type poolTxWrapper struct {
	tx pgx.Tx
}

var _ sql.Tx = &poolTxWrapper{}

// Execute implements sql.Executor for transactions.
func (t *poolTxWrapper) Execute(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
	rows, err := t.tx.Query(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("execute query in tx: %w", err)
	}
	defer rows.Close()

	oidToDataType := pg.OidTypesMap(t.tx.Conn().TypeMap())

	fields := rows.FieldDescriptions()
	columns := make([]string, len(fields))
	oids := make([]uint32, len(fields))
	for i, field := range fields {
		columns[i] = string(field.Name)
		oids[i] = field.DataTypeOID
	}

	var resultRows [][]any
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("scan row values: %w", err)
		}
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

// BeginTx implements sql.TxMaker for nested transactions (savepoints).
func (t *poolTxWrapper) BeginTx(ctx context.Context) (sql.Tx, error) {
	nestedTx, err := t.tx.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin nested transaction: %w", err)
	}
	return &poolTxWrapper{tx: nestedTx}, nil
}

// Rollback implements sql.Tx.
func (t *poolTxWrapper) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

// Commit implements sql.Tx.
func (t *poolTxWrapper) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}
