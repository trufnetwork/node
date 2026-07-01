package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/node/pg"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// settlementReadLockTimeout bounds how long a settlement poll read waits on a
// Postgres table lock before it fails.
//
// A settlement read only ever blocks when a block is holding an
// AccessExclusiveLock (in-block DDL: ALTER/DROP/etc.) on a table it reads —
// ordinary DML never conflicts with a plain SELECT's AccessShareLock. Bounding
// the wait lets such a read fail fast and release its goroutine instead of
// deadlocking block production against the engine interpreter lock.
const settlementReadLockTimeout = 3 * time.Second

// ReadPool is a dedicated, read-only Postgres connection pool for the settlement
// scheduler's poll-time reads.
//
// It deliberately bypasses the engine interpreter: reads run as plain SQL via
// Execute on its own connections, so a settlement read can never hold the
// interpreter lock while it waits on a Postgres table lock — the condition that
// previously deadlocked the chain against in-block DDL. Every connection also
// carries a bounded lock_timeout (see settlementReadLockTimeout) so a read that
// runs inside EndBlock (config reload) fails fast rather than self-blocking on
// the block's own uncommitted AccessExclusiveLock.
type ReadPool struct {
	pool *pgxpool.Pool
}

// ReadPool satisfies sql.DB so it can be handed to EngineOperations as its
// read handle. Only Execute is used; BeginTx is intentionally unsupported.
var _ sql.DB = (*ReadPool)(nil)

// NewReadPool builds an independent read-only connection pool from the node's DB
// config, applying a bounded lock_timeout on every connection.
func NewReadPool(ctx context.Context, service *common.Service, logger log.Logger) (*ReadPool, error) {
	dbConfig := service.LocalConfig.DB

	// Build the DSN without the password, then set it on the parsed ConnConfig.
	// A password containing spaces or special characters cannot be safely
	// interpolated into a key=value connection string.
	connStr := fmt.Sprintf("host=%s port=%s user=%s database=%s sslmode=disable",
		dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.DBName)

	poolConfig, err := buildReadPoolConfig(connStr)
	if err != nil {
		return nil, err
	}
	if dbConfig.Pass != "" {
		poolConfig.ConnConfig.Password = dbConfig.Pass
	}

	return newReadPoolFromConfig(ctx, poolConfig, logger)
}

// lockTimeoutMillis renders settlementReadLockTimeout as a Postgres
// millisecond string for use as a connection startup parameter.
func lockTimeoutMillis() string {
	return fmt.Sprintf("%d", settlementReadLockTimeout.Milliseconds())
}

// buildReadPoolConfig parses the connection string and applies the pool tuning
// and the bounded lock_timeout. It is separated from pool creation so a unit
// test can assert the lock_timeout is wired without needing a live database.
func buildReadPoolConfig(connStr string) (*pgxpool.Config, error) {
	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse settlement read pool config: %w", err)
	}

	// The settlement poll reads are low-frequency (a handful per scheduler tick),
	// so a small dedicated pool is plenty and keeps the connection budget modest.
	poolConfig.MaxConns = 4
	poolConfig.MinConns = 1
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute

	// Apply the bounded lock_timeout to every connection as a startup parameter so
	// a read blocked by an in-block AccessExclusiveLock fails fast instead of
	// hanging. A startup RuntimeParam (rather than an AfterConnect hook) keeps the
	// setting introspectable, so a unit test can assert it is present.
	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams = map[string]string{}
	}
	poolConfig.ConnConfig.RuntimeParams["lock_timeout"] = lockTimeoutMillis()

	return poolConfig, nil
}

// newReadPoolFromConnString builds a ReadPool from a pgx connection string.
// Used by tests to exercise the same pool tuning + lock_timeout as NewReadPool.
func newReadPoolFromConnString(ctx context.Context, connStr string, logger log.Logger) (*ReadPool, error) {
	poolConfig, err := buildReadPoolConfig(connStr)
	if err != nil {
		return nil, err
	}
	return newReadPoolFromConfig(ctx, poolConfig, logger)
}

// newReadPoolFromConfig creates the pgxpool from an already-built config and
// verifies connectivity before returning.
func newReadPoolFromConfig(ctx context.Context, poolConfig *pgxpool.Config, logger log.Logger) (*ReadPool, error) {
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create settlement read pool: %w", err)
	}

	// Verify connectivity before returning, so a misconfigured pool fails loudly
	// here rather than on the first poll read.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("test settlement read pool connection: %w", err)
	}
	conn.Release()

	logger.Info("created independent read pool for settlement",
		"max_conns", poolConfig.MaxConns,
		"lock_timeout", settlementReadLockTimeout.String())

	return &ReadPool{pool: pool}, nil
}

// Execute runs a read-only statement directly on the pool, bypassing the engine
// interpreter. It mirrors the decoding done by node/extensions/tn_local's
// PoolDBWrapper so the returned values match what the engine would yield.
func (p *ReadPool) Execute(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
	conn, err := p.pool.Acquire(ctx)
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

// BeginTx implements sql.TxMaker. The settlement read path issues single-
// statement reads via Execute and never opens transactions, so this is
// intentionally unsupported.
func (p *ReadPool) BeginTx(ctx context.Context) (sql.Tx, error) {
	return nil, fmt.Errorf("settlement read pool does not support transactions")
}

// Close closes the underlying connection pool.
func (p *ReadPool) Close() {
	if p != nil && p.pool != nil {
		p.pool.Close()
	}
}
