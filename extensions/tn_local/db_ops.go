package tn_local

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// LocalDB wraps a sql.DB and provides operations against the ext_tn_local schema.
type LocalDB struct {
	db     sql.DB
	logger log.Logger
}

// NewLocalDB creates a new LocalDB.
func NewLocalDB(db sql.DB, logger log.Logger) *LocalDB {
	return &LocalDB{db: db, logger: logger}
}

// dbCreateStream inserts a new stream into ext_tn_local.streams.
func (ext *Extension) dbCreateStream(ctx context.Context, dataProvider, streamID, streamType string) error {
	_, err := ext.db.Execute(ctx, fmt.Sprintf(
		`INSERT INTO %s.streams (data_provider, stream_id, stream_type, created_at)
		 VALUES ($1, $2, $3, $4)`, SchemaName),
		dataProvider, streamID, streamType, time.Now().Unix())
	return err
}

// SetupSchema creates the ext_tn_local schema and all tables within a single transaction.
func (l *LocalDB) SetupSchema(ctx context.Context) error {
	l.logger.Info("setting up local storage schema")

	tx, err := l.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := setupLocalSchema(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	l.logger.Info("local storage schema setup complete")
	return nil
}
