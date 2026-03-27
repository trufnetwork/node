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

// dbLookupStreamRef looks up a stream by data_provider and stream_id.
// Returns (id, stream_type, nil) if found, or (0, "", nil) if not found.
func (ext *Extension) dbLookupStreamRef(ctx context.Context, dataProvider, streamID string) (int64, string, error) {
	rs, err := ext.db.Execute(ctx, fmt.Sprintf(
		`SELECT id, stream_type FROM %s.streams WHERE data_provider = $1 AND stream_id = $2`, SchemaName),
		dataProvider, streamID)
	if err != nil {
		return 0, "", err
	}
	if len(rs.Rows) == 0 {
		return 0, "", nil
	}
	id, ok := rs.Rows[0][0].(int64)
	if !ok {
		return 0, "", fmt.Errorf("unexpected id type: %T", rs.Rows[0][0])
	}
	streamType, ok := rs.Rows[0][1].(string)
	if !ok {
		return 0, "", fmt.Errorf("unexpected stream_type type: %T", rs.Rows[0][1])
	}
	return id, streamType, nil
}

// dbInsertRecords batch-inserts resolved records into ext_tn_local.primitive_events
// within a transaction. Mirrors the consensus INSERT in 003-primitive-insertion.sql.
func (ext *Extension) dbInsertRecords(ctx context.Context, streamRefs []int64, eventTimes []int64, values []string) error {
	createdAt := time.Now().Unix()

	tx, err := ext.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for i := range streamRefs {
		_, err := tx.Execute(ctx, fmt.Sprintf(
			`INSERT INTO %s.primitive_events (stream_ref, event_time, value, created_at)
			 VALUES ($1, $2, $3, $4)`, SchemaName),
			streamRefs[i], eventTimes[i], values[i], createdAt)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// dbGetNextGroupSequence returns MAX(group_sequence)+1 for a parent stream, or 1 if none exist.
// Mirrors consensus get_current_group_sequence + 1 (004-composed-taxonomy.sql:86).
func (ext *Extension) dbGetNextGroupSequence(ctx context.Context, tx sql.Tx, parentRef int64) (int, error) {
	rs, err := tx.Execute(ctx, fmt.Sprintf(
		`SELECT COALESCE(MAX(group_sequence), 0) + 1 FROM %s.taxonomies WHERE stream_ref = $1`, SchemaName),
		parentRef)
	if err != nil {
		return 0, err
	}
	if len(rs.Rows) == 0 {
		return 1, nil
	}
	val, ok := rs.Rows[0][0].(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected group_sequence type: %T", rs.Rows[0][0])
	}
	return int(val), nil
}

// dbInsertTaxonomyRow inserts a single taxonomy row within a transaction.
func (ext *Extension) dbInsertTaxonomyRow(ctx context.Context, tx sql.Tx, taxonomyID string, parentRef int64, childRef int64, weight string, startDate int64, groupSeq int, createdAt int64) error {
	_, err := tx.Execute(ctx, fmt.Sprintf(
		`INSERT INTO %s.taxonomies (taxonomy_id, stream_ref, child_stream_ref, weight, start_time, group_sequence, created_at)
		 VALUES ($1::UUID, $2, $3, $4, $5, $6, $7)`, SchemaName),
		taxonomyID, parentRef, childRef, weight, startDate, groupSeq, createdAt)
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
