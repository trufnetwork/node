package tn_local

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// setupLocalSchema creates the ext_tn_local schema and all tables.
// Tables mirror the normalized consensus schema (post migration 017).
func setupLocalSchema(ctx context.Context, tx sql.Tx) error {
	if _, err := tx.Execute(ctx, `CREATE SCHEMA IF NOT EXISTS `+SchemaName); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	if err := ensureStreamsTable(ctx, tx); err != nil {
		return err
	}

	if err := ensurePrimitiveEventsTable(ctx, tx); err != nil {
		return err
	}

	if err := ensureTaxonomiesTable(ctx, tx); err != nil {
		return err
	}

	return nil
}

// ensureStreamsTable creates the streams table.
// Mirrors: consensus streams table after 017-normalize-tables.sql
func ensureStreamsTable(ctx context.Context, tx sql.Tx) error {
	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.streams (
			id SERIAL PRIMARY KEY,
			data_provider TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			stream_type TEXT NOT NULL,
			created_at INT8 NOT NULL,
			UNIQUE(data_provider, stream_id),
			CHECK (stream_type IN ('primitive', 'composed'))
		)`, SchemaName)

	if _, err := tx.Execute(ctx, createTable); err != nil {
		return fmt.Errorf("create streams table: %w", err)
	}
	return nil
}

// ensurePrimitiveEventsTable creates the primitive_events table with an index.
// Mirrors: consensus primitive_events after 017-normalize-tables.sql
func ensurePrimitiveEventsTable(ctx context.Context, tx sql.Tx) error {
	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.primitive_events (
			stream_ref INT NOT NULL REFERENCES %s.streams(id) ON DELETE CASCADE,
			event_time INT8 NOT NULL,
			value NUMERIC(36,18) NOT NULL,
			created_at INT8 NOT NULL DEFAULT 0,
			PRIMARY KEY (stream_ref, event_time, created_at)
		)`, SchemaName, SchemaName)

	if _, err := tx.Execute(ctx, createTable); err != nil {
		return fmt.Errorf("create primitive_events table: %w", err)
	}

	// Non-unique index mirrors consensus primitive_events_query_idx (017-normalize-tables.sql:105).
	// Multiple rows per (stream_ref, event_time) are allowed — created_at provides versioning.
	createIndex := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS local_pe_stream_time_idx
		ON %s.primitive_events (stream_ref, event_time)`, SchemaName)

	if _, err := tx.Execute(ctx, createIndex); err != nil {
		return fmt.Errorf("create primitive_events index: %w", err)
	}
	return nil
}

// ensureTaxonomiesTable creates the taxonomies table.
// Mirrors: consensus taxonomies after 017-normalize-tables.sql
// Local composed streams can ONLY reference other local streams.
func ensureTaxonomiesTable(ctx context.Context, tx sql.Tx) error {
	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.taxonomies (
			taxonomy_id UUID PRIMARY KEY,
			stream_ref INT NOT NULL REFERENCES %s.streams(id) ON DELETE CASCADE,
			child_stream_ref INT NOT NULL REFERENCES %s.streams(id) ON DELETE CASCADE,
			weight NUMERIC(36,18) NOT NULL,
			start_time INT8 NOT NULL,
			group_sequence INT NOT NULL DEFAULT 0,
			created_at INT8 NOT NULL,
			disabled_at INT8,
			CHECK (weight >= 0),
			CHECK (group_sequence >= 0),
			CHECK (start_time >= 0)
		)`, SchemaName, SchemaName, SchemaName)

	if _, err := tx.Execute(ctx, createTable); err != nil {
		return fmt.Errorf("create taxonomies table: %w", err)
	}
	return nil
}
