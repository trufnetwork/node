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

	// Migrate existing non-unique index to unique if needed.
	if err := migrateToUniqueEventIndex(ctx, tx); err != nil {
		return err
	}

	createIndex := fmt.Sprintf(`
		CREATE UNIQUE INDEX IF NOT EXISTS local_pe_stream_time_idx
		ON %s.primitive_events (stream_ref, event_time)`, SchemaName)

	if _, err := tx.Execute(ctx, createIndex); err != nil {
		return fmt.Errorf("create primitive_events index: %w", err)
	}
	return nil
}

// migrateToUniqueEventIndex detects a pre-existing non-unique local_pe_stream_time_idx,
// deduplicates rows that would violate uniqueness on (stream_ref, event_time),
// drops the old index, and lets the caller recreate it as UNIQUE.
func migrateToUniqueEventIndex(ctx context.Context, tx sql.Tx) error {
	// Step 1: check if the index exists and is non-unique.
	rs, err := tx.Execute(ctx, `
		SELECT NOT i.indisunique
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = 'local_pe_stream_time_idx'`)
	if err != nil {
		return fmt.Errorf("check index uniqueness: %w", err)
	}

	// Index does not exist yet (fresh install) — nothing to migrate.
	if len(rs.Rows) == 0 {
		return nil
	}

	isNonUnique, ok := rs.Rows[0][0].(bool)
	if !ok || !isNonUnique {
		// Already unique — nothing to do.
		return nil
	}

	// Step 2: deduplicate — keep the row with the latest created_at per (stream_ref, event_time).
	if _, err := tx.Execute(ctx, fmt.Sprintf(`
		DELETE FROM %s.primitive_events pe1
		WHERE EXISTS (
			SELECT 1 FROM %s.primitive_events pe2
			WHERE pe2.stream_ref = pe1.stream_ref
			  AND pe2.event_time = pe1.event_time
			  AND pe2.created_at > pe1.created_at
		)`, SchemaName, SchemaName)); err != nil {
		return fmt.Errorf("deduplicate primitive_events: %w", err)
	}

	// Step 3: drop the non-unique index.
	if _, err := tx.Execute(ctx, fmt.Sprintf(
		`DROP INDEX IF EXISTS %s.local_pe_stream_time_idx`, SchemaName)); err != nil {
		return fmt.Errorf("drop non-unique index: %w", err)
	}

	// Step 4: caller recreates as CREATE UNIQUE INDEX.
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
