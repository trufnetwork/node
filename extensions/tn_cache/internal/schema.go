package internal

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/constants"
)

// ensureCachedStreamsSchema creates or upgrades the cached_streams table so it supports base_time variants.
// It is idempotent: fresh installs create the table, while existing nodes gain the new column and primary key.
func ensureCachedStreamsSchema(ctx context.Context, tx sql.Tx) error {
	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.cached_streams (
			data_provider TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			base_time INT8 NOT NULL DEFAULT %d,
			from_timestamp INT8,
			cache_refreshed_at_timestamp INT8,
			cache_height INT8,
			cron_schedule TEXT,
			PRIMARY KEY (data_provider, stream_id, base_time)
		)`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)

	if _, err := tx.Execute(ctx, createTable); err != nil {
		return fmt.Errorf("create cached_streams table: %w", err)
	}

	addColumn := fmt.Sprintf(`
		ALTER TABLE %s.cached_streams
		ADD COLUMN IF NOT EXISTS base_time INT8 NOT NULL DEFAULT %d`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)
	if _, err := tx.Execute(ctx, addColumn); err != nil {
		return fmt.Errorf("add base_time to cached_streams: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_streams
		DROP CONSTRAINT IF EXISTS cached_streams_pkey`); err != nil {
		return fmt.Errorf("drop cached_streams primary key: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_streams
		ADD CONSTRAINT cached_streams_pkey PRIMARY KEY (data_provider, stream_id, base_time)`); err != nil {
		return fmt.Errorf("add cached_streams primary key: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_streams_cron_schedule 
		ON `+constants.CacheSchemaName+`.cached_streams (cron_schedule)`); err != nil {
		return fmt.Errorf("create cached_streams cron index: %w", err)
	}

	return nil
}

// ensureCachedEventsSchema guarantees the raw events table exists with the expected index.
func ensureCachedEventsSchema(ctx context.Context, tx sql.Tx) error {
	if _, err := tx.Execute(ctx, `
		CREATE TABLE IF NOT EXISTS `+constants.CacheSchemaName+`.cached_events (
			data_provider TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			event_time INT8 NOT NULL,
			value NUMERIC(36, 18) NOT NULL,
			PRIMARY KEY (data_provider, stream_id, event_time)
		)`); err != nil {
		return fmt.Errorf("create cached_events table: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_events_time_range 
		ON `+constants.CacheSchemaName+`.cached_events (data_provider, stream_id, event_time)`); err != nil {
		return fmt.Errorf("create cached_events index: %w", err)
	}

	return nil
}

// ensureCachedIndexEventsSchema creates or upgrades the derived-index cache table with base_time support.
func ensureCachedIndexEventsSchema(ctx context.Context, tx sql.Tx) error {
	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.cached_index_events (
			data_provider TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			base_time INT8 NOT NULL DEFAULT %d,
			event_time INT8 NOT NULL,
			value NUMERIC(36, 18) NOT NULL,
			PRIMARY KEY (data_provider, stream_id, base_time, event_time)
		)`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)

	if _, err := tx.Execute(ctx, createTable); err != nil {
		return fmt.Errorf("create cached_index_events table: %w", err)
	}

	addColumn := fmt.Sprintf(`
		ALTER TABLE %s.cached_index_events
		ADD COLUMN IF NOT EXISTS base_time INT8 NOT NULL DEFAULT %d`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)
	if _, err := tx.Execute(ctx, addColumn); err != nil {
		return fmt.Errorf("add base_time to cached_index_events: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_index_events
		DROP CONSTRAINT IF EXISTS cached_index_events_pkey`); err != nil {
		return fmt.Errorf("drop cached_index_events primary key: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_index_events
		ADD CONSTRAINT cached_index_events_pkey PRIMARY KEY (data_provider, stream_id, base_time, event_time)`); err != nil {
		return fmt.Errorf("add cached_index_events primary key: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		DROP INDEX IF EXISTS idx_cached_index_events_time_range`); err != nil {
		return fmt.Errorf("drop cached_index_events index: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_index_events_time_range 
		ON `+constants.CacheSchemaName+`.cached_index_events (data_provider, stream_id, base_time, event_time)`); err != nil {
		return fmt.Errorf("create cached_index_events index: %w", err)
	}

	return nil
}
