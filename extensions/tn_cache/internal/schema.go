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
			base_time INT8,
			from_timestamp INT8,
			cache_refreshed_at_timestamp INT8,
			cache_height INT8,
			cron_schedule TEXT,
			base_time_key INT8 GENERATED ALWAYS AS (COALESCE(base_time, %d)) STORED,
			PRIMARY KEY (data_provider, stream_id, base_time_key)
		)`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)

	if _, err := tx.Execute(ctx, createTable); err != nil {
		return fmt.Errorf("create cached_streams table: %w", err)
	}

	addBaseTimeColumn := fmt.Sprintf(`
		ALTER TABLE %s.cached_streams
		ADD COLUMN IF NOT EXISTS base_time INT8`, constants.CacheSchemaName)
	if _, err := tx.Execute(ctx, addBaseTimeColumn); err != nil {
		return fmt.Errorf("add base_time to cached_streams: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_streams
		ALTER COLUMN base_time DROP DEFAULT`); err != nil {
		return fmt.Errorf("drop cached_streams base_time default: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_streams
		ALTER COLUMN base_time DROP NOT NULL`); err != nil {
		return fmt.Errorf("drop cached_streams base_time not null: %w", err)
	}

	updateLegacyNulls := fmt.Sprintf(`
		UPDATE %s.cached_streams
		SET base_time = NULL
		WHERE base_time = %d`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)
	if _, err := tx.Execute(ctx, updateLegacyNulls); err != nil {
		return fmt.Errorf("nullify cached_streams legacy sentinel base_time: %w", err)
	}

	addBaseTimeKeyColumn := fmt.Sprintf(`
		ALTER TABLE %s.cached_streams
		ADD COLUMN IF NOT EXISTS base_time_key INT8 GENERATED ALWAYS AS (COALESCE(base_time, %d)) STORED`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)
	if _, err := tx.Execute(ctx, addBaseTimeKeyColumn); err != nil {
		return fmt.Errorf("add cached_streams base_time_key: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_streams
		DROP CONSTRAINT IF EXISTS cached_streams_pkey`); err != nil {
		return fmt.Errorf("drop cached_streams primary key: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_streams
		ADD CONSTRAINT cached_streams_pkey PRIMARY KEY (data_provider, stream_id, base_time_key)`); err != nil {
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
	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.cached_events (
			data_provider TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			base_time INT8,
			event_time INT8 NOT NULL,
			value NUMERIC(36, 18) NOT NULL,
			base_time_key INT8 GENERATED ALWAYS AS (COALESCE(base_time, %d)) STORED,
			PRIMARY KEY (data_provider, stream_id, base_time_key, event_time)
		)`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)

	if _, err := tx.Execute(ctx, createTable); err != nil {
		return fmt.Errorf("create cached_events table: %w", err)
	}

	addBaseTimeColumn := fmt.Sprintf(`
		ALTER TABLE %s.cached_events
		ADD COLUMN IF NOT EXISTS base_time INT8`, constants.CacheSchemaName)
	if _, err := tx.Execute(ctx, addBaseTimeColumn); err != nil {
		return fmt.Errorf("add base_time to cached_events: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_events
		ALTER COLUMN base_time DROP DEFAULT`); err != nil {
		return fmt.Errorf("drop cached_events base_time default: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_events
		ALTER COLUMN base_time DROP NOT NULL`); err != nil {
		return fmt.Errorf("drop cached_events base_time not null: %w", err)
	}

	updateLegacyNulls := fmt.Sprintf(`
		UPDATE %s.cached_events
		SET base_time = NULL
		WHERE base_time = %d`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)
	if _, err := tx.Execute(ctx, updateLegacyNulls); err != nil {
		return fmt.Errorf("nullify cached_events legacy sentinel base_time: %w", err)
	}

	addBaseTimeKey := fmt.Sprintf(`
		ALTER TABLE %s.cached_events
		ADD COLUMN IF NOT EXISTS base_time_key INT8 GENERATED ALWAYS AS (COALESCE(base_time, %d)) STORED`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)
	if _, err := tx.Execute(ctx, addBaseTimeKey); err != nil {
		return fmt.Errorf("add cached_events base_time_key: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_events
		DROP CONSTRAINT IF EXISTS cached_events_pkey`); err != nil {
		return fmt.Errorf("drop cached_events primary key: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_events
		ADD CONSTRAINT cached_events_pkey PRIMARY KEY (data_provider, stream_id, base_time_key, event_time)`); err != nil {
		return fmt.Errorf("add cached_events primary key: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		DROP INDEX IF EXISTS `+constants.CacheSchemaName+`.idx_cached_events_time_range`); err != nil {
		return fmt.Errorf("drop cached_events index: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_events_time_range 
		ON `+constants.CacheSchemaName+`.cached_events (data_provider, stream_id, base_time_key, event_time)`); err != nil {
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
			base_time INT8,
			event_time INT8 NOT NULL,
			value NUMERIC(36, 18) NOT NULL,
			base_time_key INT8 GENERATED ALWAYS AS (COALESCE(base_time, %d)) STORED,
			PRIMARY KEY (data_provider, stream_id, base_time_key, event_time)
		)`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)

	if _, err := tx.Execute(ctx, createTable); err != nil {
		return fmt.Errorf("create cached_index_events table: %w", err)
	}

	addBaseTimeColumn := fmt.Sprintf(`
		ALTER TABLE %s.cached_index_events
		ADD COLUMN IF NOT EXISTS base_time INT8`, constants.CacheSchemaName)
	if _, err := tx.Execute(ctx, addBaseTimeColumn); err != nil {
		return fmt.Errorf("add base_time to cached_index_events: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_index_events
		ALTER COLUMN base_time DROP DEFAULT`); err != nil {
		return fmt.Errorf("drop cached_index_events base_time default: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_index_events
		ALTER COLUMN base_time DROP NOT NULL`); err != nil {
		return fmt.Errorf("drop cached_index_events base_time not null: %w", err)
	}

	updateLegacyNulls := fmt.Sprintf(`
		UPDATE %s.cached_index_events
		SET base_time = NULL
		WHERE base_time = %d`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)
	if _, err := tx.Execute(ctx, updateLegacyNulls); err != nil {
		return fmt.Errorf("nullify cached_index_events legacy sentinel base_time: %w", err)
	}

	addBaseTimeKey := fmt.Sprintf(`
		ALTER TABLE %s.cached_index_events
		ADD COLUMN IF NOT EXISTS base_time_key INT8 GENERATED ALWAYS AS (COALESCE(base_time, %d)) STORED`, constants.CacheSchemaName, constants.BaseTimeNoneSentinel)
	if _, err := tx.Execute(ctx, addBaseTimeKey); err != nil {
		return fmt.Errorf("add cached_index_events base_time_key: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_index_events
		DROP CONSTRAINT IF EXISTS cached_index_events_pkey`); err != nil {
		return fmt.Errorf("drop cached_index_events primary key: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		ALTER TABLE `+constants.CacheSchemaName+`.cached_index_events
		ADD CONSTRAINT cached_index_events_pkey PRIMARY KEY (data_provider, stream_id, base_time_key, event_time)`); err != nil {
		return fmt.Errorf("add cached_index_events primary key: %w", err)
	}

	dropIndex := fmt.Sprintf(`
		DROP INDEX IF EXISTS %s.idx_cached_index_events_time_range`, constants.CacheSchemaName)
	if _, err := tx.Execute(ctx, dropIndex); err != nil {
		return fmt.Errorf("drop cached_index_events index: %w", err)
	}

	if _, err := tx.Execute(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_index_events_time_range 
		ON `+constants.CacheSchemaName+`.cached_index_events (data_provider, stream_id, base_time_key, event_time)`); err != nil {
		return fmt.Errorf("create cached_index_events index: %w", err)
	}

	return nil
}
