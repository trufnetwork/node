package tn_cache

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	"github.com/trufnetwork/kwil-db/node/types/sql"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
)

// Use ExtensionName from constants
const ExtensionName = config.ExtensionName

var (
	logger    log.Logger
	cacheDB   *internal.CacheDB
	scheduler *CacheScheduler
)

// ParseConfig parses the extension configuration from the node's config file
func ParseConfig(service *common.Service) (*config.ProcessedConfig, error) {
	logger = service.Logger.New("tn_cache")

	// Get extension configuration from the node config
	extConfig, ok := service.LocalConfig.Extensions[ExtensionName]
	if !ok {
		// Extension is not configured, return default disabled config
		logger.Debug("extension not configured, disabling")
		return &config.ProcessedConfig{
			Enabled:    false,
			Directives: []config.CacheDirective{},
			Sources:    []string{},
		}, nil
	}

	// Use the new configuration loader to load and process the configuration
	loader := config.NewLoader()
	processedConfig, err := loader.LoadAndProcessFromMap(context.Background(), extConfig)
	if err != nil {
		logger.Error("failed to process configuration", "error", err)
		return nil, fmt.Errorf("configuration processing failed: %w", err)
	}

	logger.Info("configuration processed successfully",
		"enabled", processedConfig.Enabled,
		"directives_count", len(processedConfig.Directives),
		"sources", processedConfig.Sources)

	return processedConfig, nil
}

func init() {
	// Register engine ready hook
	err := hooks.RegisterEngineReadyHook(ExtensionName+"_engine_ready", engineReadyHook)
	if err != nil {
		panic(fmt.Sprintf("failed to register engine ready hook: %v", err))
	}

	// Register end block hook
	err = hooks.RegisterEndBlockHook(ExtensionName+"_end_block", endBlockHook)
	if err != nil {
		panic(fmt.Sprintf("failed to register end block hook: %v", err))
	}
}

// engineReadyHook is called when the engine is ready
// This is where we initialize our extension
func engineReadyHook(ctx context.Context, app *common.App) error {
	processedConfig, err := ParseConfig(app.Service)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Get the database from the app
	db, ok := app.DB.(sql.DB)
	if !ok {
		return fmt.Errorf("app.DB is not a sql.DB")
	}

	// If disabled, ensure schema is cleaned up
	if !processedConfig.Enabled {
		logger.Info("extension is disabled, cleaning up any existing schema")
		return cleanupExtensionSchema(ctx, db)
	}

	logger.Info("initializing extension",
		"enabled", processedConfig.Enabled,
		"directives_count", len(processedConfig.Directives),
		"sources", processedConfig.Sources)

	// Create the CacheDB instance
	cacheDB = internal.NewCacheDB(db, logger)

	// Initialize extension resources
	err = setupCacheSchema(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to setup cache schema: %w", err)
	}

	// Register SQL functions
	err = registerSQLFunctions(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to register SQL functions: %w", err)
	}

	// Initialize scheduler if we have directives
	if len(processedConfig.Directives) > 0 {
		scheduler = NewCacheScheduler(app, cacheDB, logger)
		if err := scheduler.Start(ctx, processedConfig); err != nil {
			return fmt.Errorf("failed to start scheduler: %w", err)
		}

		// Start a goroutine to handle graceful shutdown when context is cancelled
		go func() {
			<-ctx.Done()
			logger.Info("context cancelled, stopping scheduler")
			if err := scheduler.Stop(); err != nil {
				logger.Error("error stopping scheduler", "error", err)
			}
		}()
	}

	return nil
}

// endBlockHook is called at the end of each block
func endBlockHook(ctx context.Context, app *common.App, block *common.BlockContext) error {
	// This hook can be used for block-based processing or metrics collection
	// For now, we're not doing anything at end of block
	return nil
}

// setupCacheSchema creates the necessary database schema for the cache
func setupCacheSchema(ctx context.Context, db sql.DB) error {
	logger.Info("setting up cache schema")

	// Begin a transaction to ensure atomicity of schema creation
	tx, err := db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// Create schema - private schema not prefixed with ds_, ignored by consensus
	if _, err := tx.Execute(ctx, `CREATE SCHEMA IF NOT EXISTS ext_tn_cache`); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	// Create cached_streams table
	if _, err := tx.Execute(ctx, `
		CREATE TABLE IF NOT EXISTS ext_tn_cache.cached_streams (
			data_provider TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			from_timestamp INT8,
			last_refreshed TEXT,
			cron_schedule TEXT,
			PRIMARY KEY (data_provider, stream_id)
		)`); err != nil {
		return fmt.Errorf("create cached_streams table: %w", err)
	}

	// Create index for efficient querying by cron schedule
	if _, err := tx.Execute(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_streams_cron_schedule 
		ON ext_tn_cache.cached_streams (cron_schedule)`); err != nil {
		return fmt.Errorf("create cron schedule index: %w", err)
	}

	// Create cached_events table
	if _, err := tx.Execute(ctx, `
		CREATE TABLE IF NOT EXISTS ext_tn_cache.cached_events (
			data_provider TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			event_time INT8 NOT NULL,
			value NUMERIC(36, 18) NOT NULL,
			PRIMARY KEY (data_provider, stream_id, event_time)
		)`); err != nil {
		return fmt.Errorf("create cached_events table: %w", err)
	}

	// Create index for efficiently retrieving events by time range
	if _, err := tx.Execute(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_events_time_range 
		ON ext_tn_cache.cached_events (data_provider, stream_id, event_time)`); err != nil {
		return fmt.Errorf("create event time range index: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	logger.Info("cache schema setup complete")
	return nil
}

// cleanupExtensionSchema removes the cache schema when the extension is disabled
func cleanupExtensionSchema(ctx context.Context, db sql.DB) error {
	logger.Info("cleaning up cache schema")

	tx, err := db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// Drop schema CASCADE to remove all tables and indexes
	if _, err := tx.Execute(ctx, `DROP SCHEMA IF EXISTS ext_tn_cache CASCADE`); err != nil {
		return fmt.Errorf("drop schema: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	logger.Info("cache schema cleaned up successfully")
	return nil
}

// HasCachedData checks if there is cached data for a stream in a time range
func HasCachedData(ctx context.Context, dataProvider, streamID string, fromTime, toTime int64) (bool, error) {
	if cacheDB == nil {
		return false, fmt.Errorf("cache extension not initialized")
	}
	return cacheDB.HasCachedData(ctx, dataProvider, streamID, fromTime, toTime)
}

// GetCachedData retrieves cached data for a stream
func GetCachedData(ctx context.Context, dataProvider, streamID string, fromTime, toTime int64) ([]internal.CachedEvent, error) {
	if cacheDB == nil {
		return nil, fmt.Errorf("cache extension not initialized")
	}
	return cacheDB.GetEvents(ctx, dataProvider, streamID, fromTime, toTime)
}

// registerSQLFunctions registers all SQL functions for the tn_cache extension
func registerSQLFunctions(ctx context.Context, db sql.DB) error {
	logger.Info("registering SQL functions")

	tx, err := db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// Register is_enabled function
	if _, err := tx.Execute(ctx, `
		CREATE OR REPLACE FUNCTION ext_tn_cache.is_enabled()
		RETURNS BOOLEAN
		LANGUAGE plpgsql
		STABLE
		AS $$
		BEGIN
			-- Check if running in a write transaction - ext_tn_cache functions are read-only
			IF pg_catalog.current_setting('transaction_read_only')::boolean = FALSE THEN
				RAISE EXCEPTION 'ext_tn_cache functions cannot be used in write transactions';
			END IF;
			
			-- Since this function exists and is callable, the cache is enabled
			RETURN TRUE;
		END;
		$$;
	`); err != nil {
		return fmt.Errorf("failed to create is_enabled function: %w", err)
	}

	// Register has_cached_data function
	if _, err := tx.Execute(ctx, `
		CREATE OR REPLACE FUNCTION ext_tn_cache.has_cached_data(
			p_data_provider TEXT,
			p_stream_id TEXT,
			p_from_time BIGINT,
			p_to_time BIGINT
		)
		RETURNS BOOLEAN
		LANGUAGE plpgsql
		STABLE
		AS $$
		DECLARE
			event_count INTEGER := 0;
		BEGIN
			-- Check if running in a write transaction - ext_tn_cache functions are read-only
			IF pg_catalog.current_setting('transaction_read_only')::boolean = FALSE THEN
				RAISE EXCEPTION 'ext_tn_cache functions cannot be used in write transactions';
			END IF;
			
			-- Normalize data provider to lowercase
			p_data_provider := LOWER(p_data_provider);
			
			-- Validate input parameters
			IF p_data_provider IS NULL OR p_stream_id IS NULL OR p_from_time IS NULL THEN
				RAISE EXCEPTION 'data_provider, stream_id, and from_time cannot be NULL';
			END IF;
			
			-- Check if data exists in the cache
			IF p_to_time IS NULL THEN
				-- No upper bound specified
				SELECT COUNT(*) INTO event_count
				FROM ext_tn_cache.cached_events
				WHERE data_provider = p_data_provider 
				AND stream_id = p_stream_id 
				AND event_time >= p_from_time;
			ELSE
				-- Upper bound specified
				SELECT COUNT(*) INTO event_count
				FROM ext_tn_cache.cached_events
				WHERE data_provider = p_data_provider 
				AND stream_id = p_stream_id 
				AND event_time >= p_from_time 
				AND event_time <= p_to_time;
			END IF;
			
			-- Return true if we have cached data
			RETURN event_count > 0;
		END;
		$$;
	`); err != nil {
		return fmt.Errorf("failed to create has_cached_data function: %w", err)
	}

	// Register get_cached_data function
	if _, err := tx.Execute(ctx, `
		CREATE OR REPLACE FUNCTION ext_tn_cache.get_cached_data(
			p_data_provider TEXT,
			p_stream_id TEXT,
			p_from_time BIGINT,
			p_to_time BIGINT
		)
		RETURNS TABLE(
			data_provider TEXT,
			stream_id TEXT,
			event_time BIGINT,
			value NUMERIC(36,18)
		)
		LANGUAGE plpgsql
		STABLE
		AS $$
		BEGIN
			-- Check if running in a write transaction - ext_tn_cache functions are read-only
			IF pg_catalog.current_setting('transaction_read_only')::boolean = FALSE THEN
				RAISE EXCEPTION 'ext_tn_cache functions cannot be used in write transactions';
			END IF;
			
			-- Normalize data provider to lowercase
			p_data_provider := LOWER(p_data_provider);
			
			-- Validate input parameters
			IF p_data_provider IS NULL OR p_stream_id IS NULL OR p_from_time IS NULL THEN
				RAISE EXCEPTION 'data_provider, stream_id, and from_time cannot be NULL';
			END IF;
			
			-- Return the cached data
			IF p_to_time IS NULL THEN
				-- No upper bound specified
				RETURN QUERY
				SELECT 
					e.data_provider,
					e.stream_id,
					e.event_time,
					e.value
				FROM ext_tn_cache.cached_events e
				WHERE e.data_provider = p_data_provider 
				AND e.stream_id = p_stream_id 
				AND e.event_time >= p_from_time
				ORDER BY e.event_time ASC;
			ELSE
				-- Upper bound specified
				RETURN QUERY
				SELECT 
					e.data_provider,
					e.stream_id,
					e.event_time,
					e.value
				FROM ext_tn_cache.cached_events e
				WHERE e.data_provider = p_data_provider 
				AND e.stream_id = p_stream_id 
				AND e.event_time >= p_from_time 
				AND e.event_time <= p_to_time
				ORDER BY e.event_time ASC;
			END IF;
		END;
		$$;
	`); err != nil {
		return fmt.Errorf("failed to create get_cached_data function: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	logger.Info("SQL functions registered successfully")
	return nil
}
