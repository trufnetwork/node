package tn_cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	"github.com/trufnetwork/kwil-db/node/types/sql"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/constants"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Use ExtensionName from constants
const ExtensionName = config.ExtensionName

var (
	logger          log.Logger
	cacheDB         *internal.CacheDB
	scheduler       *CacheScheduler
	metricsRecorder metrics.MetricsRecorder
	cachePool       *pgxpool.Pool // Independent connection pool for cache operations
	isEnabled       bool          // Track if extension is enabled
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
	// Register precompile functions
	err := precompiles.RegisterPrecompile(constants.PrecompileName, precompiles.Precompile{
		Methods: []precompiles.Method{
			{
				Name:            "is_enabled",
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Parameters:      []precompiles.PrecompileValue{},
				Returns: &precompiles.MethodReturn{
					IsTable: false,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("enabled", types.BoolType, false),
					},
				},
				Handler: handleIsEnabled,
			},
			{
				Name:            "has_cached_data",
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Parameters: []precompiles.PrecompileValue{
					precompiles.NewPrecompileValue("data_provider", types.TextType, false),
					precompiles.NewPrecompileValue("stream_id", types.TextType, false),
					precompiles.NewPrecompileValue("from_time", types.IntType, false),
					precompiles.NewPrecompileValue("to_time", types.IntType, true),
				},
				Returns: &precompiles.MethodReturn{
					IsTable: false,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("has_data", types.BoolType, false),
					},
				},
				Handler: handleHasCachedData,
			},
			{
				Name:            "get_cached_data",
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Parameters: []precompiles.PrecompileValue{
					precompiles.NewPrecompileValue("data_provider", types.TextType, false),
					precompiles.NewPrecompileValue("stream_id", types.TextType, false),
					precompiles.NewPrecompileValue("from_time", types.IntType, false),
					precompiles.NewPrecompileValue("to_time", types.IntType, true),
				},
				Returns: &precompiles.MethodReturn{
					IsTable: true,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("data_provider", types.TextType, false),
						precompiles.NewPrecompileValue("stream_id", types.TextType, false),
						precompiles.NewPrecompileValue("event_time", types.IntType, false),
						precompiles.NewPrecompileValue("value", types.NumericType, false),
					},
				},
				Handler: handleGetCachedData,
			},
		},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to register ext_tn_cache precompile: %v", err))
	}

	// Register engine ready hook
	err = hooks.RegisterEngineReadyHook(ExtensionName+"_engine_ready", engineReadyHook)
	if err != nil {
		panic(fmt.Sprintf("failed to register engine ready hook: %v", err))
	}

	// Register end block hook
	err = hooks.RegisterEndBlockHook(ExtensionName+"_end_block", endBlockHook)
	if err != nil {
		panic(fmt.Sprintf("failed to register end block hook: %v", err))
	}
}

// handleIsEnabled handles the is_enabled precompile method
func handleIsEnabled(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	// Return the actual enabled state from configuration
	return resultFn([]any{isEnabled})
}

// handleHasCachedData handles the has_cached_data precompile method
func handleHasCachedData(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) (err error) {
	// Check if extension is enabled
	if !isEnabled {
		return fmt.Errorf("tn_cache extension is not enabled")
	}

	// Extract parameters
	dataProvider := strings.ToLower(inputs[0].(string))
	streamID := inputs[1].(string)
	fromTime := inputs[2].(int64)

	var toTime *int64
	if len(inputs) > 3 && inputs[3] != nil {
		t := inputs[3].(int64)
		toTime = &t
	}

	// Add tracing
	attrs := []attribute.KeyValue{
		attribute.Int64("from", fromTime),
	}
	if toTime != nil {
		attrs = append(attrs, attribute.Int64("to", *toTime))
	}
	traceCtx, end := tracing.StreamOperation(ctx.TxContext.Ctx, tracing.OpCacheCheck, dataProvider, streamID, attrs...)
	defer func() {
		end(err)
	}()

	// Update context for tracing
	ctx.TxContext.Ctx = traceCtx

	// Check cached_streams table to see if we have this stream cached
	// and if the requested time range is within our cached range
	db, ok := app.DB.(sql.DB)
	if !ok {
		return fmt.Errorf("app.DB is not a sql.DB")
	}

	tx, err := db.BeginTx(ctx.TxContext.Ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx.TxContext.Ctx)

	// Check if stream is configured and has been refreshed
	var configuredFromTime *int64
	var lastRefreshed *string

	result, err := tx.Execute(ctx.TxContext.Ctx, `
		SELECT from_timestamp, last_refreshed
		FROM `+constants.CacheSchemaName+`.cached_streams
		WHERE data_provider = $1 AND stream_id = $2
	`, dataProvider, streamID)
	if err != nil {
		return fmt.Errorf("failed to query cached_streams: %w", err)
	}

	if len(result.Rows) == 0 {
		// Stream not configured for caching
		return resultFn([]any{false})
	}

	row := result.Rows[0]
	if row[0] != nil {
		t := row[0].(int64)
		configuredFromTime = &t
	}
	if row[1] != nil {
		s := row[1].(string)
		lastRefreshed = &s
	}

	// If stream hasn't been refreshed yet, no cached data
	if lastRefreshed == nil {
		return resultFn([]any{false})
	}

	// Check if requested from_time is within our cached range
	if configuredFromTime != nil && fromTime < *configuredFromTime {
		// Requested time is before what we have cached
		return resultFn([]any{false})
	}

	// At this point, we know the stream is configured and has been refreshed
	// Check if we actually have events in the requested range
	var eventCount int64
	if toTime == nil {
		// No upper bound specified
		result, err = tx.Execute(ctx.TxContext.Ctx, `
			SELECT COUNT(*) FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3
		`, dataProvider, streamID, fromTime)
	} else {
		// Upper bound specified
		result, err = tx.Execute(ctx.TxContext.Ctx, `
			SELECT COUNT(*) FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3 AND event_time <= $4
		`, dataProvider, streamID, fromTime, *toTime)
	}

	if err != nil {
		return fmt.Errorf("failed to count cached events: %w", err)
	}

	if len(result.Rows) > 0 {
		eventCount = result.Rows[0][0].(int64)
	}

	// Record cache hit/miss metric
	hasData := eventCount > 0
	if hasData {
		metricsRecorder.RecordCacheHit(ctx.TxContext.Ctx, dataProvider, streamID)

		// Calculate and record data age since we already have lastRefreshed
		if lastRefreshed != nil {
			if refreshTime, err := time.Parse(time.RFC3339, *lastRefreshed); err == nil {
				dataAge := time.Since(refreshTime).Seconds()
				metricsRecorder.RecordCacheDataAge(ctx.TxContext.Ctx, dataProvider, streamID, dataAge)
			}
		}
	} else {
		metricsRecorder.RecordCacheMiss(ctx.TxContext.Ctx, dataProvider, streamID)
	}

	return resultFn([]any{hasData})
}

// handleGetCachedData handles the get_cached_data precompile method
func handleGetCachedData(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	// Check if extension is enabled
	if !isEnabled {
		return fmt.Errorf("tn_cache extension is not enabled")
	}

	// Extract parameters
	dataProvider := strings.ToLower(inputs[0].(string))
	streamID := inputs[1].(string)
	fromTime := inputs[2].(int64)

	var toTime int64
	if len(inputs) > 3 && inputs[3] != nil {
		toTime = inputs[3].(int64)
	}

	// Add tracing
	attrs := []attribute.KeyValue{
		attribute.Int64("from", fromTime),
	}
	if toTime > 0 {
		attrs = append(attrs, attribute.Int64("to", toTime))
	}
	traceCtx, end := tracing.StreamOperation(ctx.TxContext.Ctx, tracing.OpCacheGet, dataProvider, streamID, attrs...)

	// Track results for hit/miss recording
	var rowCount int
	defer func() {
		end(nil)
		// Record hit/miss in span
		span := trace.SpanFromContext(traceCtx)
		span.SetAttributes(
			attribute.Bool("cache.hit", rowCount > 0),
			attribute.Int("cache.rows", rowCount),
		)
	}()

	// Update context for tracing
	ctx.TxContext.Ctx = traceCtx

	// Get database connection
	db, ok := app.DB.(sql.DB)
	if !ok {
		return fmt.Errorf("app.DB is not a sql.DB")
	}

	tx, err := db.BeginTx(ctx.TxContext.Ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx.TxContext.Ctx)

	var result *sql.ResultSet

	if toTime > 0 {
		// Query with upper bound, including anchor value
		result, err = tx.Execute(ctx.TxContext.Ctx, `
			WITH anchor_record AS (
				SELECT data_provider, stream_id, event_time, value
				FROM `+constants.CacheSchemaName+`.cached_events
				WHERE data_provider = $1 AND stream_id = $2
					AND event_time <= $3
				ORDER BY event_time DESC
				LIMIT 1
			),
			interval_records AS (
				SELECT data_provider, stream_id, event_time, value
				FROM `+constants.CacheSchemaName+`.cached_events
				WHERE data_provider = $1 AND stream_id = $2
					AND event_time > $3 AND event_time <= $4
			),
			combined_results AS (
				SELECT * FROM anchor_record
				UNION ALL
				SELECT * FROM interval_records
			)
			SELECT data_provider, stream_id, event_time, value
			FROM combined_results
			ORDER BY event_time ASC
		`, dataProvider, streamID, fromTime, toTime)
	} else {
		// Upper bound specified
		result, err = tx.Execute(ctx.TxContext.Ctx, `
			WITH anchor_record AS (
				SELECT data_provider, stream_id, event_time, value
				FROM `+constants.CacheSchemaName+`.cached_events
				WHERE data_provider = $1 AND stream_id = $2
					AND event_time <= $3
				ORDER BY event_time DESC
				LIMIT 1
			),
			interval_records AS (
				SELECT data_provider, stream_id, event_time, value
				FROM `+constants.CacheSchemaName+`.cached_events
				WHERE data_provider = $1 AND stream_id = $2
					AND event_time > $3
			),
			combined_results AS (
				SELECT * FROM anchor_record
				UNION ALL
				SELECT * FROM interval_records
			)
			SELECT data_provider, stream_id, event_time, value
			FROM combined_results
			ORDER BY event_time ASC
		`, dataProvider, streamID, fromTime)
	}

	if err != nil {
		return fmt.Errorf("failed to query cached events: %w", err)
	}

	// Record metrics for data served
	rowCount = len(result.Rows)
	if rowCount > 0 {
		metricsRecorder.RecordCacheDataServed(ctx.TxContext.Ctx, dataProvider, streamID, rowCount)
	}

	// Return each row via resultFn
	for _, row := range result.Rows {
		if err := resultFn(row); err != nil {
			return err
		}
	}

	return nil
}

// engineReadyHook is called when the engine is ready
// This is where we initialize our extension
func engineReadyHook(ctx context.Context, app *common.App) error {
	processedConfig, err := ParseConfig(app.Service)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Initialize metrics recorder (with auto-detection)
	metricsRecorder = metrics.NewMetricsRecorder(logger)

	// Store the enabled state globally
	isEnabled = processedConfig.Enabled

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

	// Wait for database to be ready before proceeding
	if err := waitForDatabaseReady(ctx, db, 30*time.Second); err != nil {
		return fmt.Errorf("database not ready: %w", err)
	}

	logger.Info("initializing extension",
		"enabled", processedConfig.Enabled,
		"directives_count", len(processedConfig.Directives),
		"sources", processedConfig.Sources)

	// Create independent connection pool for cache operations
	// This prevents "tx is closed" errors caused by kwil-db's connection lifecycle
	pool, err := createIndependentConnectionPool(ctx, app.Service)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}
	cachePool = pool

	// Create the CacheDB instance with our independent pool
	cacheDB = internal.NewCacheDB(pool, logger)

	// Initialize extension resources using the pool for schema setup
	err = setupCacheSchema(ctx, pool)
	if err != nil {
		cachePool.Close()
		return fmt.Errorf("failed to setup cache schema: %w", err)
	}

	// Initialize scheduler if we have directives
	if len(processedConfig.Directives) > 0 {
		scheduler = NewCacheScheduler(app, cacheDB, logger, metricsRecorder)
		if err := scheduler.Start(ctx, processedConfig); err != nil {
			return fmt.Errorf("failed to start scheduler: %w", err)
		}

		// Record initial gauge metrics
		metricsRecorder.RecordStreamConfigured(ctx, len(processedConfig.Directives))

		// Query actual active streams on startup (cache persists across restarts)
		scheduler.updateGaugeMetrics(ctx)

		// Start a goroutine to handle graceful shutdown when context is cancelled
		go func() {
			<-ctx.Done()
			logger.Info("context cancelled, stopping extension")

			// Stop scheduler first
			if scheduler != nil {
				if err := scheduler.Stop(); err != nil {
					logger.Error("error stopping scheduler", "error", err)
				}
			}

			// Close connection pool
			if cachePool != nil {
				cachePool.Close()
				logger.Info("closed cache connection pool")
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
func setupCacheSchema(ctx context.Context, pool *pgxpool.Pool) error {
	logger.Info("setting up cache schema")

	// Begin a transaction to ensure atomicity of schema creation
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create schema - private schema not prefixed with ds_, ignored by consensus
	if _, err := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+constants.CacheSchemaName); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	// Create cached_streams table
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+constants.CacheSchemaName+`.cached_streams (
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
	if _, err := tx.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_streams_cron_schedule 
		ON `+constants.CacheSchemaName+`.cached_streams (cron_schedule)`); err != nil {
		return fmt.Errorf("create cron schedule index: %w", err)
	}

	// Create cached_events table
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+constants.CacheSchemaName+`.cached_events (
			data_provider TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			event_time INT8 NOT NULL,
			value NUMERIC(36, 18) NOT NULL,
			PRIMARY KEY (data_provider, stream_id, event_time)
		)`); err != nil {
		return fmt.Errorf("create cached_events table: %w", err)
	}

	// Create index for efficiently retrieving events by time range
	if _, err := tx.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_events_time_range 
		ON `+constants.CacheSchemaName+`.cached_events (data_provider, stream_id, event_time)`); err != nil {
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
// waitForDatabaseReady validates that the database connection is stable before proceeding
func waitForDatabaseReady(ctx context.Context, db sql.DB, maxWait time.Duration) error {
	timeout := time.NewTimer(maxWait)
	defer timeout.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("database not ready after %v", maxWait)
		case <-ticker.C:
			// Test database readiness with a simple transaction
			tx, err := db.BeginTx(ctx)
			if err != nil {
				continue // Keep trying
			}
			_, err = tx.Execute(ctx, "SELECT 1")
			if err != nil {
				tx.Rollback(ctx)
				continue // Keep trying
			}
			tx.Rollback(ctx)
			return nil // Database is ready
		}
	}
}

// createIndependentConnectionPool creates a dedicated connection pool for cache operations
func createIndependentConnectionPool(ctx context.Context, service *common.Service) (*pgxpool.Pool, error) {
	dbConfig := service.LocalConfig.DB

	// Build connection string using same parameters as main database
	connStr := fmt.Sprintf("host=%s port=%s user=%s database=%s sslmode=disable",
		dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.DBName)

	if dbConfig.Pass != "" {
		connStr += " password=" + dbConfig.Pass
	}

	// Parse configuration to customize pool settings
	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}

	// Configure pool specifically for cache operations
	poolConfig.MaxConns = 10 // Dedicated connections for cache operations
	poolConfig.MinConns = 2  // Keep minimum connections ready
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute

	// Create the pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	// Test the connection
	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("test connection: %w", err)
	}
	conn.Release()

	logger.Info("created independent connection pool for cache operations",
		"max_conns", poolConfig.MaxConns,
		"min_conns", poolConfig.MinConns)

	return pool, nil
}

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
	if _, err := tx.Execute(ctx, `DROP SCHEMA IF EXISTS `+constants.CacheSchemaName+` CASCADE`); err != nil {
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
