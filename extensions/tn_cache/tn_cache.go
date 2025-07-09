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
	kwilconfig "github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/constants"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Constants
const (
	ExtensionName = config.ExtensionName

	// SQL constants
	maxInt8          = int64(9223372036854775000) // Default for NULL upper bounds
	numericPrecision = 36
	numericScale     = 18

	// Error messages
	errExtensionNotEnabled   = "tn_cache extension is not enabled"
	errCacheDBNotInitialized = "cache database not initialized"
	errValueNotDecimal       = "value is not a decimal. received %T"
)

var TNNumericType = &types.DataType{
	Name:     types.NumericStr,
	Metadata: [2]uint16{numericPrecision, numericScale},
}

var (
	logger          log.Logger
	cacheDB         *internal.CacheDB
	scheduler       *CacheScheduler
	syncChecker     *SyncChecker // Monitors node sync status
	metricsRecorder metrics.MetricsRecorder
	cachePool       *pgxpool.Pool // Independent connection pool for cache operations
	isEnabled       bool          // Track if extension is enabled
)

// getWrappedCacheDB returns a sql.DB interface wrapping the independent connection pool
// This is used instead of app.DB to avoid "tx is closed" errors
func getWrappedCacheDB() sql.DB {
	// Check for test injection first
	if db := getTestDB(); db != nil {
		return db
	}
	if cachePool != nil {
		return newPoolDBWrapper(cachePool)
	}
	// This should never happen after initialization
	logger.Error("cache pool is nil, extension not properly initialized")
	return nil
}

// getExtensionNames returns the names of configured extensions for debugging
func getExtensionNames(extensions map[string]map[string]string) []string {
	var names []string
	for name := range extensions {
		names = append(names, name)
	}
	return names
}

// ParseConfig parses the extension configuration from the node's config file
func ParseConfig(service *common.Service) (*config.ProcessedConfig, error) {
	if logger == nil {
		logger = service.Logger.New("tn_cache")
	}

	// Check for test configuration override first
	var extConfig map[string]string
	var ok bool

	if testConfig := getTestConfig(); testConfig != nil {
		extConfig = testConfig
		ok = true
		logger.Debug("using test configuration override")
	} else {
		// Get extension configuration from the node config
		extConfig, ok = service.LocalConfig.Extensions[ExtensionName]
	}

	if !ok {
		// Extension is not configured, return default disabled config
		logger.Debug("extension not configured, disabling",
			"extension_name", ExtensionName,
			"available_extensions", getExtensionNames(service.LocalConfig.Extensions))
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
	// Register precompile using initializer to receive metadata from test framework
	err := precompiles.RegisterInitializer(constants.PrecompileName, initializeExtension)
	if err != nil {
		panic(fmt.Sprintf("failed to register ext_tn_cache initializer: %v", err))
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

// initializeExtension is called by the framework with metadata during initialization
func initializeExtension(ctx context.Context, service *common.Service, db sql.DB, alias string, metadata map[string]any) (precompiles.Precompile, error) {
	// Initialize logger if not already done
	if logger == nil {
		logger = service.Logger.New("tn_cache")
	}

	// Note: We intentionally ignore metadata here because tn_cache is a node-level
	// extension, not a per-instance precompile. Configuration should come from
	// service.LocalConfig.Extensions in the engineReadyHook.

	// Return the precompile definition
	return precompiles.Precompile{
		// all the methods should be private, as the external user shouldn't be able to call directly
		// but there's some bug preventing internal calls to private methods (or is it private for the namespace?)
		//
		// these actions should be readonly, not to interfere with consensus
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
					precompiles.NewPrecompileValue("from_time", types.IntType, true), // nullable like standard queries
					precompiles.NewPrecompileValue("to_time", types.IntType, true),
				},
				Returns: &precompiles.MethodReturn{
					IsTable: false,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("has_data", types.BoolType, false),
						precompiles.NewPrecompileValue("cached_at", types.IntType, false),
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
					precompiles.NewPrecompileValue("from_time", types.IntType, true), // nullable like standard queries
					precompiles.NewPrecompileValue("to_time", types.IntType, true),
				},
				Returns: &precompiles.MethodReturn{
					IsTable: true,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("data_provider", types.TextType, false),
						precompiles.NewPrecompileValue("stream_id", types.TextType, false),
						precompiles.NewPrecompileValue("event_time", types.IntType, false),
						precompiles.NewPrecompileValue("value", TNNumericType, false),
					},
				},
				Handler: handleGetCachedData,
			},
			{
				Name:            "get_cached_last_before",
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Parameters: []precompiles.PrecompileValue{
					precompiles.NewPrecompileValue("data_provider", types.TextType, false),
					precompiles.NewPrecompileValue("stream_id", types.TextType, false),
					precompiles.NewPrecompileValue("before", types.IntType, true), // nullable
				},
				Returns: &precompiles.MethodReturn{
					IsTable: false,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("event_time", types.IntType, false),
						precompiles.NewPrecompileValue("value", TNNumericType, false),
					},
				},
				Handler: handleGetCachedLastBefore,
			},
			{
				Name:            "get_cached_first_after",
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Parameters: []precompiles.PrecompileValue{
					precompiles.NewPrecompileValue("data_provider", types.TextType, false),
					precompiles.NewPrecompileValue("stream_id", types.TextType, false),
					precompiles.NewPrecompileValue("after", types.IntType, true), // nullable
				},
				Returns: &precompiles.MethodReturn{
					IsTable: false,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("event_time", types.IntType, false),
						precompiles.NewPrecompileValue("value", TNNumericType, false),
					},
				},
				Handler: handleGetCachedFirstAfter,
			},
		},
	}, nil
}

// Common validation and helper functions
func checkExtensionEnabled() error {
	if !isEnabled {
		return fmt.Errorf(errExtensionNotEnabled)
	}
	return nil
}

func checkCacheDB() (sql.DB, error) {
	db := getWrappedCacheDB()
	if db == nil {
		return nil, fmt.Errorf(errCacheDBNotInitialized)
	}
	return db, nil
}

func extractTimeParameter(input interface{}) *int64 {
	if input == nil {
		return nil
	}
	t := input.(int64)
	return &t
}

func normalizeDataProvider(input string) string {
	return strings.ToLower(input)
}

func ensureDecimalValue(value interface{}) (*types.Decimal, error) {
	dec, ok := value.(*types.Decimal)
	if !ok {
		return nil, fmt.Errorf(errValueNotDecimal, value)
	}
	dec.SetPrecisionAndScale(numericPrecision, numericScale)
	return dec, nil
}

// processSingleRowResult handles the common pattern of returning a single (event_time, value) row
func processSingleRowResult(result *sql.ResultSet, resultFn func([]any) error) error {
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		dec, err := ensureDecimalValue(row[1])
		if err != nil {
			return err
		}
		return resultFn([]any{row[0], dec})
	}
	// No record found - this is not an error, just no data
	return nil
}

// createStreamOperationContext sets up tracing for stream operations
func createStreamOperationContext(ctx context.Context, op tracing.Operation, dataProvider, streamID string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	return tracing.StreamOperation(ctx, op, dataProvider, streamID, attrs...)
}

// buildTimeAttributes creates tracing attributes for time parameters
func buildTimeAttributes(fromTime, toTime *int64) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	if fromTime != nil {
		attrs = append(attrs, attribute.Int64("from", *fromTime))
	}
	if toTime != nil {
		attrs = append(attrs, attribute.Int64("to", *toTime))
	}
	return attrs
}

// handleIsEnabled handles the is_enabled precompile method
func handleIsEnabled(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	return resultFn([]any{isEnabled})
}

// fromTimeOrZero returns the fromTime value or 0 if nil
func fromTimeOrZero(fromTime *int64) int64 {
	if fromTime == nil {
		return 0
	}
	return *fromTime
}

// handleHasCachedData handles the has_cached_data precompile method
// handleHasCachedData checks if we have cached data for a stream in the given time range
// This is a READ-ONLY operation that queries the cache metadata
func handleHasCachedData(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) (err error) {
	if err := checkExtensionEnabled(); err != nil {
		return err
	}

	// Extract parameters
	dataProvider := normalizeDataProvider(inputs[0].(string))
	streamID := inputs[1].(string)
	fromTime := extractTimeParameter(inputs[2])
	toTime := extractTimeParameter(inputs[3])

	// Set up tracing
	attrs := buildTimeAttributes(fromTime, toTime)
	traceCtx, end := createStreamOperationContext(ctx.TxContext.Ctx, tracing.OpCacheCheck, dataProvider, streamID, attrs...)
	defer func() { end(err) }()
	ctx.TxContext.Ctx = traceCtx

	// Check cached_streams table to see if we have this stream cached
	// and if the requested time range is within our cached range
	db, ok := app.DB.(sql.DB)
	if !ok {
		return fmt.Errorf("app.DB is not a sql.DB")
	}

	// Check if stream is configured and has been refreshed
	var configuredFromTime *int64
	var lastRefreshed *int64
	var lastRefreshedTimestamp int64

	result, err := db.Execute(ctx.TxContext.Ctx, `
		SELECT 
			from_timestamp, 
			COALESCE(last_refreshed, 0) as last_refreshed
		FROM `+constants.CacheSchemaName+`.cached_streams
		WHERE data_provider = $1 AND stream_id = $2
	`, dataProvider, streamID)
	if err != nil {
		return fmt.Errorf("failed to query cached_streams: %w", err)
	}

	if len(result.Rows) == 0 {
		// Stream not configured for caching
		return resultFn([]any{false, int64(0)})
	}

	row := result.Rows[0]
	if row[0] != nil {
		t := row[0].(int64)
		configuredFromTime = &t
	}
	if row[1] != nil {
		lastRefreshedTimestamp = row[1].(int64)
		if lastRefreshedTimestamp > 0 {
			lastRefreshed = &lastRefreshedTimestamp
		}
	}

	// If stream hasn't been refreshed yet, no cached data
	if lastRefreshedTimestamp == 0 {
		return resultFn([]any{false, int64(0)})
	}

	// Special case: if both from and to are NULL, user wants latest value only
	// We can serve this if we have ANY data cached
	if fromTime == nil && toTime == nil {
		// Just check if we have any data at all
		result, err = db.Execute(ctx.TxContext.Ctx, `
			SELECT COUNT(*) > 0 FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2
			LIMIT 1
		`, dataProvider, streamID)
		if err != nil {
			return fmt.Errorf("failed to check for any cached events: %w", err)
		}
		hasData := false
		if len(result.Rows) > 0 && result.Rows[0][0] != nil {
			hasData = result.Rows[0][0].(bool)
		}
		return resultFn([]any{hasData, lastRefreshedTimestamp})
	}

	// If from is NULL but to is not, treat from as 0 (beginning of time)
	effectiveFrom := int64(0)
	if fromTime != nil {
		effectiveFrom = *fromTime
	}

	// Check if requested from_time is within our cached range
	// Only check if we have a configured from_time and the request specifies a from_time
	if configuredFromTime != nil && effectiveFrom < *configuredFromTime {
		// Requested time is before what we have cached
		return resultFn([]any{false, int64(0)})
	}

	// At this point, we know the stream is configured and has been refreshed
	// Check if we actually have events in the requested range
	var eventCount int64
	if toTime == nil {
		// No upper bound specified - to is treated as max_int8 (end of time)
		result, err = db.Execute(ctx.TxContext.Ctx, `
			SELECT COUNT(*) FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3
		`, dataProvider, streamID, effectiveFrom)
	} else {
		// Upper bound specified
		result, err = db.Execute(ctx.TxContext.Ctx, `
			SELECT COUNT(*) FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3 AND event_time <= $4
		`, dataProvider, streamID, effectiveFrom, *toTime)
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
		if lastRefreshed != nil && *lastRefreshed > 0 {
			refreshTime := time.Unix(*lastRefreshed, 0)
			dataAge := time.Since(refreshTime).Seconds()
			metricsRecorder.RecordCacheDataAge(ctx.TxContext.Ctx, dataProvider, streamID, dataAge)
		}
	} else {
		metricsRecorder.RecordCacheMiss(ctx.TxContext.Ctx, dataProvider, streamID)
	}

	return resultFn([]any{hasData, lastRefreshedTimestamp})
}

// handleGetCachedData handles the get_cached_data precompile method
func handleGetCachedData(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	if err := checkExtensionEnabled(); err != nil {
		return err
	}

	// Extract parameters
	dataProvider := normalizeDataProvider(inputs[0].(string))
	streamID := inputs[1].(string)
	fromTime := extractTimeParameter(inputs[2])
	toTime := extractTimeParameter(inputs[3])

	// Set up tracing
	attrs := buildTimeAttributes(fromTime, toTime)
	traceCtx, end := createStreamOperationContext(ctx.TxContext.Ctx, tracing.OpCacheGet, dataProvider, streamID, attrs...)

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
	db, err := checkCacheDB()
	if err != nil {
		return err
	}

	var result *sql.ResultSet

	// Special case: if both from and to are NULL, return latest value only
	if fromTime == nil && toTime == nil {
		result, err = db.Execute(ctx.TxContext.Ctx, `
			SELECT data_provider, stream_id, event_time, value
			FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2
			ORDER BY event_time DESC
			LIMIT 1
		`, dataProvider, streamID)
	} else if toTime != nil {
		// Both bounds specified OR from is NULL with to specified
		result, err = db.Execute(ctx.TxContext.Ctx, `
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
		`, dataProvider, streamID, fromTimeOrZero(fromTime), *toTime)
	} else {
		// Only from specified, to is NULL (treat as max_int8)
		result, err = db.Execute(ctx.TxContext.Ctx, `
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
		`, dataProvider, streamID, fromTimeOrZero(fromTime))
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
		// Ensure value is properly formatted
		_, err := ensureDecimalValue(row[3])
		if err != nil {
			return err
		}
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

	// If disabled, ensure schema is cleaned up
	if !processedConfig.Enabled {
		logger.Info("extension is disabled")
		// In test environments, we might not have proper DB config
		// Try to create a pool for cleanup, but don't fail if we can't
		tempPool, err := createIndependentConnectionPool(ctx, app.Service)
		if err != nil {
			// If we can't create a pool, we can't clean up - just log and return
			logger.Debug("skipping schema cleanup - no database connection", "error", err)
			return nil
		}
		defer tempPool.Close()

		// Only try to clean up if we have a valid pool
		logger.Info("cleaning up any existing schema")
		return cleanupExtensionSchema(ctx, tempPool)
	}

	logger.Info("initializing extension",
		"enabled", processedConfig.Enabled,
		"directives_count", len(processedConfig.Directives),
		"sources", processedConfig.Sources)

	// Check if test has injected a DB - if so, skip pool creation
	if pool := getTestDBPool(); pool != nil {
		logger.Info("using test-injected database connection")
		cacheDB = internal.NewCacheDB(pool, logger)
		// Skip pool creation and database ready check
	} else {
		// Create independent connection pool for cache operations
		// This prevents "tx is closed" errors caused by kwil-db's connection lifecycle
		pool, err := createIndependentConnectionPool(ctx, app.Service)
		if err != nil {
			return fmt.Errorf("failed to create connection pool: %w", err)
		}
		cachePool = pool

		// Create the CacheDB instance with our independent pool
		cacheDB = internal.NewCacheDB(pool, logger)
		
		// Wait for database to be ready before proceeding
		if err := waitForDatabaseReady(ctx, pool, 30*time.Second); err != nil {
			return fmt.Errorf("database not ready: %w", err)
		}
	}

	// For engine calls, we'll create transactions as needed rather than
	// trying to wrap the pool. This avoids the "cannot query with scan values" error

	// Initialize extension resources using the appropriate connection
	if getTestDB() != nil {
		// For tests, assume schema is already set up by test framework
		logger.Info("skipping schema setup in test mode")
	} else {
		// Initialize extension resources using the pool for schema setup
		err = setupCacheSchema(ctx, cachePool)
		if err != nil {
			cachePool.Close()
			return fmt.Errorf("failed to setup cache schema: %w", err)
		}
	}

	// Initialize scheduler if we have directives
	if len(processedConfig.Directives) > 0 {
		scheduler = NewCacheScheduler(app, cacheDB, logger, metricsRecorder)

		// Initialize sync checker for sync-aware caching
		syncChecker = NewSyncChecker(logger, processedConfig.MaxBlockAge)
		syncChecker.Start(ctx)
		scheduler.SetSyncChecker(syncChecker)

		if processedConfig.MaxBlockAge > 0 {
			logger.Info("sync-aware caching enabled", "max_block_age", processedConfig.MaxBlockAge)
		}

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

			// Stop sync checker first
			if syncChecker != nil {
				syncChecker.Stop()
				logger.Info("stopped sync checker")
			}

			// Stop scheduler
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
			last_refreshed INT8,
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
func waitForDatabaseReady(ctx context.Context, pool *pgxpool.Pool, maxWait time.Duration) error {
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
			_, err := pool.Exec(ctx, "SELECT 1")
			if err != nil {
				continue
			}
			return nil // Database is ready
		}
	}
}

// createIndependentConnectionPool creates a dedicated connection pool for cache operations
func createIndependentConnectionPool(ctx context.Context, service *common.Service) (*pgxpool.Pool, error) {
	// Check for test database configuration override first
	var dbConfig kwilconfig.DBConfig
	if testDBConfig := getTestDBConfig(); testDBConfig != nil {
		dbConfig = *testDBConfig
		logger.Debug("using test database configuration override")
	} else {
		dbConfig = service.LocalConfig.DB
	}

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

func cleanupExtensionSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		logger.Warn("cannot cleanup schema: pool is nil")
		return nil
	}

	logger.Info("cleaning up cache schema")

	tx, err := pool.Begin(ctx)
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
	if _, err = tx.Exec(ctx, `DROP SCHEMA IF EXISTS `+constants.CacheSchemaName+` CASCADE`); err != nil {
		return fmt.Errorf("drop schema: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
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

// handleGetCachedLastBefore handles the get_cached_last_before precompile method
// Returns the most recent record before a given timestamp
func handleGetCachedLastBefore(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	if err := checkExtensionEnabled(); err != nil {
		return err
	}

	// Extract parameters
	dataProvider := normalizeDataProvider(inputs[0].(string))
	streamID := inputs[1].(string)
	before := extractTimeParameter(inputs[2])

	// Set up tracing
	var attrs []attribute.KeyValue
	if before != nil {
		attrs = append(attrs, attribute.Int64("before", *before))
	}
	traceCtx, end := createStreamOperationContext(ctx.TxContext.Ctx, tracing.OpCacheGet, dataProvider, streamID, attrs...)
	defer end(nil)
	ctx.TxContext.Ctx = traceCtx

	// Get database connection
	db, err := checkCacheDB()
	if err != nil {
		return err
	}

	// Default to max_int8 if before is NULL
	effectiveBefore := maxInt8
	if before != nil {
		effectiveBefore = *before
	}

	// Query for the last record before the timestamp
	result, err := db.Execute(ctx.TxContext.Ctx, `
		SELECT event_time, value
		FROM `+constants.CacheSchemaName+`.cached_events
		WHERE data_provider = $1 AND stream_id = $2 AND event_time < $3
		ORDER BY event_time DESC
		LIMIT 1
	`, dataProvider, streamID, effectiveBefore)

	if err != nil {
		return fmt.Errorf("failed to query cached events: %w", err)
	}

	return processSingleRowResult(result, resultFn)
}

// handleGetCachedFirstAfter handles the get_cached_first_after precompile method
// Returns the earliest record after a given timestamp
func handleGetCachedFirstAfter(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	if err := checkExtensionEnabled(); err != nil {
		return err
	}

	// Extract parameters
	dataProvider := normalizeDataProvider(inputs[0].(string))
	streamID := inputs[1].(string)
	after := extractTimeParameter(inputs[2])

	// Set up tracing
	var attrs []attribute.KeyValue
	if after != nil {
		attrs = append(attrs, attribute.Int64("after", *after))
	}
	traceCtx, end := createStreamOperationContext(ctx.TxContext.Ctx, tracing.OpCacheGet, dataProvider, streamID, attrs...)
	defer end(nil)
	ctx.TxContext.Ctx = traceCtx

	// Get database connection
	db, err := checkCacheDB()
	if err != nil {
		return err
	}

	// Default to 0 if after is NULL
	effectiveAfter := int64(0)
	if after != nil {
		effectiveAfter = *after
	}

	// Query for the first record after the timestamp
	result, err := db.Execute(ctx.TxContext.Ctx, `
		SELECT event_time, value
		FROM `+constants.CacheSchemaName+`.cached_events
		WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3
		ORDER BY event_time ASC
		LIMIT 1
	`, dataProvider, streamID, effectiveAfter)

	if err != nil {
		return fmt.Errorf("failed to query cached events: %w", err)
	}

	return processSingleRowResult(result, resultFn)
}
