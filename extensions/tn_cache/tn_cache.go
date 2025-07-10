package tn_cache

import (
	"context"
	"fmt"
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
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
)

// Constants
const (
	ExtensionName = config.ExtensionName
)

var TNNumericType = &types.DataType{
	Name:     types.NumericStr,
	Metadata: [2]uint16{36, 18}, // precision, scale
}

// Global variables removed - now using Extension struct
/*
var (
	logger          log.Logger
	cacheDB         *internal.CacheDB
	scheduler       *CacheScheduler
	syncChecker     *SyncChecker // Monitors node sync status
	metricsRecorder metrics.MetricsRecorder
	cachePool       *pgxpool.Pool // Independent connection pool for cache operations
	isEnabled       bool          // Track if extension is enabled
)
*/

// getWrappedCacheDB returns a sql.DB interface wrapping the independent connection pool
// This is used instead of app.DB to avoid "tx is closed" errors
func getWrappedCacheDB() sql.DB {
	// Check for test injection first
	if db := getTestDB(); db != nil {
		return db
	}
	ext := GetExtension()
	if ext != nil && ext.CachePool() != nil {
		// Type assert to *pgxpool.Pool for production use
		if pool, ok := ext.CachePool().(*pgxpool.Pool); ok {
			return newPoolDBWrapper(pool)
		}
	}
	// This should never happen after initialization
	if ext != nil && ext.Logger() != nil {
		ext.Logger().Error("cache pool is nil, extension not properly initialized")
	}
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
	// Use temporary logger for parsing
	tempLogger := service.Logger.New("tn_cache")

	// Check for test configuration override first
	var extConfig map[string]string
	var ok bool

	if testConfig := getTestConfig(); testConfig != nil {
		extConfig = testConfig
		ok = true
		tempLogger.Debug("using test configuration override")
	} else {
		// Get extension configuration from the node config
		extConfig, ok = service.LocalConfig.Extensions[ExtensionName]
	}

	if !ok {
		// Extension is not configured, return default disabled config
		tempLogger.Debug("extension not configured, disabling",
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
		tempLogger.Error("failed to process configuration", "error", err)
		return nil, fmt.Errorf("configuration processing failed: %w", err)
	}

	tempLogger.Info("configuration processed successfully",
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
	// Initialize extension instance if not already done
	if GetExtension() == nil {
		SetExtension(&Extension{
			logger: service.Logger.New("tn_cache"),
		})
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
				Handler: HandleIsEnabled,
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
				Handler: HandleHasCachedData,
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
				Handler: HandleGetCachedData,
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
				Handler: HandleGetCachedLastBefore,
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
				Handler: HandleGetCachedFirstAfter,
			},
			{
				Name:            "get_cached_index_data",
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
				Handler: HandleGetCachedIndexData,
			},
		},
	}, nil
}

// engineReadyHook is called when the engine is ready
// This is where we initialize our extension
func engineReadyHook(ctx context.Context, app *common.App) error {
	processedConfig, err := ParseConfig(app.Service)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Get the extension instance
	ext := GetExtension()
	if ext == nil {
		// In some environments (like tests), initializeExtension might not be called
		// Create a minimal extension instance
		logger := app.Service.Logger.New("tn_cache")
		SetExtension(&Extension{
			logger: logger,
		})
		ext = GetExtension()
	}

	// Initialize metrics recorder (with auto-detection)
	metricsRecorder := metrics.NewMetricsRecorder(ext.logger)

	// Update the extension with enabled state
	ext.isEnabled = processedConfig.Enabled

	// If disabled, ensure schema is cleaned up
	if !processedConfig.Enabled {
		ext.logger.Info("extension is disabled")
		// In test environments, we might not have proper DB config
		// Try to create a pool for cleanup, but don't fail if we can't
		tempPool, err := createIndependentConnectionPool(ctx, app.Service, ext.logger)
		if err != nil {
			// If we can't create a pool, we can't clean up - just log and return
			ext.logger.Debug("skipping schema cleanup - no database connection", "error", err)
			return nil
		}
		defer tempPool.Close()

		// Only try to clean up if we have a valid pool
		ext.logger.Info("cleaning up any existing schema")
		return cleanupExtensionSchema(ctx, tempPool, ext.logger)
	}

	ext.logger.Info("initializing extension",
		"enabled", processedConfig.Enabled,
		"directives_count", len(processedConfig.Directives),
		"sources", processedConfig.Sources)

	// Check if test has injected a DB - if so, skip pool creation
	if pool := getTestDBPool(); pool != nil {
		ext.logger.Info("using test-injected database connection")
		ext.cacheDB = internal.NewCacheDB(pool, ext.logger)
		// Skip pool creation and database ready check
	} else {
		// Create independent connection pool for cache operations
		// This prevents "tx is closed" errors caused by kwil-db's connection lifecycle
		pool, err := createIndependentConnectionPool(ctx, app.Service, ext.logger)
		if err != nil {
			return fmt.Errorf("failed to create connection pool: %w", err)
		}
		ext.cachePool = pool

		// Create the CacheDB instance with our independent pool
		ext.cacheDB = internal.NewCacheDB(pool, ext.logger)

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
		ext.logger.Info("skipping schema setup in test mode")
	} else {
		// Initialize extension resources using the pool for schema setup
		if pool, ok := ext.cachePool.(*pgxpool.Pool); ok {
			err = setupCacheSchema(ctx, pool, ext.logger)
			if err != nil {
				pool.Close()
				return fmt.Errorf("failed to setup cache schema: %w", err)
			}
		} else {
			return fmt.Errorf("cache pool is not a *pgxpool.Pool")
		}
	}

	// Initialize scheduler if we have directives
	if len(processedConfig.Directives) > 0 {
		ext.scheduler = NewCacheScheduler(app, ext.cacheDB, ext.logger, metricsRecorder)

		// Initialize sync checker for sync-aware caching
		ext.syncChecker = NewSyncChecker(ext.logger, processedConfig.MaxBlockAge)
		ext.syncChecker.Start(ctx)
		ext.scheduler.SetSyncChecker(ext.syncChecker)

		if processedConfig.MaxBlockAge > 0 {
			ext.logger.Info("sync-aware caching enabled", "max_block_age", processedConfig.MaxBlockAge)
		}

		if err := ext.scheduler.Start(ctx, processedConfig); err != nil {
			return fmt.Errorf("failed to start scheduler: %w", err)
		}

		// Record initial gauge metrics
		metricsRecorder.RecordStreamConfigured(ctx, len(processedConfig.Directives))

		// Query actual active streams on startup (cache persists across restarts)
		ext.scheduler.updateGaugeMetrics(ctx)

		// Start a goroutine to handle graceful shutdown when context is cancelled
		go func() {
			<-ctx.Done()
			ext.logger.Info("context cancelled, stopping extension")

			// Stop sync checker first
			if ext.syncChecker != nil {
				ext.syncChecker.Stop()
				ext.logger.Info("stopped sync checker")
			}

			// Stop scheduler
			if ext.scheduler != nil {
				if err := ext.scheduler.Stop(); err != nil {
					ext.logger.Error("error stopping scheduler", "error", err)
				}
			}

			// Close connection pool
			if ext.cachePool != nil {
				if pool, ok := ext.cachePool.(*pgxpool.Pool); ok {
					pool.Close()
					ext.logger.Info("closed cache connection pool")
				}
			}
		}()
	}

	// Update the extension with all initialized components
	ext.metricsRecorder = metricsRecorder

	return nil
}

// endBlockHook is called at the end of each block
func endBlockHook(ctx context.Context, app *common.App, block *common.BlockContext) error {
	// This hook can be used for block-based processing or metrics collection
	// For now, we're not doing anything at end of block
	return nil
}

// setupCacheSchema creates the necessary database schema for the cache
func setupCacheSchema(ctx context.Context, pool *pgxpool.Pool, logger log.Logger) error {
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

	// Create cached_index_events table for storing pre-calculated index values
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+constants.CacheSchemaName+`.cached_index_events (
			data_provider TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			event_time INT8 NOT NULL,
			value NUMERIC(36, 18) NOT NULL,
			PRIMARY KEY (data_provider, stream_id, event_time)
		)`); err != nil {
		return fmt.Errorf("create cached_index_events table: %w", err)
	}

	// Create index for efficiently retrieving index events by time range
	if _, err := tx.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cached_index_events_time_range 
		ON `+constants.CacheSchemaName+`.cached_index_events (data_provider, stream_id, event_time)`); err != nil {
		return fmt.Errorf("create index event time range index: %w", err)
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
func createIndependentConnectionPool(ctx context.Context, service *common.Service, logger log.Logger) (*pgxpool.Pool, error) {
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

func cleanupExtensionSchema(ctx context.Context, pool *pgxpool.Pool, logger log.Logger) error {
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
	ext := GetExtension()
	if ext == nil || ext.cacheDB == nil {
		return false, fmt.Errorf("cache extension not initialized")
	}
	return ext.cacheDB.HasCachedData(ctx, dataProvider, streamID, fromTime, toTime)
}

// GetCachedData retrieves cached data for a stream
func GetCachedData(ctx context.Context, dataProvider, streamID string, fromTime, toTime int64) ([]internal.CachedEvent, error) {
	ext := GetExtension()
	if ext == nil || ext.cacheDB == nil {
		return nil, fmt.Errorf("cache extension not initialized")
	}
	return ext.cacheDB.GetEvents(ctx, dataProvider, streamID, fromTime, toTime)
}
