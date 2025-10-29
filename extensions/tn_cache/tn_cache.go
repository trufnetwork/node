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

	"github.com/jackc/pgx/v5/pgxpool" // for admin.Status
	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/constants"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
	"github.com/trufnetwork/node/extensions/tn_cache/scheduler"
	"github.com/trufnetwork/node/extensions/tn_cache/syncschecker"
	"github.com/trufnetwork/node/extensions/tn_cache/utilities"
)

// Constants
const (
	ExtensionName = config.ExtensionName
)

var TNNumericType = &types.DataType{
	Name:     types.NumericStr,
	Metadata: [2]uint16{36, 18}, // precision, scale
}

// ParseConfig parses the extension configuration from the node's config file
func ParseConfig(service *common.Service) (*config.ProcessedConfig, error) {
	// Use temporary logger for parsing
	tempLogger := service.Logger.New("tn_cache")

	// Check for test configuration override first
	var extConfig map[string]string
	var ok bool
	if testCfg := getTestConfig(); testCfg != nil {
		extConfig = testCfg
		// Using test configuration override
		ok = true
	} else {
		// Get extension configuration from the node config
		extConfig, ok = service.LocalConfig.Extensions[ExtensionName]
	}

	if !ok {
		// Extension is not configured, return default disabled config
		// Extension not configured, returning disabled config
		return &config.ProcessedConfig{
			Enabled:    false,
			Directives: []config.CacheDirective{},
			Sources:    []string{},
		}, nil
	}

	// Use the new configuration loader to load and process the configuration
	loader := config.NewLoader(tempLogger)
	processedConfig, err := loader.LoadAndProcessFromMap(context.Background(), extConfig)
	if err != nil {
		tempLogger.Error("failed to process configuration", "error", err)
		return nil, fmt.Errorf("configuration processing failed: %w", err)
	}

	// Configuration processed successfully

	return processedConfig, nil
}

// safeGetExtension returns the extension instance with proper error handling
func safeGetExtension() (*Extension, error) {
	ext := GetExtension()
	if ext == nil {
		return nil, fmt.Errorf("tn_cache extension not initialized")
	}
	if !ext.IsEnabled() {
		return nil, fmt.Errorf("tn_cache extension is disabled")
	}
	return ext, nil
}

func InitializeExtension() {
	// Register precompile using initializer to receive metadata from test framework. This should happen before the engine is ready.
	err := precompiles.RegisterInitializer(constants.PrecompileName, InitializeCachePrecompile)
	if err != nil {
		panic(fmt.Sprintf("failed to register ext_tn_cache initializer: %v", err))
	}

	// Register engine ready hook
	err = hooks.RegisterEngineReadyHook(ExtensionName+"_engine_ready", engineReadyHook)
	if err != nil {
		panic(fmt.Sprintf("failed to register engine ready hook: %v", err))
	}
}

// InitializeCachePrecompile is called by the framework with metadata during initialization
func InitializeCachePrecompile(ctx context.Context, service *common.Service, db sql.DB, alias string, metadata map[string]any) (precompiles.Precompile, error) {
	// Get the extension instance (lazy initialization ensures it exists)
	ext := GetExtension()

	if ext == nil {
		ext = &Extension{
			logger: service.Logger.New("tn_cache"),
		}
		SetExtension(ext)
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
		// Register both legacy and v2 precompile methods. The legacy names (no _v2)
		// keep the old 4-argument signature so pre-base_time deployments still work.
		// The *_v2 variants include the base_time parameter. Legacy methods are now
		// deprecated and will be removed in the next release.
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
						precompiles.NewPrecompileValue("cache_refreshed_at_timestamp", types.IntType, false),
						precompiles.NewPrecompileValue("cache_height", types.IntType, false),
					},
				},
				Handler: HandleHasCachedData,
			},
			{
				Name:            "has_cached_data_v2",
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Parameters: []precompiles.PrecompileValue{
					precompiles.NewPrecompileValue("data_provider", types.TextType, false),
					precompiles.NewPrecompileValue("stream_id", types.TextType, false),
					precompiles.NewPrecompileValue("from_time", types.IntType, true), // nullable like standard queries
					precompiles.NewPrecompileValue("to_time", types.IntType, true),
					precompiles.NewPrecompileValue("base_time", types.IntType, true),
				},
				Returns: &precompiles.MethodReturn{
					IsTable: false,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("has_data", types.BoolType, false),
						precompiles.NewPrecompileValue("cache_refreshed_at_timestamp", types.IntType, false),
						precompiles.NewPrecompileValue("cache_height", types.IntType, false),
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
				Name:            "get_cached_data_v2",
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Parameters: []precompiles.PrecompileValue{
					precompiles.NewPrecompileValue("data_provider", types.TextType, false),
					precompiles.NewPrecompileValue("stream_id", types.TextType, false),
					precompiles.NewPrecompileValue("from_time", types.IntType, true), // nullable like standard queries
					precompiles.NewPrecompileValue("to_time", types.IntType, true),
					precompiles.NewPrecompileValue("base_time", types.IntType, true),
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
				Name:            "get_cached_last_before_v2",
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Parameters: []precompiles.PrecompileValue{
					precompiles.NewPrecompileValue("data_provider", types.TextType, false),
					precompiles.NewPrecompileValue("stream_id", types.TextType, false),
					precompiles.NewPrecompileValue("before", types.IntType, true), // nullable
					precompiles.NewPrecompileValue("base_time", types.IntType, true),
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
				Name:            "get_cached_first_after_v2",
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Parameters: []precompiles.PrecompileValue{
					precompiles.NewPrecompileValue("data_provider", types.TextType, false),
					precompiles.NewPrecompileValue("stream_id", types.TextType, false),
					precompiles.NewPrecompileValue("after", types.IntType, true), // nullable
					precompiles.NewPrecompileValue("base_time", types.IntType, true),
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
						precompiles.NewPrecompileValue("event_time", types.IntType, false),
						precompiles.NewPrecompileValue("value", TNNumericType, false),
					},
				},
				Handler: HandleGetCachedIndexData,
			},
			{
				Name:            "get_cached_index_data_v2",
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Parameters: []precompiles.PrecompileValue{
					precompiles.NewPrecompileValue("data_provider", types.TextType, false),
					precompiles.NewPrecompileValue("stream_id", types.TextType, false),
					precompiles.NewPrecompileValue("from_time", types.IntType, true), // nullable like standard queries
					precompiles.NewPrecompileValue("to_time", types.IntType, true),
					precompiles.NewPrecompileValue("base_time", types.IntType, true),
				},
				Returns: &precompiles.MethodReturn{
					IsTable: true,
					Fields: []precompiles.PrecompileValue{
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
		app.Service.Logger.Info("tn_cache extension not initialized, skipping engine ready hook")
		return nil
	}

	ext, err = SetupCacheExtension(ctx, processedConfig, app.Engine, app.Service)
	if err != nil {
		return fmt.Errorf("failed to setup cache extension: %w", err)
	}

	SetExtension(ext)

	return nil
}

func SetupCacheExtension(ctx context.Context, config *config.ProcessedConfig, engine common.Engine, service *common.Service) (*Extension, error) {
	// Initialize metrics recorder (with auto-detection)
	metricsRecorder := metrics.NewMetricsRecorder(service.Logger)

	// Check for test overrides first
	var db sql.DB
	if testDB := getTestDB(); testDB != nil {
		db = testDB
		service.Logger.Info("using injected test database connection")
	} else {
		// Create independent connection pool for cache operations
		// This prevents "tx is closed" errors caused by kwil-db's connection lifecycle
		pool, err := createIndependentConnectionPool(ctx, service, service.Logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection pool: %w", err)
		}
		db = utilities.NewPoolDBWrapper(pool)
	}

	// If disabled, ensure schema is cleaned up
	if !config.Enabled {
		service.Logger.Info("extension is disabled")
		service.Logger.Info("cleaning up any existing schema")
		cacheDB := internal.NewCacheDB(db, service.Logger)
		return NewExtension(service.Logger, cacheDB, nil, nil, metricsRecorder, nil, db, config.Enabled), cacheDB.CleanupExtensionSchema(ctx)
	}

	service.Logger.Info("initializing extension",
		"enabled", config.Enabled,
		"directives_count", len(config.Directives),
		"sources", config.Sources)

	// if we have no directives, we can skip the rest of the initialization
	if len(config.Directives) == 0 {
		service.Logger.Warn("no directives found, skipping extension initialization")
		return NewExtension(service.Logger, nil, nil, nil, metricsRecorder, nil, db, config.Enabled), nil
	}

	// Create the CacheDB instance
	cacheDB := internal.NewCacheDB(db, service.Logger)

	// Wait for database to be ready before proceeding
	if err := cacheDB.WaitForDatabaseReady(ctx, 30*time.Second); err != nil {
		return nil, fmt.Errorf("database not ready: %w", err)
	}

	// Initialize extension resources
	err := cacheDB.SetupCacheSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to setup cache schema: %w", err)
	}

	engineOps := internal.NewEngineOperations(engine, db, "main", service.Logger)

	syncChecker := syncschecker.NewSyncChecker(service.Logger, config.MaxBlockAge)
	syncChecker.Start(ctx)

	scheduler := scheduler.NewCacheScheduler(scheduler.NewCacheSchedulerParams{
		Service:         service,
		CacheDB:         cacheDB,
		EngineOps:       engineOps,
		Logger:          service.Logger,
		MetricsRecorder: metricsRecorder,
		Namespace:       "",
		SyncChecker:     syncChecker,
	})

	if config.MaxBlockAge > 0 {
		service.Logger.Info("sync-aware caching enabled", "max_block_age", config.MaxBlockAge)
	}

	if err := scheduler.Start(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to start scheduler: %w", err)
	}

	// Record initial gauge metrics
	metricsRecorder.RecordStreamConfigured(ctx, len(config.Directives))

	// Query actual active streams on startup (cache persists across restarts)
	scheduler.UpdateGaugeMetrics(ctx)

	return NewExtension(service.Logger, cacheDB, scheduler, syncChecker, metricsRecorder, engineOps, db, config.Enabled), nil
}

// createIndependentConnectionPool creates a dedicated connection pool for cache operations
func createIndependentConnectionPool(ctx context.Context, service *common.Service, logger log.Logger) (*pgxpool.Pool, error) {
	// Check for test DB config override first
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
