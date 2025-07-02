package tn_cache

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	"github.com/trufnetwork/kwil-db/node/types/sql"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/validation"
)

// Use ExtensionName from constants
const ExtensionName = config.ExtensionName

var (
	logger  log.Logger
	cacheDB *internal.CacheDB
)

// ParseConfig parses the extension configuration from the node's config file using the new configuration system
func ParseConfig(service *common.Service) (*config.ProcessedConfig, error) {
	logger = service.Logger.New("tn_cache")

	// Get extension configuration from the node config
	extConfig, ok := service.LocalConfig.Extensions[ExtensionName]
	if !ok {
		// Extension is not configured, return default disabled config
		logger.Debug("extension not configured, disabling")
		return &config.ProcessedConfig{
			Enabled:      false,
			Instructions: []config.InstructionDirective{},
			Sources:      []string{},
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
		"instructions_count", len(processedConfig.Instructions),
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

	if !processedConfig.Enabled {
		logger.Info("extension is disabled")
		return nil
	}

	logger.Info("initializing extension",
		"enabled", processedConfig.Enabled,
		"instructions_count", len(processedConfig.Instructions),
		"sources", processedConfig.Sources)

	// Get the database from the app
	db, ok := app.DB.(sql.DB)
	if !ok {
		return fmt.Errorf("app.DB is not a sql.DB")
	}

	// Create the CacheDB instance
	cacheDB = internal.NewCacheDB(db, logger)

	// Initialize extension resources
	err = setupCacheSchema(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to setup cache schema: %w", err)
	}

	// Start background processes for cache instructions
	if len(processedConfig.Instructions) > 0 {
		go startBackgroundRefreshes(ctx, app, processedConfig)
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

// startBackgroundRefreshes starts background goroutines for scheduled refreshes using cache instructions
func startBackgroundRefreshes(ctx context.Context, app *common.App, processedConfig *config.ProcessedConfig) {
	logger.Info("starting background refreshes", "instructions_count", len(processedConfig.Instructions))

	// Setup stream configurations in the database
	err := setupStreamConfigs(ctx, processedConfig.Instructions)
	if err != nil {
		logger.Error("failed to setup stream configurations", "error", err)
		return
	}

	// Group instructions by schedule for batch processing
	scheduleGroups := make(map[string][]config.InstructionDirective)
	for _, instruction := range processedConfig.Instructions {
		schedule := instruction.Schedule.CronExpr
		scheduleGroups[schedule] = append(scheduleGroups[schedule], instruction)
	}

	// Start a goroutine for each schedule group
	for schedule, instructions := range scheduleGroups {
		go runScheduledRefreshes(ctx, app, schedule, instructions)
	}
}

// setupStreamConfigs adds the configured streams to the database
func setupStreamConfigs(ctx context.Context, instructions []config.InstructionDirective) error {
	for _, instruction := range instructions {
		// Get the from timestamp, defaulting to 0 if not set
		var fromTimestamp int64
		if instruction.TimeRange.From != nil {
			fromTimestamp = *instruction.TimeRange.From
		}

		if err := cacheDB.AddStreamConfig(ctx, internal.StreamCacheConfig{
			DataProvider:  instruction.DataProvider,
			StreamID:      instruction.StreamID,
			FromTimestamp: fromTimestamp,
			LastRefreshed: time.Now().UTC().Format(time.RFC3339),
			CronSchedule:  instruction.Schedule.CronExpr,
		}); err != nil {
			return fmt.Errorf("failed to add stream config for %s/%s: %w",
				instruction.DataProvider, instruction.StreamID, err)
		}
	}
	return nil
}

// runScheduledRefreshes runs refreshes for a group of instructions with the same schedule
func runScheduledRefreshes(ctx context.Context, app *common.App, schedule string, instructions []config.InstructionDirective) {
	logger.Info("started refresh routine",
		"schedule", schedule,
		"instructions_count", len(instructions))

	// Validate the cron schedule
	if err := validation.ValidateCronSchedule(schedule); err != nil {
		logger.Error("invalid cron schedule", "schedule", schedule, "error", err)
		return
	}

	// In a real implementation, this would use the proper cron scheduler
	// For now, we'll use a simplified ticker for demonstration
	ticker := time.NewTicker(30 * time.Second) // Simplified for demo
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			logger.Debug("refresh tick", "schedule", schedule)
			for _, instruction := range instructions {
				if err := refreshCacheInstruction(ctx, app, instruction); err != nil {
					logger.Error("failed to refresh cache instruction",
						"instruction_id", instruction.ID,
						"data_provider", instruction.DataProvider,
						"stream_id", instruction.StreamID,
						"type", instruction.Type,
						"error", err)
				}
			}
		case <-ctx.Done():
			logger.Info("shutting down refresh routine", "schedule", schedule)
			return
		}
	}
}

// refreshCacheInstruction refreshes the cache for a single instruction directive
func refreshCacheInstruction(ctx context.Context, app *common.App, instruction config.InstructionDirective) error {
	logger.Debug("refreshing cache instruction",
		"instruction_id", instruction.ID,
		"data_provider", instruction.DataProvider,
		"stream_id", instruction.StreamID,
		"type", instruction.Type,
		"include_children", instruction.IncludeChildren,
		"source", instruction.Metadata.Source)

	// TODO: Implement actual cache refresh logic based on instruction type
	// For now, this is a placeholder implementation
	// In a real implementation, this would:
	// 1. Fetch data from the data provider
	// 2. Convert the data to CachedEvent objects
	// 3. Store the events in the cache using cacheDB.CacheEvents

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
