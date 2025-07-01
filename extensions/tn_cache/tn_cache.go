package tn_cache

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/extensions/hooks"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/validation"
)

// Use ExtensionName from constants
const ExtensionName = config.ExtensionName

var (
	logger log.Logger
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

	// Initialize extension resources
	err = setupCacheSchema(ctx, app)
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
func setupCacheSchema(ctx context.Context, app *common.App) error {
	// This would create the extension's private schema and tables
	// Placeholder for actual implementation
	logger.Debug("setting up cache schema")
	return nil
}

// startBackgroundRefreshes starts background goroutines for scheduled refreshes using cache instructions
func startBackgroundRefreshes(ctx context.Context, app *common.App, processedConfig *config.ProcessedConfig) {
	logger.Info("starting background refreshes", "instructions_count", len(processedConfig.Instructions))

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
	// For DirectiveSpecific: refresh cache for specific stream
	// For DirectiveProviderWildcard: refresh cache for all streams from provider
	// Consider instruction.IncludeChildren: if true, also cache children of composed streams
	
	return nil
}
