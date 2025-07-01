package tn_cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
)

const (
	// ExtensionName is the unique name of the extension
	ExtensionName = "tn_cache"
)

var (
	logger log.Logger
)

// StreamConfig represents the configuration for a single stream to be cached
type StreamConfig struct {
	DataProvider string `json:"data_provider"`
	StreamID     string `json:"stream_id"`
	CronSchedule string `json:"cron_schedule"`
	From         int64  `json:"from,omitempty"`
}

// Config represents the extension's configuration
type Config struct {
	Enabled bool           `json:"enabled"`
	Streams []StreamConfig `json:"streams"`
}

// ParseConfig parses the extension configuration from the node's config file
func ParseConfig(service *common.Service) (*Config, error) {
	logger = service.Logger.New("tn_cache")

	// Get extension configuration from the node config
	extConfig, ok := service.LocalConfig.Extensions[ExtensionName]
	if !ok {
		// Extension is not configured, return default disabled config
		logger.Debug("extension not configured, disabling")
		return &Config{Enabled: false}, nil
	}

	config := &Config{
		Enabled: false,
		Streams: []StreamConfig{},
	}

	// Parse the enabled flag
	enabledStr, ok := extConfig["enabled"]
	if ok {
		if strings.ToLower(enabledStr) == "true" {
			config.Enabled = true
		}
	}

	// Parse streams configuration
	streamsJSON, ok := extConfig["streams"]
	if ok && config.Enabled {
		var streams []StreamConfig
		if err := json.Unmarshal([]byte(streamsJSON), &streams); err != nil {
			logger.Error("failed to parse streams configuration", "error", err)
			return nil, fmt.Errorf("invalid streams configuration: %w", err)
		}
		config.Streams = streams
	}

	return config, nil
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
	config, err := ParseConfig(app.Service)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	if !config.Enabled {
		logger.Info("extension is disabled")
		return nil
	}

	logger.Info("initializing extension",
		"enabled", config.Enabled,
		"streams_count", len(config.Streams))

	// Initialize extension resources
	err = setupCacheSchema(ctx, app)
	if err != nil {
		return fmt.Errorf("failed to setup cache schema: %w", err)
	}

	// Start background processes for each stream group
	if len(config.Streams) > 0 {
		go startBackgroundRefreshes(ctx, app, config)
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

// startBackgroundRefreshes starts background goroutines for scheduled refreshes
func startBackgroundRefreshes(ctx context.Context, app *common.App, config *Config) {
	logger.Info("starting background refreshes", "streams_count", len(config.Streams))

	// Group streams by schedule for batch processing
	scheduleGroups := make(map[string][]StreamConfig)
	for _, stream := range config.Streams {
		scheduleGroups[stream.CronSchedule] = append(scheduleGroups[stream.CronSchedule], stream)
	}

	// Start a goroutine for each schedule group
	for schedule, streams := range scheduleGroups {
		go runScheduledRefreshes(ctx, app, schedule, streams)
	}
}

// runScheduledRefreshes runs refreshes for a group of streams with the same schedule
func runScheduledRefreshes(ctx context.Context, app *common.App, schedule string, streams []StreamConfig) {
	logger.Info("started refresh routine", "schedule", schedule, "streams_count", len(streams))

	// In a real implementation, this would use a proper cron library
	// For simplicity, we're just logging that we would do this
	ticker := time.NewTicker(30 * time.Second) // Simplified for demo
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			logger.Debug("refresh tick", "schedule", schedule)
			for _, stream := range streams {
				if err := refreshStreamCache(ctx, app, stream); err != nil {
					logger.Error("failed to refresh stream cache",
						"data_provider", stream.DataProvider,
						"stream_id", stream.StreamID,
						"error", err)
				}
			}
		case <-ctx.Done():
			logger.Info("shutting down refresh routine", "schedule", schedule)
			return
		}
	}
}

// refreshStreamCache refreshes the cache for a single stream
func refreshStreamCache(ctx context.Context, app *common.App, stream StreamConfig) error {
	logger.Debug("refreshing stream cache",
		"data_provider", stream.DataProvider,
		"stream_id", stream.StreamID)
	// TODO: Placeholder for actual implementation
	return nil
}
