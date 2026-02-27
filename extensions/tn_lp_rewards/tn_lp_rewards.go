package tn_lp_rewards

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_lp_rewards/internal"
)

const (
	ExtensionName = "tn_lp_rewards"

	// Default configuration
	DefaultSamplingIntervalBlocks = 10
	DefaultMaxMarketsPerRun       = 50
)

// Extension holds the singleton state for LP rewards sampling
type Extension struct {
	mu sync.RWMutex

	logger   log.Logger
	service  *common.Service
	engOps   *internal.EngineOperations

	// Configuration (loaded from database)
	enabled                bool
	samplingIntervalBlocks int64
	maxMarketsPerRun       int
	configReloadInterval   int64 // Reload config every N blocks
	lastCheckedHeight      int64
}

var (
	extensionInstance *Extension
	extensionMu       sync.RWMutex
)

// GetExtension returns the singleton extension instance
func GetExtension() *Extension {
	extensionMu.RLock()
	if extensionInstance != nil {
		defer extensionMu.RUnlock()
		return extensionInstance
	}
	extensionMu.RUnlock()

	extensionMu.Lock()
	defer extensionMu.Unlock()

	if extensionInstance == nil {
		extensionInstance = &Extension{
			logger:                 log.New(log.WithLevel(log.LevelInfo)),
			enabled:                true,
			samplingIntervalBlocks: DefaultSamplingIntervalBlocks,
			maxMarketsPerRun:       DefaultMaxMarketsPerRun,
			configReloadInterval:   100,
		}
	}
	return extensionInstance
}

// InitializeExtension registers hooks needed by this extension
func InitializeExtension() {
	// Register precompile to make extension visible in logs
	err := precompiles.RegisterInitializer(ExtensionName, initializePrecompile)
	if err != nil {
		panic(fmt.Sprintf("failed to register %s initializer: %v", ExtensionName, err))
	}

	// Register engine ready hook
	if err := hooks.RegisterEngineReadyHook(ExtensionName+"_engine_ready", engineReadyHook); err != nil {
		panic(fmt.Sprintf("failed to register %s engine ready hook: %v", ExtensionName, err))
	}

	// Register end-block hook for KEYLESS internal sampling
	// This hook runs on ALL nodes during block finalisation.
	if err := hooks.RegisterEndBlockHook(ExtensionName+"_end_block", endBlockHook); err != nil {
		panic(fmt.Sprintf("failed to register %s end block hook: %v", ExtensionName, err))
	}
}

// initializePrecompile makes the extension visible in logs
func initializePrecompile(ctx context.Context, service *common.Service, db sql.DB, alias string, metadata map[string]any) (precompiles.Precompile, error) {
	return precompiles.Precompile{}, nil
}

// engineReadyHook initializes engine operations
func engineReadyHook(ctx context.Context, app *common.App) error {
	logger := app.Service.Logger.New(ExtensionName)

	var db sql.DB = app.DB
	if db == nil {
		logger.Warn("app.DB is nil; LP rewards extension may not be fully operational")
	}

	// Build engine operations wrapper
	engOps := internal.NewEngineOperations(app.Engine, db, app.Service.DBPool, app.Accounts, app.Service.Logger)

	// Create extension instance and set basic values
	ext := GetExtension()
	ext.logger = logger
	ext.service = app.Service
	ext.engOps = engOps

	// Load config from database - only update if successful, otherwise keep defaults
	enabled, interval, maxMarkets, err := engOps.LoadLPRewardsConfig(ctx)
	if err != nil {
		logger.Warn("failed to load LP rewards config; using defaults", "error", err)
	} else {
		ext.enabled = enabled
		ext.samplingIntervalBlocks = int64(interval)
		ext.maxMarketsPerRun = maxMarkets
	}

	// Load config from node TOML [extensions.tn_lp_rewards]
	if ext.service != nil && ext.service.LocalConfig != nil {
		if m, ok := ext.service.LocalConfig.Extensions[ExtensionName]; ok {
			// config_reload_interval_blocks (default: 100)
			if v, ok2 := m["config_reload_interval_blocks"]; ok2 && v != "" {
				var parsed int64
				if _, err := fmt.Sscan(v, &parsed); err != nil {
					logger.Warn("failed to parse config_reload_interval_blocks, using default", "value", v, "error", err)
				} else if parsed > 0 {
					ext.configReloadInterval = parsed
				}
			}
		}
	}

	logger.Info("LP rewards extension initialized (Internal Hook mode)",
		"enabled", ext.enabled,
		"sampling_interval_blocks", ext.samplingIntervalBlocks,
		"max_markets_per_run", ext.maxMarketsPerRun)

	return nil
}

// endBlockHook is called on every node at the end of each block.
// It performs reward sampling as a consensus rule (no keys/nonces needed).
func endBlockHook(ctx context.Context, app *common.App, block *common.BlockContext) error {
	ext := GetExtension()

	if block == nil || ext.engOps == nil {
		return nil
	}

	blockHeight := block.Height

	// Snapshot config values under RLock to avoid races with reloadConfig
	ext.mu.RLock()
	enabled := ext.enabled
	samplingIntervalBlocks := ext.samplingIntervalBlocks
	configReloadInterval := ext.configReloadInterval
	maxMarketsPerRun := ext.maxMarketsPerRun
	ext.mu.RUnlock()

	// Reload config periodically
	if configReloadInterval > 0 && blockHeight-atomic.LoadInt64(&ext.lastCheckedHeight) >= configReloadInterval {
		ext.reloadConfig(ctx)
		atomic.StoreInt64(&ext.lastCheckedHeight, blockHeight)
	}

	if !enabled {
		return nil
	}

	// Check if it's time to sample (every N blocks)
	if samplingIntervalBlocks <= 0 || blockHeight%samplingIntervalBlocks != 0 {
		return nil
	}

	// Execute sampling logic directly against the engine (Internal call)
	// This is deterministic and run by all nodes at the same height.
	return ext.performInternalSampling(ctx, app, blockHeight, maxMarketsPerRun)
}

// reloadConfig reloads configuration from database
func (ext *Extension) reloadConfig(ctx context.Context) {
	if ext.engOps == nil {
		return
	}

	enabled, interval, maxMarkets, err := ext.engOps.LoadLPRewardsConfig(ctx)
	if err != nil {
		ext.logger.Warn("failed to reload LP rewards config", "error", err)
		return
	}

	ext.mu.Lock()
	ext.enabled = enabled
	ext.samplingIntervalBlocks = int64(interval)
	ext.maxMarketsPerRun = maxMarkets
	ext.mu.Unlock()

	ext.logger.Debug("reloaded LP rewards config",
		"enabled", enabled,
		"sampling_interval_blocks", interval,
		"max_markets_per_run", maxMarkets)
}

// performInternalSampling executes sampling logic for all active markets in a single batch.
// This runs within the block execution transaction.
func (ext *Extension) performInternalSampling(ctx context.Context, app *common.App, blockHeight int64, maxMarketsPerRun int) error {
	ext.logger.Info("performing batch LP rewards sampling",
		"block", blockHeight,
		"limit", maxMarketsPerRun)

	// Call sample_all_active_lp_rewards directly via Engine (Internal execution)
	// This performs the entire sampling loop inside a single SQL context for efficiency.
	res, err := app.Engine.CallWithoutEngineCtx(ctx, app.DB, "main", "sample_all_active_lp_rewards", []any{
		blockHeight,
		int64(maxMarketsPerRun),
	}, nil)

	if err != nil {
		ext.logger.Error("batch internal sampling failed", "error", err)
		return nil // We don't return error to avoid halting network on logic issues
	}

	if res.Error != nil {
		ext.logger.Warn("batch sampling execution error", "error", res.Error)
	} else {
		ext.logger.Info("batch internal LP rewards sampling completed", "block", blockHeight)
	}

	return nil
}
