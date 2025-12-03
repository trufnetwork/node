package tn_settlement

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/leaderwatch"
	"github.com/trufnetwork/node/extensions/tn_settlement/internal"
)

// InitializeExtension registers hooks needed by this extension
func InitializeExtension() {
	// Register precompile to make extension visible in logs
	err := precompiles.RegisterInitializer(ExtensionName, InitializeSettlementPrecompile)
	if err != nil {
		panic(fmt.Sprintf("failed to register %s initializer: %v", ExtensionName, err))
	}

	// Register engine ready hook
	if err := hooks.RegisterEngineReadyHook(ExtensionName+"_engine_ready", engineReadyHook); err != nil {
		panic(fmt.Sprintf("failed to register %s engine ready hook: %v", ExtensionName, err))
	}

	// Register end-block hook
	if err := hooks.RegisterEndBlockHook(ExtensionName+"_end_block", endBlockHook); err != nil {
		panic(fmt.Sprintf("failed to register %s end block hook: %v", ExtensionName, err))
	}

	// Register with leaderwatch for leadership coordination
	if err := leaderwatch.Register(ExtensionName, leaderwatch.Callbacks{
		OnAcquire:  settlementLeaderAcquire,
		OnLose:     settlementLeaderLose,
		OnEndBlock: settlementLeaderEndBlock,
	}); err != nil {
		panic(fmt.Sprintf("failed to register %s leader watcher: %v", ExtensionName, err))
	}
}

// InitializeSettlementPrecompile makes the extension visible in logs
func InitializeSettlementPrecompile(ctx context.Context, service *common.Service, db sql.DB, alias string, metadata map[string]any) (precompiles.Precompile, error) {
	// Return empty precompile just to make the extension show up in "registered extension" logs
	return precompiles.Precompile{}, nil
}

// engineReadyHook initializes engine operations and config snapshot (no scheduler start here)
func engineReadyHook(ctx context.Context, app *common.App) error {
	logger := app.Service.Logger.New(ExtensionName)

	// Use app.DB for read/query
	var db sql.DB
	db = app.DB
	if db == nil {
		logger.Warn("app.DB is nil; settlement extension may not be fully operational")
	}

	// Build engine operations wrapper
	engOps := internal.NewEngineOperations(app.Engine, db, app.Accounts, app.Service.Logger)

	// Load schedule from config; fall back to defaults if absent
	enabled, schedule, maxMarkets, retries, _ := engOps.LoadSettlementConfig(ctx)
	if schedule == "" {
		schedule = DefaultSettlementSchedule
	}
	if maxMarkets <= 0 {
		maxMarkets = 10
	}
	if retries <= 0 {
		retries = 3
	}

	// Create extension instance and snapshot references
	ext := GetExtension()
	ext.logger = logger
	ext.SetService(app.Service)
	ext.SetEngineOps(engOps)
	ext.SetConfig(enabled, schedule, maxMarkets, retries)

	// Load config from node TOML [extensions.tn_settlement]
	if ext.Service() != nil && ext.Service().LocalConfig != nil {
		if m, ok := ext.Service().LocalConfig.Extensions[ExtensionName]; ok {
			// reload_interval_blocks (default: 100)
			if v, ok2 := m["reload_interval_blocks"]; ok2 && v != "" {
				var parsed int64
				if _, err := fmt.Sscan(v, &parsed); err != nil {
					logger.Warn("failed to parse reload_interval_blocks, using default", "value", v, "error", err)
				} else if parsed > 0 {
					ext.SetReloadIntervalBlocks(parsed)
				}
			}
			// reload_retry_backoff_seconds (default: 60)
			if v, ok2 := m["reload_retry_backoff_seconds"]; ok2 && v != "" {
				var seconds int64
				if _, err := fmt.Sscan(v, &seconds); err != nil {
					logger.Warn("failed to parse reload_retry_backoff_seconds, using default", "value", v, "error", err)
				} else if seconds > 0 {
					ext.SetReloadRetryBackoff(time.Duration(seconds) * time.Second)
				}
			}
			// reload_max_retries (default: 15)
			if v, ok2 := m["reload_max_retries"]; ok2 && v != "" {
				var retriesVal int
				if _, err := fmt.Sscan(v, &retriesVal); err != nil {
					logger.Warn("failed to parse reload_max_retries, using default", "value", v, "error", err)
				} else if retriesVal > 0 {
					ext.SetReloadMaxRetries(retriesVal)
				}
			}
		}
	}
	// Set defaults if not configured
	if ext.ReloadIntervalBlocks() == 0 {
		ext.SetReloadIntervalBlocks(100)
	}
	if ext.ReloadRetryBackoff() == 0 {
		ext.SetReloadRetryBackoff(60 * time.Second)
	}
	if ext.ReloadMaxRetries() == 0 {
		ext.SetReloadMaxRetries(15)
	}

	// Fill in signer and broadcaster once engine is ready
	wireSignerAndBroadcaster(app, ext)

	// Start background retry worker for config reload resilience
	ext.startRetryWorker()

	// Do not start scheduler here; EndBlockHook will manage based on leader
	return nil
}

// endBlockHook placeholder (kept for compatibility)
func endBlockHook(ctx context.Context, app *common.App, block *common.BlockContext) error {
	return nil
}
