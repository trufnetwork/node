package tn_digest

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/leaderwatch"
	"github.com/trufnetwork/node/extensions/tn_digest/internal"
)

// InitializeExtension registers hooks needed by this extension.
func InitializeExtension() {
	// Register precompile to make extension visible in logs (similar to tn_cache)
	err := precompiles.RegisterInitializer(ExtensionName, InitializeDigestPrecompile)
	if err != nil {
		panic(fmt.Sprintf("failed to register %s initializer: %v", ExtensionName, err))
	}

	// Register engine ready hook
	if err := hooks.RegisterEngineReadyHook(ExtensionName+"_engine_ready", engineReadyHook); err != nil {
		panic(fmt.Sprintf("failed to register %s engine ready hook: %v", ExtensionName, err))
	}
	// Register end-block hook (kept for compatibility; actual leader handling via leaderwatch)
	if err := hooks.RegisterEndBlockHook(ExtensionName+"_end_block", endBlockHook); err != nil {
		panic(fmt.Sprintf("failed to register %s end block hook: %v", ExtensionName, err))
	}
	if err := leaderwatch.Register(ExtensionName, leaderwatch.Callbacks{
		OnAcquire:  digestLeaderAcquire,
		OnLose:     digestLeaderLose,
		OnEndBlock: digestLeaderEndBlock,
	}); err != nil {
		panic(fmt.Sprintf("failed to register %s leader watcher: %v", ExtensionName, err))
	}
}

// InitializeDigestPrecompile makes the extension visible in logs
func InitializeDigestPrecompile(ctx context.Context, service *common.Service, db sql.DB, alias string, metadata map[string]any) (precompiles.Precompile, error) {
	// Return empty precompile just to make the extension show up in "registered extension" logs
	return precompiles.Precompile{}, nil
}

// InitializeExtensionWithNodeCapabilities is deprecated. The extension now
// self-wires signer and broadcaster from node configuration in engineReadyHook.
func InitializeExtensionWithNodeCapabilities(_ TxBroadcaster, _ auth.Signer) { InitializeExtension() }

// engineReadyHook initializes engine operations and config snapshot (no scheduler start here).
func engineReadyHook(ctx context.Context, app *common.App) error {
	logger := app.Service.Logger.New(ExtensionName)

	// Use app.DB for read/query; if a separate RO pool is needed in future, wire it here.
	var db sql.DB
	db = app.DB
	if db == nil {
		logger.Warn("app.DB is nil; digest extension may not be fully operational")
	}

	// Build engine operations wrapper
	engOps := internal.NewEngineOperations(app.Engine, db, app.Accounts, app.Service.Logger)

	// Load schedule from config; fall back to default if absent
	enabled, schedule, _ := engOps.LoadDigestConfig(ctx)
	if schedule == "" {
		schedule = DefaultDigestSchedule
	}

	// Create extension instance and snapshot references
	ext := GetExtension()
	ext.logger = logger
	ext.SetService(app.Service)
	ext.SetEngineOps(engOps)
	ext.SetConfig(enabled, schedule)
	// default reload interval: 1000 blocks; allow override via node config TOML
	var reload int64 = 1000
	if ext.Service() != nil && ext.Service().LocalConfig != nil {
		if m, ok := ext.Service().LocalConfig.Extensions[ExtensionName]; ok {
			if v, ok2 := m["reload_interval_blocks"]; ok2 && v != "" {
				// best-effort parse
				var parsed int64
				_, _ = fmt.Sscan(v, &parsed)
				if parsed > 0 {
					reload = parsed
				}
			}
		}
	}
	ext.SetReloadIntervalBlocks(reload)

	// Fill in signer and broadcaster once engine is ready
	wireSignerAndBroadcaster(app, ext)

	// Do not start scheduler here; EndBlockHook will manage based on leader
	return nil
}

// endBlockHook toggles scheduler based on leader status and config
func endBlockHook(ctx context.Context, app *common.App, block *common.BlockContext) error {
	return nil
}

func digestLeaderAcquire(ctx context.Context, app *common.App, block *common.BlockContext) {
	ext := GetExtension()
	if ext == nil {
		return
	}
	ext.setLeader(true)
	if !ext.ConfigEnabled() {
		return
	}
	service := ext.Service()
	if app != nil && app.Service != nil {
		service = app.Service
		if ext.Service() == nil {
			ext.SetService(service)
		}
	}
	if ext.ensureSchedulerWithService(service) {
		// scheduler created; fall through to start
	}
	if ext.Scheduler() == nil {
		ext.Logger().Debug("tn_digest: prerequisites missing; deferring start until broadcaster/signer/engine/service are available")
		return
	}
	if err := ext.startScheduler(ctx); err != nil {
		ext.Logger().Warn("failed to start tn_digest scheduler on leader acquire", "error", err)
	} else {
		ext.Logger().Info("tn_digest started (leader)", "schedule", ext.Schedule())
	}
}

func digestLeaderLose(ctx context.Context, app *common.App, block *common.BlockContext) {
	ext := GetExtension()
	if ext == nil {
		return
	}
	ext.setLeader(false)
	ext.stopSchedulerIfRunning()
	if ext.Logger() != nil {
		ext.Logger().Info("tn_digest stopped (lost leadership)")
	}
}

func digestLeaderEndBlock(ctx context.Context, app *common.App, block *common.BlockContext) {
	ext := GetExtension()
	if ext == nil {
		return
	}

	if block == nil {
		return
	}

	reload := ext.ReloadIntervalBlocks()
	if reload <= 0 {
		return
	}

	if block.Height-ext.LastCheckedHeight() < reload {
		return
	}

	if ext.EngineOps() == nil {
		ext.Logger().Debug("tn_digest: skip reload; EngineOps not ready")
		ext.SetLastCheckedHeight(block.Height)
		return
	}

	enabled, schedule, _ := ext.EngineOps().LoadDigestConfig(ctx)
	if schedule == "" {
		schedule = DefaultDigestSchedule
	}

	if enabled != ext.ConfigEnabled() || schedule != ext.Schedule() {
		ext.SetConfig(enabled, schedule)
		if !enabled {
			ext.stopSchedulerIfRunning()
			ext.Logger().Info("tn_digest stopped due to config disabled")
		} else if ext.IsLeader() {
			service := ext.Service()
			if app != nil && app.Service != nil {
				service = app.Service
				if ext.Service() == nil {
					ext.SetService(service)
				}
			}
			if ext.Scheduler() == nil && !ext.ensureSchedulerWithService(service) {
				ext.Logger().Debug("tn_digest: prerequisites missing; deferring (re)start after config update")
			} else if ext.Scheduler() != nil {
				ext.stopSchedulerIfRunning()
				if err := ext.startScheduler(ctx); err != nil {
					ext.Logger().Warn("failed to (re)start tn_digest scheduler after config update", "error", err)
				} else {
					ext.Logger().Info("tn_digest (re)started with new schedule", "schedule", ext.Schedule())
				}
			}
		}
	}

	ext.SetLastCheckedHeight(block.Height)
}
