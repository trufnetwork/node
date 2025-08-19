package tn_digest

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_digest/internal"
)

// InitializeExtension registers hooks needed by this extension.
func InitializeExtension() {
	// Register engine ready hook
	if err := hooks.RegisterEngineReadyHook(ExtensionName+"_engine_ready", engineReadyHook); err != nil {
		panic(fmt.Sprintf("failed to register %s engine ready hook: %v", ExtensionName, err))
	}
	// Register end-block hook for leader gating
	if err := hooks.RegisterEndBlockHook(ExtensionName+"_end_block", endBlockHook); err != nil {
		panic(fmt.Sprintf("failed to register %s end block hook: %v", ExtensionName, err))
	}
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
	engOps := internal.NewEngineOperations(app.Engine, db, app.Service.Logger)

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
	ext := GetExtension()
	if ext == nil {
		return nil
	}

	// Determine leader: compare NetworkParameters.Leader with node identity
	isLeader := isCurrentLeader(app, block)

	prev := ext.IsLeader()
	if !prev && isLeader {
		// became leader: start scheduler if enabled
		if ext.ConfigEnabled() {
			// lazily create scheduler if missing using app.Service (tests may not set ext.Service)
			if ext.ensureSchedulerWithService(app.Service) {
				// created scheduler, continue to start below
			}
			if ext.Scheduler() == nil { // still missing due to missing prereqs
				return nil
			}
			if err := ext.startScheduler(ctx); err != nil {
				ext.Logger().Warn("failed to start tn_digest scheduler on leader acquire", "error", err)
			} else {
				ext.Logger().Info("tn_digest started (leader)", "schedule", ext.Schedule())
			}
		}
	}
	if prev && !isLeader {
		// lost leadership: stop scheduler if running
		ext.stopSchedulerIfRunning()
		ext.Logger().Info("tn_digest stopped (lost leadership)")
	}
	ext.setLeader(isLeader)

	// Periodic config reload
	if block != nil && ext.ReloadIntervalBlocks() > 0 {
		if block.Height-ext.LastCheckedHeight() >= ext.ReloadIntervalBlocks() {
			if ext.EngineOps() != nil {
				enabled, schedule, _ := ext.EngineOps().LoadDigestConfig(ctx)
				// Only act if changed
				if enabled != ext.ConfigEnabled() || (schedule != "" && schedule != ext.Schedule()) {
					// fallback to default if schedule empty
					if schedule == "" {
						schedule = DefaultDigestSchedule
					}
					ext.SetConfig(enabled, schedule)
					// reconcile based on new config and current leadership
					if !enabled {
						// disabled -> stop if running
						ext.stopSchedulerIfRunning()
						ext.Logger().Info("tn_digest stopped due to config disabled")
					} else if isLeader {
						// enabled and leader -> (re)start with new schedule
						if ext.Scheduler() == nil {
							if !ext.ensureSchedulerWithService(app.Service) {
								return nil
							}
						} else {
							ext.stopSchedulerIfRunning()
						}
						if err := ext.startScheduler(ctx); err != nil {
							ext.Logger().Warn("failed to (re)start tn_digest scheduler after config update", "error", err)
						} else {
							ext.Logger().Info("tn_digest (re)started with new schedule", "schedule", ext.Schedule())
						}
					}
				}
			} else {
				ext.Logger().Debug("tn_digest: skip reload; EngineOps not ready")
			}
			ext.SetLastCheckedHeight(block.Height)
		}
	}
	return nil
}
