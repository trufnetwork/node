package tn_settlement

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/app/key"
	appconf "github.com/trufnetwork/kwil-db/app/node/conf"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/node/extensions/tn_settlement/scheduler"
)

// havePrereqs returns true if the extension has everything needed to build a scheduler
func (e *Extension) havePrereqs(service *common.Service) bool {
	return e.Broadcaster() != nil && e.NodeSigner() != nil && e.EngineOps() != nil && service != nil
}

// buildScheduler creates a new scheduler instance using the extension's wiring
func (e *Extension) buildScheduler(service *common.Service) *scheduler.SettlementScheduler {
	return scheduler.NewSettlementScheduler(scheduler.NewSettlementSchedulerParams{
		Service:          service,
		Logger:           e.Logger(),
		EngineOps:        e.EngineOps(),
		Tx:               e.Broadcaster(),
		Signer:           e.NodeSigner(),
		MaxMarketsPerRun: e.MaxMarketsPerRun(),
		RetryAttempts:    e.RetryAttempts(),
	})
}

// ensureSchedulerWithService creates the scheduler if not present using the provided service
func (e *Extension) ensureSchedulerWithService(service *common.Service) bool {
	if e.Scheduler() != nil {
		return false
	}
	if !e.havePrereqs(service) {
		e.Logger().Warn("tn_settlement prerequisites missing (broadcaster/signer/engine/service); skipping scheduler creation")
		return false
	}
	e.SetScheduler(e.buildScheduler(service))
	return true
}

func (e *Extension) startScheduler(ctx context.Context) error {
	sched := e.Scheduler()
	if sched == nil {
		return fmt.Errorf("scheduler not initialized")
	}
	return sched.Start(ctx, e.Schedule())
}

func (e *Extension) stopSchedulerIfRunning() {
	if e.Scheduler() != nil {
		_ = e.Scheduler().Stop()
	}
}

// wireSignerAndBroadcaster fills in signer and broadcaster if not already set
func wireSignerAndBroadcaster(app *common.App, ext *Extension) {
	if app == nil || app.Service == nil || app.Service.LocalConfig == nil {
		return
	}
	// Signer (from node key file)
	if ext.NodeSigner() == nil {
		rootDir := appconf.RootDir()
		if rootDir == "" {
			ext.Logger().Warn("root dir is empty; cannot load node key for signer")
		} else {
			keyPath := config.NodeKeyFilePath(rootDir)
			if pk, err := key.LoadNodeKey(keyPath); err != nil {
				ext.Logger().Warn("failed to load node key for signer; tn_settlement disabled until available", "path", keyPath, "error", err)
			} else {
				ext.SetNodeSigner(auth.GetUserSigner(pk))
			}
		}
	}
	// Broadcaster (JSON-RPC user service)
	if ext.Broadcaster() == nil {
		// Optional override: [extensions.tn_settlement].rpc_url
		if m, ok := app.Service.LocalConfig.Extensions[ExtensionName]; ok {
			if rpcURL := m["rpc_url"]; rpcURL != "" {
				if u, err := normalizeListenAddressForClient(rpcURL); err == nil {
					ext.SetBroadcaster(makeBroadcasterFromURL(u))
					return
				} else {
					ext.Logger().Warn("invalid extensions.tn_settlement.rpc_url; falling back to [rpc].listen", "error", err)
				}
			}
		}
		listen := app.Service.LocalConfig.RPC.ListenAddress
		if listen == "" {
			ext.Logger().Warn("RPC listen address is empty; cannot create broadcaster")
		} else if u, err := normalizeListenAddressForClient(listen); err != nil {
			ext.Logger().Warn("invalid RPC listen address; cannot create broadcaster", "addr", listen, "error", err)
		} else {
			ext.SetBroadcaster(makeBroadcasterFromURL(u))
		}
	}
}

// settlementLeaderAcquire starts scheduler when node becomes leader
func settlementLeaderAcquire(ctx context.Context, app *common.App, block *common.BlockContext) {
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
		ext.Logger().Debug("tn_settlement: prerequisites missing; deferring start until broadcaster/signer/engine/service are available")
		return
	}
	// Use background context for scheduler - block context gets canceled after block processing
	if err := ext.startScheduler(context.Background()); err != nil {
		ext.Logger().Warn("failed to start tn_settlement scheduler on leader acquire", "error", err)
	} else {
		ext.Logger().Info("tn_settlement started (leader)", "schedule", ext.Schedule())
	}
}

// settlementLeaderLose stops scheduler when node loses leadership
func settlementLeaderLose(ctx context.Context, app *common.App, block *common.BlockContext) {
	ext := GetExtension()
	if ext == nil {
		return
	}
	ext.setLeader(false)
	ext.stopSchedulerIfRunning()
	if ext.Logger() != nil {
		ext.Logger().Info("tn_settlement stopped (lost leadership)")
	}
}

// settlementLeaderEndBlock checks for config reload periodically
func settlementLeaderEndBlock(ctx context.Context, app *common.App, block *common.BlockContext) {
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
		ext.Logger().Debug("tn_settlement: skip reload; EngineOps not ready")
		ext.SetLastCheckedHeight(block.Height)
		return
	}

	// Single immediate attempt (non-blocking, consensus-safe)
	// If it fails, signal background worker to retry with backoff
	enabled, schedule, maxMarkets, retries, loadErr := ext.EngineOps().LoadSettlementConfig(ctx)
	if loadErr != nil {
		ext.Logger().Warn("config reload failed in end-block, signaling background retry worker", "error", loadErr)
		ext.SetLastCheckedHeight(block.Height)
		ext.signalRetryNeeded() // signal background worker to retry
		return
	}

	// Apply config change with proper synchronization (prevents race with background worker)
	ext.applyConfigChangeWithLock(ctx, enabled, schedule, maxMarkets, retries, app)

	ext.SetLastCheckedHeight(block.Height)
}
