package tn_vacuum

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/leaderwatch"
)

func InitializeExtension() {
	if err := precompiles.RegisterInitializer(ExtensionName, initializePrecompile); err != nil {
		panic(fmt.Sprintf("failed to register %s initializer: %v", ExtensionName, err))
	}
	if err := hooks.RegisterEngineReadyHook(ExtensionName+"_engine_ready", engineReadyHook); err != nil {
		panic(fmt.Sprintf("failed to register %s engine ready hook: %v", ExtensionName, err))
	}
	if err := hooks.RegisterEndBlockHook(ExtensionName+"_end_block", endBlockHook); err != nil {
		panic(fmt.Sprintf("failed to register %s end block hook: %v", ExtensionName, err))
	}
	if err := leaderwatch.Register(ExtensionName, leaderwatch.Callbacks{
		OnAcquire:  leaderAcquire,
		OnLose:     leaderLose,
		OnEndBlock: leaderEndBlock,
	}); err != nil {
		panic(fmt.Sprintf("failed to register %s leader watcher: %v", ExtensionName, err))
	}
}

func initializePrecompile(ctx context.Context, service *common.Service, db sql.DB, alias string, metadata map[string]any) (precompiles.Precompile, error) {
	ext := GetExtension()
	if service != nil {
		ext.setLogger(service.Logger.New(ExtensionName))
		ext.setService(service)
	}
	return precompiles.Precompile{}, nil
}

func engineReadyHook(ctx context.Context, app *common.App) error {
	ext := GetExtension()
	if app != nil && app.Service != nil {
		ext.setLogger(app.Service.Logger.New(ExtensionName))
		ext.setService(app.Service)
	}

	cfg, err := LoadConfig(app.Service)
	if err != nil {
		ext.Logger().Warn("failed to load tn_vacuum config", "error", err)
		cfg.Enabled = false
	}

	deps := MechanismDeps{Logger: ext.Logger()}
	if err := ext.reconfigure(ctx, cfg, deps); err != nil {
		ext.Logger().Warn("failed to configure tn_vacuum", "error", err)
		return nil
	}

	ext.SetReloadIntervalBlocks(cfg.ReloadIntervalBlocks)
	ext.SetLastConfigHeight(0)

	if cfg.Enabled {
		ext.startTriggerIfLeader(ctx)
	}

	return nil
}

func endBlockHook(ctx context.Context, app *common.App, block *common.BlockContext) error {
	return nil
}

func leaderAcquire(ctx context.Context, app *common.App, block *common.BlockContext) {
	ext := GetExtension()
	ext.setLeader(true)
	if trig := ext.Trigger(); trig != nil {
		_ = trig.OnLeaderChange(ctx, true)
	}
	ext.startTriggerIfLeader(ctx)
}

func leaderLose(ctx context.Context, app *common.App, block *common.BlockContext) {
	ext := GetExtension()
	ext.setLeader(false)
	if trig := ext.Trigger(); trig != nil {
		_ = trig.OnLeaderChange(ctx, false)
		_ = trig.Stop(ctx)
	}
}

func leaderEndBlock(ctx context.Context, app *common.App, block *common.BlockContext) {
	ext := GetExtension()
	cfg := ext.Config()
	if !cfg.Enabled {
		return
	}

	if trig := ext.Trigger(); trig != nil {
		_ = trig.OnEndBlock(ctx, block)
	}

	if block == nil {
		return
	}

	reload := ext.ReloadIntervalBlocks()
	if reload > 0 && block.Height-ext.LastConfigHeight() >= reload {
		var svc *common.Service
		if app != nil {
			svc = app.Service
		}
		cfg, err := LoadConfig(svc)
		if err != nil {
			ext.Logger().Warn("failed to reload tn_vacuum config", "error", err)
		} else {
			deps := MechanismDeps{Logger: ext.Logger()}
			if err := ext.reconfigure(ctx, cfg, deps); err != nil {
				ext.Logger().Warn("failed to apply tn_vacuum config", "error", err)
			}
		}
		ext.SetLastConfigHeight(block.Height)
	}
}
