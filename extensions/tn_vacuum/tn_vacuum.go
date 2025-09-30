package tn_vacuum

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
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

	if app != nil {
		ext.initializeState(ctx, app.DB)
	}

	svc := (*common.Service)(nil)
	if app != nil {
		svc = app.Service
	}

	cfg, err := LoadConfig(svc)
	if err != nil {
		ext.Logger().Warn("failed to load tn_vacuum config", "error", err)
		cfg.Enabled = false
	}

	if err := ext.configure(ctx, cfg); err != nil {
		ext.Logger().Warn("failed to configure tn_vacuum", "error", err)
	}
	if cfg.Enabled {
		ext.mu.Lock()
		ext.startWorkerLocked(ctx)
		ext.mu.Unlock()
	}
	return nil
}

func endBlockHook(ctx context.Context, app *common.App, block *common.BlockContext) error {
	if block == nil {
		return nil
	}
	ext := GetExtension()
	if app != nil && app.Service != nil {
		ext.setService(app.Service)
	}
	ext.maybeRun(ctx, block.Height)
	return nil
}
