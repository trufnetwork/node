package tn_digest

import (
	"context"

	"github.com/trufnetwork/kwil-db/app/key"
	appconf "github.com/trufnetwork/kwil-db/app/node/conf"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/node/extensions/tn_digest/scheduler"
)

// havePrereqs returns true if the extension has everything needed to build a scheduler.
func (e *Extension) havePrereqs(service *common.Service) bool {
	return e.Broadcaster() != nil && e.NodeSigner() != nil && e.EngineOps() != nil && service != nil
}

// buildScheduler creates a new scheduler instance using the extension's wiring.
func (e *Extension) buildScheduler(service *common.Service) *scheduler.DigestScheduler {
	return scheduler.NewDigestScheduler(scheduler.NewDigestSchedulerParams{
		Service:   service,
		Logger:    e.Logger(),
		EngineOps: e.EngineOps(),
		Tx:        e.Broadcaster(),
		Signer:    e.NodeSigner(),
	})
}

// ensureSchedulerWithService creates the scheduler if not present using the provided service.
func (e *Extension) ensureSchedulerWithService(service *common.Service) bool {
	if e.Scheduler() != nil {
		return false
	}
	if !e.havePrereqs(service) {
		e.Logger().Warn("tn_digest prerequisites missing (broadcaster/signer/engine/service); skipping scheduler creation")
		return false
	}
	e.SetScheduler(e.buildScheduler(service))
	return true
}

func (e *Extension) startScheduler(ctx context.Context) error {
	return e.Scheduler().Start(ctx, e.Schedule())
}

func (e *Extension) stopSchedulerIfRunning() {
	if e.Scheduler() != nil {
		_ = e.Scheduler().Stop()
	}
}

// wireSignerAndBroadcaster fills in signer and broadcaster if not already set.
func wireSignerAndBroadcaster(app *common.App, ext *Extension) {
	if app.Service == nil || app.Service.LocalConfig == nil {
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
				ext.Logger().Warn("failed to load node key for signer; tn_digest disabled until available", "path", keyPath, "error", err)
			} else {
				ext.SetNodeSigner(auth.GetNodeSigner(pk))
			}
		}
	}
	// Broadcaster (JSON-RPC user service)
	if ext.Broadcaster() == nil {
		// Optional override: [extensions.tn_digest].rpc_url
		if m, ok := app.Service.LocalConfig.Extensions[ExtensionName]; ok {
			if rpcURL := m["rpc_url"]; rpcURL != "" {
				if u, err := normalizeListenAddressForClient(rpcURL); err == nil {
					ext.SetBroadcaster(makeBroadcasterFromURL(u))
					return
				} else {
					ext.Logger().Warn("invalid extensions.tn_digest.rpc_url; falling back to [rpc].listen", "error", err)
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
