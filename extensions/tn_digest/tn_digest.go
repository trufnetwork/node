package tn_digest

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"

	appkey "github.com/trufnetwork/kwil-db/app/key"
	appconf "github.com/trufnetwork/kwil-db/app/node/conf"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	rpcclient "github.com/trufnetwork/kwil-db/core/rpc/client"
	rpcuser "github.com/trufnetwork/kwil-db/core/rpc/client/user/jsonrpc"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_digest/internal"
	"github.com/trufnetwork/node/extensions/tn_digest/scheduler"
)

const ExtensionName = "tn_digest"

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

// txBroadcasterFunc adapts a function to the TxBroadcaster interface
type txBroadcasterFunc func(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error)

func (f txBroadcasterFunc) BroadcastTx(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error) {
	return f(ctx, tx, sync)
}

// engineReadyHook initializes engine operations and config snapshot (no scheduler start here).
func engineReadyHook(ctx context.Context, app *common.App) error {
	logger := app.Service.Logger.New(ExtensionName)

	// Create independent read-only DB pool like tn_cache to avoid tx lifecycle issues
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
		schedule = "0 */6 * * *"
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

	// Wire signer from node key (root dir) and a local broadcaster via JSON-RPC user service
	if app.Service != nil && app.Service.LocalConfig != nil {
		// Load node private key to create signer
		rootDir := appconf.RootDir()
		if rootDir == "" {
			logger.Warn("root dir is empty; cannot load node key for signer")
		} else {
			keyPath := config.NodeKeyFilePath(rootDir)
			if pk, err := appkey.LoadNodeKey(keyPath); err != nil {
				logger.Warn("failed to load node key for signer; tn_digest disabled until available", "path", keyPath, "error", err)
			} else {
				ext.SetNodeSigner(auth.GetNodeSigner(pk))
			}
		}
		// Build broadcaster using local RPC listen address
		listen := app.Service.LocalConfig.RPC.ListenAddress
		if listen == "" {
			logger.Warn("RPC listen address is empty; cannot create broadcaster")
		} else {
			endpoint := listen
			if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
				endpoint = "http://" + endpoint
			}
			u, err := url.Parse(endpoint)
			if err != nil {
				logger.Warn("invalid RPC listen address; cannot create broadcaster", "addr", listen, "error", err)
			} else {
				// Normalize 0.0.0.0 (and ::) to a loopback address for client connectivity
				host, port, herr := net.SplitHostPort(u.Host)
				if herr == nil {
					if host == "0.0.0.0" || host == "::" || host == "[::]" {
						host = "127.0.0.1"
						u.Host = net.JoinHostPort(host, port)
					}
				}
				userClient := rpcuser.NewClient(u)
				bcast := txBroadcasterFunc(func(bctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error) {
					// Accept to mempool; not waiting for commit here
					h, err := userClient.Broadcast(bctx, tx, rpcclient.BroadcastWaitAccept)
					if err != nil {
						return types.Hash{}, nil, err
					}
					return h, nil, nil
				})
				ext.SetBroadcaster(bcast)
			}
		}
	}

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
	isLeader := false
	if block != nil && block.ChainContext != nil && block.ChainContext.NetworkParameters != nil && app.Service.Identity != nil {
		leaderPk := block.ChainContext.NetworkParameters.Leader
		if leaderPk.PublicKey != nil {
			isLeader = bytes.Equal(leaderPk.Bytes(), app.Service.Identity)
		}
	}

	prev := ext.IsLeader()
	if !prev && isLeader {
		// became leader: start scheduler if enabled
		if ext.ConfigEnabled() {
			// lazily create scheduler if missing
			if ext.Scheduler() == nil {
				if ext.Broadcaster() == nil || ext.NodeSigner() == nil {
					ext.Logger().Warn("tn_digest prerequisites missing (broadcaster/signer); skipping scheduler start")
					return nil
				}
				sched := scheduler.NewDigestScheduler(scheduler.NewDigestSchedulerParams{
					Service:   app.Service,
					Logger:    ext.Logger(),
					EngineOps: ext.EngineOps(),
					Tx:        ext.Broadcaster(),
					Signer:    ext.NodeSigner(),
				})
				ext.SetScheduler(sched)
			}
			if err := ext.Scheduler().Start(ctx, ext.Schedule()); err != nil {
				ext.Logger().Warn("failed to start tn_digest scheduler on leader acquire", "error", err)
			} else {
				ext.Logger().Info("tn_digest started (leader)", "schedule", ext.Schedule())
			}
		}
	}
	if prev && !isLeader {
		// lost leadership: stop scheduler if running
		if ext.Scheduler() != nil {
			_ = ext.Scheduler().Stop()
			ext.Logger().Info("tn_digest stopped (lost leadership)")
		}
	}
	ext.setLeader(isLeader)

	// Periodic config reload
	if block != nil && ext.ReloadIntervalBlocks() > 0 {
		if block.Height-ext.LastCheckedHeight() >= ext.ReloadIntervalBlocks() {
			enabled, schedule, _ := ext.EngineOps().LoadDigestConfig(ctx)
			// Only act if changed
			if enabled != ext.ConfigEnabled() || (schedule != "" && schedule != ext.Schedule()) {
				// fallback to default if schedule empty
				if schedule == "" {
					schedule = "0 */6 * * *"
				}
				ext.SetConfig(enabled, schedule)
				// reconcile based on new config and current leadership
				if !enabled {
					if ext.Scheduler() != nil {
						_ = ext.Scheduler().Stop()
						ext.Logger().Info("tn_digest stopped due to config disabled")
					}
				} else if isLeader {
					// restart with new schedule
					if ext.Scheduler() == nil {
						if ext.Broadcaster() == nil || ext.NodeSigner() == nil {
							ext.Logger().Warn("tn_digest prerequisites missing (broadcaster/signer); cannot restart scheduler")
							return nil
						}
						sched := scheduler.NewDigestScheduler(scheduler.NewDigestSchedulerParams{
							Service:   app.Service,
							Logger:    ext.Logger(),
							EngineOps: ext.EngineOps(),
							Tx:        ext.Broadcaster(),
							Signer:    ext.NodeSigner(),
						})
						ext.SetScheduler(sched)
					} else {
						_ = ext.Scheduler().Stop()
					}
					if err := ext.Scheduler().Start(ctx, ext.Schedule()); err != nil {
						ext.Logger().Warn("failed to (re)start tn_digest scheduler after config update", "error", err)
					} else {
						ext.Logger().Info("tn_digest (re)started with new schedule", "schedule", ext.Schedule())
					}
				}
			}
			ext.SetLastCheckedHeight(block.Height)
		}
	}
	return nil
}
