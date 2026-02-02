package tn_lp_rewards

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/trufnetwork/kwil-db/app/key"
	appconf "github.com/trufnetwork/kwil-db/app/node/conf"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	rpcclient "github.com/trufnetwork/kwil-db/core/rpc/client"
	rpcuser "github.com/trufnetwork/kwil-db/core/rpc/client/user/jsonrpc"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/leaderwatch"
	"github.com/trufnetwork/node/extensions/tn_lp_rewards/internal"
)

const (
	ExtensionName = "tn_lp_rewards"

	// Default configuration
	DefaultSamplingIntervalBlocks = 10
	DefaultMaxMarketsPerRun       = 50
)

// TxBroadcaster interface for broadcasting transactions
type TxBroadcaster interface {
	BroadcastTx(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error)
}

// txBroadcasterFunc adapts a function to the TxBroadcaster interface
type txBroadcasterFunc func(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error)

func (f txBroadcasterFunc) BroadcastTx(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error) {
	return f(ctx, tx, sync)
}

// Extension holds the singleton state for LP rewards sampling
type Extension struct {
	mu sync.RWMutex

	logger   log.Logger
	service  *common.Service
	engOps   *internal.EngineOperations
	isLeader atomic.Bool

	// Configuration (loaded from database)
	enabled                bool
	samplingIntervalBlocks int64
	maxMarketsPerRun       int
	configReloadInterval   int64 // Reload config every N blocks
	lastCheckedHeight      int64

	// Sampling state - prevents overlapping runs
	isSampling atomic.Bool

	// Transaction broadcasting
	signer      auth.Signer
	broadcaster TxBroadcaster
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

	// Register with leaderwatch for leadership coordination
	if err := leaderwatch.Register(ExtensionName, leaderwatch.Callbacks{
		OnAcquire:  leaderAcquire,
		OnLose:     leaderLose,
		OnEndBlock: leaderEndBlock,
	}); err != nil {
		panic(fmt.Sprintf("failed to register %s leader watcher: %v", ExtensionName, err))
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

	// Wire signer and broadcaster
	wireSignerAndBroadcaster(app, ext)

	logger.Info("LP rewards extension initialized",
		"enabled", ext.enabled,
		"sampling_interval_blocks", ext.samplingIntervalBlocks,
		"max_markets_per_run", ext.maxMarketsPerRun)

	return nil
}

// wireSignerAndBroadcaster fills in signer and broadcaster if not already set
func wireSignerAndBroadcaster(app *common.App, ext *Extension) {
	if app == nil || app.Service == nil || app.Service.LocalConfig == nil {
		return
	}
	// Signer (from node key file)
	if ext.signer == nil {
		rootDir := appconf.RootDir()
		if rootDir == "" {
			ext.logger.Warn("root dir is empty; cannot load node key for signer")
		} else {
			keyPath := config.NodeKeyFilePath(rootDir)
			if pk, err := key.LoadNodeKey(keyPath); err != nil {
				ext.logger.Warn("failed to load node key for signer; LP rewards disabled until available", "path", keyPath, "error", err)
			} else {
				ext.signer = auth.GetUserSigner(pk)
			}
		}
	}
	// Broadcaster (JSON-RPC user service)
	if ext.broadcaster == nil {
		// Optional override: [extensions.tn_lp_rewards].rpc_url
		if m, ok := app.Service.LocalConfig.Extensions[ExtensionName]; ok {
			if rpcURL := m["rpc_url"]; rpcURL != "" {
				if u, err := normalizeListenAddressForClient(rpcURL); err == nil {
					ext.broadcaster = makeBroadcasterFromURL(u)
					return
				} else {
					ext.logger.Warn("invalid extensions.tn_lp_rewards.rpc_url; falling back to [rpc].listen", "error", err)
				}
			}
		}
		listen := app.Service.LocalConfig.RPC.ListenAddress
		if listen == "" {
			ext.logger.Warn("RPC listen address is empty; cannot create broadcaster")
		} else if u, err := normalizeListenAddressForClient(listen); err != nil {
			ext.logger.Warn("invalid RPC listen address; cannot create broadcaster", "addr", listen, "error", err)
		} else {
			ext.broadcaster = makeBroadcasterFromURL(u)
		}
	}
}

// normalizeListenAddressForClient converts a server listen address into a client URL
func normalizeListenAddressForClient(listen string) (*url.URL, error) {
	if listen == "" {
		return nil, fmt.Errorf("empty listen address")
	}
	endpoint := listen
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "http://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		cleanHost := strings.Trim(u.Host, "[]")
		if cleanHost == "" {
			u.Host = "127.0.0.1"
		} else if ip := net.ParseIP(cleanHost); ip != nil && ip.IsUnspecified() {
			u.Host = "127.0.0.1"
		}
	} else {
		cleanHost := strings.Trim(host, "[]")
		if cleanHost == "" {
			u.Host = net.JoinHostPort("127.0.0.1", port)
		} else if ip := net.ParseIP(cleanHost); ip != nil && ip.IsUnspecified() {
			u.Host = net.JoinHostPort("127.0.0.1", port)
		}
	}
	return u, nil
}

// makeBroadcasterFromURL creates a TxBroadcaster backed by the user JSON-RPC client
func makeBroadcasterFromURL(u *url.URL) TxBroadcaster {
	userClient := rpcuser.NewClient(u)
	return txBroadcasterFunc(func(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error) {
		// Map sync flag to broadcast mode (0 for accept-only, 1 for wait-commit)
		mode := rpcclient.BroadcastWaitAccept
		if sync == 1 {
			mode = rpcclient.BroadcastWaitCommit
		}
		h, err := userClient.Broadcast(ctx, tx, mode)
		if err != nil {
			return ktypes.Hash{}, nil, err
		}
		// For accept mode, we don't need to query the result (just mempool acceptance)
		if mode == rpcclient.BroadcastWaitAccept {
			return h, nil, nil
		}
		// For commit mode, query the result
		txQueryResp, err := userClient.TxQuery(ctx, h)
		if err != nil {
			return h, nil, fmt.Errorf("failed to query transaction result: %w", err)
		}
		if txQueryResp == nil || txQueryResp.Result == nil {
			return h, nil, nil
		}
		return h, txQueryResp.Result, nil
	})
}

// leaderAcquire is called when this node becomes leader
func leaderAcquire(ctx context.Context, app *common.App, block *common.BlockContext) {
	ext := GetExtension()
	ext.isLeader.Store(true)
	ext.logger.Info("acquired leadership, LP rewards sampling enabled")
}

// leaderLose is called when this node loses leadership
func leaderLose(ctx context.Context, app *common.App, block *common.BlockContext) {
	ext := GetExtension()
	ext.isLeader.Store(false)
	ext.logger.Info("lost leadership, LP rewards sampling disabled")
}

// leaderEndBlock is called at the end of each block when this node is leader
func leaderEndBlock(ctx context.Context, app *common.App, block *common.BlockContext) {
	ext := GetExtension()

	if !ext.isLeader.Load() {
		return
	}

	if block == nil {
		return
	}

	blockHeight := block.Height

	// Snapshot config values under RLock to avoid races with reloadConfig
	ext.mu.RLock()
	enabled := ext.enabled
	samplingIntervalBlocks := ext.samplingIntervalBlocks
	configReloadInterval := ext.configReloadInterval
	maxMarketsPerRun := ext.maxMarketsPerRun
	ext.mu.RUnlock()

	// Reload config periodically (do this BEFORE enabled check so we can re-enable without restart)
	if configReloadInterval > 0 && blockHeight-atomic.LoadInt64(&ext.lastCheckedHeight) >= configReloadInterval {
		// Use background context since EndBlockHook context is canceled when block ends
		go ext.reloadConfig(context.Background())
		atomic.StoreInt64(&ext.lastCheckedHeight, blockHeight)
	}

	if !enabled {
		return
	}

	// Check if it's time to sample (every N blocks)
	if samplingIntervalBlocks <= 0 || blockHeight%samplingIntervalBlocks != 0 {
		return
	}

	// Skip if previous sampling run is still in progress (prevents nonce conflicts)
	if !ext.isSampling.CompareAndSwap(false, true) {
		ext.logger.Warn("skipping LP rewards sampling - previous run still in progress",
			"block", blockHeight)
		return
	}

	// Sample LP rewards in background with background context
	// (EndBlockHook context is canceled when block processing ends)
	go ext.sampleLPRewardsWithConfig(context.Background(), blockHeight, maxMarketsPerRun)
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

// sampleLPRewardsWithConfig samples LP rewards for all active markets using provided config
func (ext *Extension) sampleLPRewardsWithConfig(ctx context.Context, blockHeight int64, maxMarketsPerRun int) {
	// Always clear the sampling flag when done
	defer ext.isSampling.Store(false)

	if ext.engOps == nil || ext.signer == nil || ext.broadcaster == nil {
		ext.logger.Warn("LP rewards extension not fully initialized")
		return
	}

	// Get active markets
	markets, err := ext.engOps.GetActiveMarkets(ctx, maxMarketsPerRun)
	if err != nil {
		ext.logger.Warn("failed to get active markets", "error", err)
		return
	}

	if len(markets) == 0 {
		ext.logger.Debug("no active markets to sample", "block", blockHeight)
		return
	}

	ext.logger.Info("sampling LP rewards",
		"block", blockHeight,
		"market_count", len(markets))

	// Get chain ID from service
	chainID := ""
	if ext.service != nil && ext.service.GenesisConfig != nil {
		chainID = ext.service.GenesisConfig.ChainID
	}

	// Sample each market sequentially to avoid nonce conflicts
	// Each transaction must complete before the next one starts
	successCount := 0
	failCount := 0
	for _, queryID := range markets {
		err := ext.engOps.BroadcastSampleLPRewards(
			ctx,
			chainID,
			ext.signer,
			ext.broadcaster.BroadcastTx,
			queryID,
			blockHeight,
		)
		if err != nil {
			ext.logger.Warn("failed to sample LP rewards",
				"query_id", queryID,
				"block", blockHeight,
				"error", err)
			failCount++
			// Continue to next market even if this one fails
			continue
		}

		ext.logger.Debug("sampled LP rewards",
			"query_id", queryID,
			"block", blockHeight)
		successCount++
	}

	ext.logger.Info("LP rewards sampling completed",
		"block", blockHeight,
		"success", successCount,
		"failed", failCount)
}
