// Package tn_settlement provides automatic settlement for prediction markets.
//
// This extension polls for unsettled markets past their settle_time and
// automatically broadcasts settle_market() transactions when attestations
// are available. It follows the same architecture as tn_digest with
// leader-only execution via leaderwatch coordination.
package tn_settlement

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/node/extensions/tn_settlement/internal"
	"github.com/trufnetwork/node/extensions/tn_settlement/scheduler"
)

type TxBroadcaster interface {
	BroadcastTx(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error)
}

type Extension struct {
	logger    log.Logger
	scheduler *scheduler.SettlementScheduler
	isLeader  atomic.Bool

	// lifecycle wiring
	engineOps *internal.EngineOperations
	service   *common.Service

	// config snapshot
	enabled          bool
	schedule         string
	maxMarketsPerRun int
	retryAttempts    int

	// reload policy
	reloadIntervalBlocks int64
	lastCheckedHeight    int64
	reloadRetryBackoff   time.Duration // backoff between config reload retries (default 1 minute)
	reloadMaxRetries     int           // max retries for config reload (default 15)

	// background retry worker
	retryWorkerCtx    context.Context
	retryWorkerCancel context.CancelFunc
	retrySignal       chan struct{} // signal to trigger retry worker
	retryMu           sync.Mutex    // protects retry state

	// shutdown context for graceful termination
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	// tx submission wiring
	broadcaster TxBroadcaster
	nodeSigner  auth.Signer

	mu sync.Mutex
}

var (
	extensionInstance *Extension
	extensionMu       sync.RWMutex
)

func GetExtension() *Extension {
	// Fast path: read lock
	extensionMu.RLock()
	if extensionInstance != nil {
		defer extensionMu.RUnlock()
		return extensionInstance
	}
	extensionMu.RUnlock()

	// Slow path: write lock to create default instance
	extensionMu.Lock()
	defer extensionMu.Unlock()

	// Double-check after acquiring write lock
	if extensionInstance == nil {
		extensionInstance = &Extension{
			logger:               log.New(log.WithLevel(log.LevelInfo)),
			reloadIntervalBlocks: 100, // Check config every 100 blocks
			maxMarketsPerRun:     10,
			retryAttempts:        3,
		}
	}
	return extensionInstance
}

func SetExtension(ext *Extension) {
	extensionMu.Lock()
	defer extensionMu.Unlock()
	extensionInstance = ext
}

func NewExtension(logger log.Logger, sched *scheduler.SettlementScheduler) *Extension {
	return &Extension{
		logger:               logger,
		scheduler:            sched,
		reloadIntervalBlocks: 100, // Check config every 100 blocks
		maxMarketsPerRun:     10,
		retryAttempts:        3,
	}
}

func (e *Extension) Logger() log.Logger                           { return e.logger }
func (e *Extension) Scheduler() *scheduler.SettlementScheduler    { return e.scheduler }
func (e *Extension) IsLeader() bool                               { return e.isLeader.Load() }
func (e *Extension) setLeader(v bool)                             { e.isLeader.Store(v) }
func (e *Extension) EngineOps() *internal.EngineOperations        { return e.engineOps }
func (e *Extension) SetEngineOps(ops *internal.EngineOperations)  { e.engineOps = ops }
func (e *Extension) Service() *common.Service                     { return e.service }
func (e *Extension) SetService(s *common.Service)                 { e.service = s }
func (e *Extension) ConfigEnabled() bool                          { return e.enabled }
func (e *Extension) Schedule() string                             { return e.schedule }
func (e *Extension) MaxMarketsPerRun() int                        { return e.maxMarketsPerRun }
func (e *Extension) RetryAttempts() int                           { return e.retryAttempts }
func (e *Extension) SetScheduler(s *scheduler.SettlementScheduler) {
	e.mu.Lock()
	oldScheduler := e.scheduler
	if oldScheduler == s {
		e.mu.Unlock()
		return
	}
	e.scheduler = s
	e.mu.Unlock()

	// Stop old scheduler outside lock to avoid blocking
	if oldScheduler != nil {
		_ = oldScheduler.Stop()
	}
}

func (e *Extension) SetConfig(enabled bool, schedule string, maxMarkets, retries int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.enabled = enabled
	e.schedule = schedule
	e.maxMarketsPerRun = maxMarkets
	e.retryAttempts = retries
}

func (e *Extension) SetReloadIntervalBlocks(v int64)       { e.reloadIntervalBlocks = v }
func (e *Extension) ReloadIntervalBlocks() int64           { return e.reloadIntervalBlocks }
func (e *Extension) SetLastCheckedHeight(h int64) { atomic.StoreInt64(&e.lastCheckedHeight, h) }
func (e *Extension) LastCheckedHeight() int64     { return atomic.LoadInt64(&e.lastCheckedHeight) }
func (e *Extension) SetReloadRetryBackoff(d time.Duration) { e.reloadRetryBackoff = d }
func (e *Extension) ReloadRetryBackoff() time.Duration {
	if e.reloadRetryBackoff == 0 {
		return 1 * time.Minute // default
	}
	return e.reloadRetryBackoff
}
func (e *Extension) SetReloadMaxRetries(n int) { e.reloadMaxRetries = n }
func (e *Extension) ReloadMaxRetries() int {
	if e.reloadMaxRetries == 0 {
		return 15 // default
	}
	return e.reloadMaxRetries
}

func (e *Extension) SetBroadcaster(b TxBroadcaster) { e.broadcaster = b }
func (e *Extension) Broadcaster() TxBroadcaster     { return e.broadcaster }
func (e *Extension) SetNodeSigner(s auth.Signer)    { e.nodeSigner = s }
func (e *Extension) NodeSigner() auth.Signer        { return e.nodeSigner }

// ShutdownContext returns the extension's shutdown context, creating it if needed.
// This context is cancelled when Close() is called, allowing graceful termination
// of long-running operations like the scheduler.
func (e *Extension) ShutdownContext() context.Context {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.shutdownCtx == nil {
		e.shutdownCtx, e.shutdownCancel = context.WithCancel(context.Background())
	}
	return e.shutdownCtx
}

// startRetryWorker starts the background config reload retry worker
func (e *Extension) startRetryWorker() {
	e.retryMu.Lock()
	defer e.retryMu.Unlock()
	if e.retryWorkerCancel != nil {
		return // already started
	}
	e.retryWorkerCtx, e.retryWorkerCancel = context.WithCancel(context.Background())
	e.retrySignal = make(chan struct{}, 1) // buffered to avoid blocking
	go e.retryWorkerLoop()
}

// stopRetryWorker stops the background retry worker
func (e *Extension) stopRetryWorker() {
	e.retryMu.Lock()
	defer e.retryMu.Unlock()
	if e.retryWorkerCancel != nil {
		e.retryWorkerCancel()
		e.retryWorkerCancel = nil
	}
}

// signalRetryNeeded signals the background worker that a retry is needed
func (e *Extension) signalRetryNeeded() {
	select {
	case e.retrySignal <- struct{}{}:
	default: // already signaled
	}
}

// retryWorkerLoop is the background goroutine that retries config reloads
func (e *Extension) retryWorkerLoop() {
	for {
		select {
		case <-e.retryWorkerCtx.Done():
			return
		case <-e.retrySignal:
			e.retryConfigReload()
		}
	}
}

// retryConfigReload performs multiple retry attempts with backoff
func (e *Extension) retryConfigReload() {
	engineOps := e.EngineOps()
	if engineOps == nil {
		e.Logger().Warn("engine operations not initialized, cannot reload config")
		return
	}

	backoff := e.ReloadRetryBackoff()
	maxRetries := e.ReloadMaxRetries()

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			e.Logger().Warn("retrying config reload in background", "attempt", attempt, "backoff", backoff)
			select {
			case <-e.retryWorkerCtx.Done():
				e.Logger().Info("retry worker cancelled")
				return
			case <-time.After(backoff):
			}
		}

		enabled, schedule, maxMarkets, retries, err := engineOps.LoadSettlementConfig(e.retryWorkerCtx)
		if err == nil {
			e.Logger().Info("config reload succeeded in background",
				"attempt", attempt,
				"enabled", enabled,
				"schedule", schedule,
				"max_markets", maxMarkets,
				"retries", retries)
			e.applyConfigChangeWithLock(e.retryWorkerCtx, enabled, schedule, maxMarkets, retries, nil)
			return
		}

		// Check if context was cancelled during LoadSettlementConfig
		if e.retryWorkerCtx.Err() != nil {
			e.Logger().Info("retry worker cancelled during config reload")
			return
		}

		e.Logger().Warn("config reload attempt failed", "attempt", attempt, "error", err)
	}

	e.Logger().Error("all config reload retries failed in background, keeping current config", "attempts", maxRetries)
}

// applyConfigChangeWithLock applies config changes with proper synchronization
func (e *Extension) applyConfigChangeWithLock(ctx context.Context, enabled bool, schedule string, maxMarkets, retries int, app *common.App) {
	e.retryMu.Lock()
	defer e.retryMu.Unlock()

	if schedule == "" {
		schedule = DefaultSettlementSchedule
	}
	if maxMarkets <= 0 {
		maxMarkets = 10
	}
	if retries <= 0 {
		retries = 3
	}

	if enabled != e.ConfigEnabled() || schedule != e.Schedule() || maxMarkets != e.MaxMarketsPerRun() || retries != e.RetryAttempts() {
		e.Logger().Info("settlement config changed, updating scheduler",
			"old_enabled", e.ConfigEnabled(),
			"new_enabled", enabled,
			"old_schedule", e.Schedule(),
			"new_schedule", schedule,
			"old_max_markets", e.MaxMarketsPerRun(),
			"new_max_markets", maxMarkets,
			"old_retries", e.RetryAttempts(),
			"new_retries", retries,
			"is_leader", e.IsLeader())

		e.SetConfig(enabled, schedule, maxMarkets, retries)

		if !enabled {
			e.stopSchedulerIfRunning()
			e.Logger().Info("tn_settlement stopped due to config disabled")
		} else if e.IsLeader() {
			service := e.Service()
			if app != nil && app.Service != nil {
				service = app.Service
				if e.Service() == nil {
					e.SetService(service)
				}
			}
			if e.Scheduler() == nil && !e.ensureSchedulerWithService(service) {
				e.Logger().Debug("tn_settlement: prerequisites missing; deferring (re)start after config update")
			} else if e.Scheduler() != nil {
				e.stopSchedulerIfRunning()
				// Use extension's shutdown context for scheduler - ensures graceful shutdown
				if err := e.startScheduler(e.ShutdownContext()); err != nil {
					e.Logger().Warn("failed to (re)start tn_settlement scheduler after config update", "error", err)
				} else {
					e.Logger().Info("tn_settlement (re)started with new schedule", "schedule", e.Schedule())
				}
			}
		} else {
			e.Logger().Info("tn_settlement config enabled but not leader, will start when leadership acquired")
		}
	}
}

// Close stops background jobs and cancels the shutdown context
func (e *Extension) Close() {
	e.stopRetryWorker()
	if e.scheduler != nil {
		_ = e.scheduler.Stop()
	}
	// Cancel shutdown context to signal any operations using it
	e.mu.Lock()
	if e.shutdownCancel != nil {
		e.shutdownCancel()
		e.shutdownCancel = nil
	}
	e.mu.Unlock()
}
