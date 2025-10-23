// Package tn_digest provides the TrufNetwork digest extension (stub version).
//
// This minimal version wires a scheduler that periodically invokes a no-op
// Kuneiform action `auto_digest()` so we can verify scheduling and wiring
// without any consensus-impacting behavior.
package tn_digest

import (
	"context"
	"sync"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/node/extensions/tn_digest/internal"
	"github.com/trufnetwork/node/extensions/tn_digest/scheduler"
)

type TxBroadcaster interface {
	BroadcastTx(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error)
}

type Extension struct {
	logger    log.Logger
	scheduler *scheduler.DigestScheduler
	isLeader  bool

	// lifecycle wiring
	engineOps *internal.EngineOperations
	service   *common.Service

	// config snapshot
	enabled  bool
	schedule string

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

	// tx submission wiring
	broadcaster TxBroadcaster
	nodeSigner  auth.Signer
}

var (
	extensionInstance *Extension
	once              sync.Once
)

func GetExtension() *Extension {
	if extensionInstance == nil {
		once.Do(func() {
			if extensionInstance == nil {
				extensionInstance = &Extension{
					logger: log.New(log.WithLevel(log.LevelInfo)),
				}
			}
		})
	}
	return extensionInstance
}
func SetExtension(ext *Extension) { extensionInstance = ext }

func NewExtension(logger log.Logger, sched *scheduler.DigestScheduler) *Extension {
	return &Extension{logger: logger, scheduler: sched}
}

func (e *Extension) Logger() log.Logger                    { return e.logger }
func (e *Extension) Scheduler() *scheduler.DigestScheduler { return e.scheduler }
func (e *Extension) IsLeader() bool                        { return e.isLeader }
func (e *Extension) setLeader(v bool)                      { e.isLeader = v }

func (e *Extension) EngineOps() *internal.EngineOperations { return e.engineOps }
func (e *Extension) SetEngineOps(ops *internal.EngineOperations) {
	e.engineOps = ops
}
func (e *Extension) Service() *common.Service { return e.service }
func (e *Extension) SetService(s *common.Service) {
	e.service = s
}
func (e *Extension) SetConfig(enabled bool, schedule string) {
	e.enabled = enabled
	e.schedule = schedule
}
func (e *Extension) ConfigEnabled() bool { return e.enabled }
func (e *Extension) Schedule() string    { return e.schedule }
func (e *Extension) SetScheduler(s *scheduler.DigestScheduler) {
	if e.scheduler == s {
		return
	}
	if e.scheduler != nil {
		_ = e.scheduler.Stop()
	}
	e.scheduler = s
}

func (e *Extension) SetReloadIntervalBlocks(v int64)       { e.reloadIntervalBlocks = v }
func (e *Extension) ReloadIntervalBlocks() int64           { return e.reloadIntervalBlocks }
func (e *Extension) SetLastCheckedHeight(h int64)          { e.lastCheckedHeight = h }
func (e *Extension) LastCheckedHeight() int64              { return e.lastCheckedHeight }
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

// startRetryWorker starts the background config reload retry worker
func (e *Extension) startRetryWorker() {
	if e.retryWorkerCancel != nil {
		return // already started
	}
	e.retryWorkerCtx, e.retryWorkerCancel = context.WithCancel(context.Background())
	e.retrySignal = make(chan struct{}, 1) // buffered to avoid blocking
	go e.retryWorkerLoop()
}

// stopRetryWorker stops the background retry worker
func (e *Extension) stopRetryWorker() {
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

		enabled, schedule, err := e.EngineOps().LoadDigestConfig(e.retryWorkerCtx)
		if err == nil {
			// Success! Update config (app=nil since we're in background, service already cached)
			e.Logger().Info("config reload succeeded in background", "attempt", attempt, "enabled", enabled, "schedule", schedule)
			e.applyConfigChangeWithLock(e.retryWorkerCtx, enabled, schedule, nil)
			return
		}

		// Check if context was cancelled during LoadDigestConfig
		if e.retryWorkerCtx.Err() != nil {
			e.Logger().Info("retry worker cancelled during config reload")
			return
		}

		e.Logger().Warn("config reload attempt failed", "attempt", attempt, "error", err)
	}

	e.Logger().Error("all config reload retries failed in background, keeping current config", "attempts", maxRetries)
}

// applyConfigChangeWithLock applies config changes with proper synchronization
// This method is called from both digestLeaderEndBlock and the background retry worker
func (e *Extension) applyConfigChangeWithLock(ctx context.Context, enabled bool, schedule string, app *common.App) {
	e.retryMu.Lock()
	defer e.retryMu.Unlock()

	if schedule == "" {
		schedule = DefaultDigestSchedule
	}

	if enabled != e.ConfigEnabled() || schedule != e.Schedule() {
		e.Logger().Info("digest config changed, updating scheduler",
			"old_enabled", e.ConfigEnabled(),
			"new_enabled", enabled,
			"old_schedule", e.Schedule(),
			"new_schedule", schedule,
			"is_leader", e.IsLeader())
		e.SetConfig(enabled, schedule)
		if !enabled {
			e.stopSchedulerIfRunning()
			e.Logger().Info("tn_digest stopped due to config disabled")
		} else if e.IsLeader() {
			service := e.Service()
			if app != nil && app.Service != nil {
				service = app.Service
				if e.Service() == nil {
					e.SetService(service)
				}
			}
			if e.Scheduler() == nil && !e.ensureSchedulerWithService(service) {
				e.Logger().Debug("tn_digest: prerequisites missing; deferring (re)start after config update")
			} else if e.Scheduler() != nil {
				e.stopSchedulerIfRunning()
				if err := e.startScheduler(ctx); err != nil {
					e.Logger().Warn("failed to (re)start tn_digest scheduler after config update", "error", err)
				} else {
					e.Logger().Info("tn_digest (re)started with new schedule", "schedule", e.Schedule())
				}
			}
		} else {
			e.Logger().Info("tn_digest config enabled but not leader, will start when leadership acquired")
		}
	}
}

// Close stops background jobs.
func (e *Extension) Close() {
	e.stopRetryWorker()
	if e.scheduler != nil {
		_ = e.scheduler.Stop()
	}
}
