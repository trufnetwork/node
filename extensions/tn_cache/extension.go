// Package tn_cache provides the TrufNetwork cache extension.
//
// extension.go manages the singleton Extension instance that holds all
// cache subsystems (DB, scheduler, metrics). This centralized state
// allows SQL precompiles and background jobs to access cache functionality.
package tn_cache

import (
	"sync"

	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
	"github.com/trufnetwork/node/extensions/tn_cache/scheduler"
	"github.com/trufnetwork/node/extensions/tn_cache/syncschecker"
	"github.com/trufnetwork/node/extensions/tn_cache/utilities"
)

type Extension struct {
	logger           log.Logger
	scheduler        *scheduler.CacheScheduler
	syncChecker      *syncschecker.SyncChecker
	metricsRecorder  metrics.MetricsRecorder
	db               sql.DB
	isEnabled        bool
	cacheDB          *internal.CacheDB
	engineOperations *internal.EngineOperations
}

var (
	extensionInstance *Extension
	once              sync.Once
)

func GetExtension() *Extension {
	once.Do(func() {
		// Minimal fallback init - full init happens in engine ready hook
		extensionInstance = &Extension{
			logger:    log.New(log.WithLevel(log.LevelInfo)),
			isEnabled: false, // Default disabled state
		}
	})
	return extensionInstance
}

func SetExtension(ext *Extension) {
	extensionInstance = ext
}

func NewExtension(logger log.Logger, cacheDB *internal.CacheDB, scheduler *scheduler.CacheScheduler,
	syncChecker *syncschecker.SyncChecker, metricsRecorder metrics.MetricsRecorder, engineOperations *internal.EngineOperations,
	db sql.DB, isEnabled bool) *Extension {
	return &Extension{
		logger:           logger,
		scheduler:        scheduler,
		syncChecker:      syncChecker,
		metricsRecorder:  metricsRecorder,
		db:               db,
		isEnabled:        isEnabled,
		cacheDB:          cacheDB,
		engineOperations: engineOperations,
	}
}

// Close closes the extension's connection pool if it's a PoolDBWrapper
func (e *Extension) Close() {
	if e.db != nil {
		if wrapper, ok := e.db.(*utilities.PoolDBWrapper); ok {
			wrapper.Close()
			e.logger.Info("closed cache connection pool")
		}
	}
}

// useful for testing, when we want all the extension's methods to use the same transaction
func (e *Extension) SetTx(tx sql.DB) {
	e.cacheDB.SetTx(tx)
	e.engineOperations.SetTx(tx)
}

func (e *Extension) Logger() log.Logger                           { return e.logger }
func (e *Extension) Scheduler() *scheduler.CacheScheduler         { return e.scheduler }
func (e *Extension) SyncChecker() *syncschecker.SyncChecker       { return e.syncChecker }
func (e *Extension) MetricsRecorder() metrics.MetricsRecorder     { return e.metricsRecorder }
func (e *Extension) DB() sql.DB                                   { return e.db }
func (e *Extension) CacheDB() *internal.CacheDB                   { return e.cacheDB }
func (e *Extension) EngineOperations() *internal.EngineOperations { return e.engineOperations }
func (e *Extension) IsEnabled() bool                              { return e.isEnabled }
