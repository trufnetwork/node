package tn_cache

import (
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
)

type Extension struct {
	logger          log.Logger
	cacheDB         *internal.CacheDB
	scheduler       *CacheScheduler
	syncChecker     *SyncChecker
	metricsRecorder metrics.MetricsRecorder
	cachePool       interface{} // Can be *pgxpool.Pool or pgxmock.PgxPoolIface for testing
	isEnabled       bool
}

var extensionInstance *Extension

func GetExtension() *Extension {
	return extensionInstance
}

func SetExtension(ext *Extension) {
	extensionInstance = ext
}

func NewExtension(logger log.Logger, cacheDB *internal.CacheDB, scheduler *CacheScheduler,
	syncChecker *SyncChecker, metricsRecorder metrics.MetricsRecorder,
	cachePool interface{}, isEnabled bool) *Extension {
	return &Extension{
		logger:          logger,
		cacheDB:         cacheDB,
		scheduler:       scheduler,
		syncChecker:     syncChecker,
		metricsRecorder: metricsRecorder,
		cachePool:       cachePool,
		isEnabled:       isEnabled,
	}
}

func (e *Extension) Logger() log.Logger                    { return e.logger }
func (e *Extension) CacheDB() *internal.CacheDB            { return e.cacheDB }
func (e *Extension) Scheduler() *CacheScheduler            { return e.scheduler }
func (e *Extension) SyncChecker() *SyncChecker             { return e.syncChecker }
func (e *Extension) MetricsRecorder() metrics.MetricsRecorder { return e.metricsRecorder }
func (e *Extension) CachePool() interface{}                 { return e.cachePool }
func (e *Extension) IsEnabled() bool                       { return e.isEnabled }