package tn_local

import (
	"sync"
	"sync/atomic"

	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// Extension holds all state for the tn_local extension.
type Extension struct {
	logger    log.Logger
	db        sql.DB
	localDB   *LocalDB
	isEnabled atomic.Bool
	// height tracks the latest committed block height, updated by EndBlockHook.
	// Local streams use the same created_at = block height semantics as consensus.
	height atomic.Int64
}

var (
	extensionInstance *Extension
	once             sync.Once
)

// GetExtension returns the singleton Extension instance.
// The returned pointer is stable — adminServerHook registers it as a Svc,
// so it must never be replaced. Use configure() to update fields in-place.
func GetExtension() *Extension {
	once.Do(func() {
		extensionInstance = &Extension{
			logger: log.New(log.WithLevel(log.LevelInfo)),
		}
	})
	return extensionInstance
}

// configure updates the extension's internal state in-place.
// This preserves the pointer identity so that the admin server's registered
// Svc reference remains valid. Called during sequential startup before the
// server accepts requests.
func (e *Extension) configure(logger log.Logger, db sql.DB, localDB *LocalDB) {
	e.logger = logger
	e.db = db
	e.localDB = localDB
	e.isEnabled.Store(true)
}

// currentHeight returns the latest committed block height.
// Falls back to 0 if no blocks have been processed yet (e.g. during tests).
func (e *Extension) currentHeight() int64 {
	return e.height.Load()
}

// Close closes the extension's connection pool.
func (e *Extension) Close() {
	if e.db != nil {
		if wrapper, ok := e.db.(*PoolDBWrapper); ok {
			wrapper.Close()
			e.logger.Info("closed local storage connection pool")
		}
	}
}
