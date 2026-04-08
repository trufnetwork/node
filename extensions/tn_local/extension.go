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
	// nodeAddress is the lowercase 0x-prefixed Ethereum address derived from the
	// node's secp256k1 operator key. It is the implicit data_provider for every
	// local stream operation — clients never supply a data_provider. Empty when
	// the node has no secp256k1 key (read-only / ed25519 nodes); in that case
	// isEnabled stays false and all handlers return a clear error.
	nodeAddress string
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
//
// nodeAddress must be the lowercase 0x-prefixed Ethereum address derived from
// the node's secp256k1 key. If empty (no secp256k1 key available), the
// extension stays disabled and all handlers return an error — local streams
// require a stable operator identity to claim ownership of.
func (e *Extension) configure(logger log.Logger, db sql.DB, localDB *LocalDB, nodeAddress string) {
	e.logger = logger
	e.db = db
	e.localDB = localDB
	e.nodeAddress = nodeAddress
	// Always set isEnabled deterministically based on nodeAddress so that
	// re-configuring an extension (e.g. in tests or on node re-init) can't
	// leave a stale-true flag when the new address is empty.
	e.isEnabled.Store(nodeAddress != "")
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
