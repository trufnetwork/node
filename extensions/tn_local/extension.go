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

	// requireSignature gates application-level auth. When true, every handler
	// rejects requests whose `_auth` header doesn't recover to ext.nodeAddress.
	// Default false — preserves the pre-auth behavior unless an operator
	// explicitly opts in via the [extensions.tn_local] config.
	requireSignature atomic.Bool
	// replayWindowSeconds is the ± skew window for auth timestamps and the
	// max age of replay-cache entries. Atomic so configure() can update it
	// safely after construction (e.g. during reconfigure in tests).
	replayWindowSeconds atomic.Int64
	// replayCache holds recent valid signatures so a duplicate within the
	// window is rejected as a replay. Always non-nil after configure().
	replayCache *replayCache
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

	// Auth defaults — caller can override via configureAuth() after this.
	// Defaults match the "no auth, behave as today" behavior.
	if e.replayWindowSeconds.Load() == 0 {
		e.replayWindowSeconds.Store(defaultReplayWindowSeconds)
	}
	if e.replayCache == nil {
		e.replayCache = newReplayCache(defaultReplayCacheSize, e.replayWindowSeconds.Load())
	}
}

// configureAuth lets the extension opt into application-level signature
// auth (require_signature=true). Window is the ± skew tolerance for auth
// timestamps and the max age for replay-cache entries.
//
// Pass require=false (the default) to keep behavior identical to pre-auth
// tn_local. Pass require=true to reject any request whose `_auth` header
// doesn't recover to this node's address.
func (e *Extension) configureAuth(require bool, windowSeconds int64) {
	if windowSeconds <= 0 {
		windowSeconds = defaultReplayWindowSeconds
	}
	e.replayWindowSeconds.Store(windowSeconds)
	// Rebuild the cache on window change so eviction works against the
	// new window. Cheap — cache is small.
	e.replayCache = newReplayCache(defaultReplayCacheSize, windowSeconds)
	e.requireSignature.Store(require)
}

const (
	// defaultReplayWindowSeconds is the ± clock-skew tolerance and replay-
	// cache age cutoff in seconds. 60s leaves ample room for NTP-synced
	// hosts without making the replay window meaningfully exploitable.
	defaultReplayWindowSeconds int64 = 60
	// defaultReplayCacheSize bounds the replay cache. 100k × 32 bytes ≈ 3MB.
	defaultReplayCacheSize int = 100_000
)

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
