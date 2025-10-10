package tn_attestation

import (
	"context"
	"fmt"
	"sync"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
)

// signerExtension captures node-level wiring required for the attestation signer.
// The struct will evolve as we thread additional dependencies (engine, accounts,
// signer, broadcaster, etc.) through the extension during subsequent steps.
type signerExtension struct {
	logger  log.Logger
	service *common.Service

	scanIntervalBlocks int64
	scanBatchLimit     int64
	lastScanHeight     int64
	isLeader           bool

	engine   common.Engine
	db       sql.DB
	accounts common.Accounts

	broadcaster TxBroadcaster
	nodeSigner  auth.Signer

	processOverride func(context.Context, []string)

	mu sync.RWMutex
}

var (
	extensionOnce sync.Once
	extensionInst *signerExtension
)

// getExtension returns the singleton instance, initialising it lazily so tests
// can replace or reset state as needed.
func getExtension() *signerExtension {
	extensionOnce.Do(func() {
		extensionInst = &signerExtension{
			logger:             log.New(log.WithLevel(log.LevelInfo)).New(ExtensionName),
			scanIntervalBlocks: 100,
			scanBatchLimit:     100,
		}
	})
	return extensionInst
}

// SetExtension allows tests to inject a pre-configured instance.
func SetExtension(ext *signerExtension) {
	extensionInst = ext
}

// Logger provides the extension logger, defaulting to a module-specific child of
// the global logger.
func (e *signerExtension) Logger() log.Logger {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.logger
}

// Service retrieves the cached service pointer. The service includes configs,
// identity, and logger; storing it lets the extension re-use those resources
// outside hook invocations.
func (e *signerExtension) Service() *common.Service {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.service
}

// setService captures the service and refreshes the module logger.
func (e *signerExtension) setService(svc *common.Service) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.service = svc
	if svc != nil && svc.Logger != nil {
		e.logger = svc.Logger.New(ExtensionName)
	}
}

func (e *signerExtension) applyConfig(service *common.Service) {
	if service == nil || service.LocalConfig == nil {
		return
	}
	if cfg, ok := service.LocalConfig.Extensions[ExtensionName]; ok {
		if v, ok := cfg["scan_interval_blocks"]; ok && v != "" {
			if parsed, err := parsePositiveInt64(v); err == nil {
				e.setScanIntervalBlocks(parsed)
			} else {
				e.Logger().Warn("invalid scan_interval_blocks; using default", "value", v, "error", err)
			}
		}
		if v, ok := cfg["scan_batch_limit"]; ok && v != "" {
			if parsed, err := parsePositiveInt64(v); err == nil {
				e.setScanBatchLimit(parsed)
			} else {
				e.Logger().Warn("invalid scan_batch_limit; using default", "value", v, "error", err)
			}
		}
	}
}

func (e *signerExtension) setApp(app *common.App) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if app != nil {
		e.engine = app.Engine
		e.db = app.DB
		e.accounts = app.Accounts
	}
}

func (e *signerExtension) Engine() common.Engine {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.engine
}

func (e *signerExtension) DB() sql.DB {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.db
}

func (e *signerExtension) Accounts() common.Accounts {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.accounts
}

func (e *signerExtension) setBroadcaster(b TxBroadcaster) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.broadcaster = b
}

func (e *signerExtension) Broadcaster() TxBroadcaster {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.broadcaster
}

func (e *signerExtension) setNodeSigner(s auth.Signer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nodeSigner = s
}

func (e *signerExtension) NodeSigner() auth.Signer {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.nodeSigner
}

func (e *signerExtension) setProcessOverride(fn func(context.Context, []string)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.processOverride = fn
}

func (e *signerExtension) setLeader(isLeader bool, height int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.isLeader = isLeader
	if isLeader && height > 0 {
		e.lastScanHeight = height
	}
}

func (e *signerExtension) Leader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

func (e *signerExtension) setScanIntervalBlocks(v int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if v > 0 {
		e.scanIntervalBlocks = v
	}
}

func (e *signerExtension) setScanBatchLimit(v int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if v > 0 {
		e.scanBatchLimit = v
	}
}

func (e *signerExtension) ScanIntervalBlocks() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.scanIntervalBlocks <= 0 {
		return 100
	}
	return e.scanIntervalBlocks
}

func (e *signerExtension) ScanBatchLimit() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.scanBatchLimit <= 0 {
		return 100
	}
	return e.scanBatchLimit
}

func (e *signerExtension) recordScanHeight(height int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if height > e.lastScanHeight {
		e.lastScanHeight = height
	}
}

func (e *signerExtension) LastScanHeight() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastScanHeight
}

func (e *signerExtension) shouldPerformScan(height int64) bool {
	interval := e.ScanIntervalBlocks()
	if interval <= 0 {
		return false
	}

	last := e.LastScanHeight()
	if last == 0 {
		e.recordScanHeight(height)
		return true
	}

	if height-last >= interval {
		e.recordScanHeight(height)
		return true
	}
	return false
}

func parsePositiveInt64(raw string) (int64, error) {
	var v int64
	_, err := fmt.Sscan(raw, &v)
	if err != nil {
		return 0, err
	}
	if v <= 0 {
		return 0, fmt.Errorf("value must be positive, got %d", v)
	}
	return v, nil
}

// TxBroadcaster matches the subset of the JSON-RPC client used by the signing
// worker to inject transactions.
type TxBroadcaster interface {
	BroadcastTx(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error)
}
