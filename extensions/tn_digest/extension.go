// Package tn_digest provides the TrufNetwork digest extension (stub version).
//
// This minimal version wires a scheduler that periodically invokes a no-op
// Kuneiform action `auto_digest()` so we can verify scheduling and wiring
// without any consensus-impacting behavior.
package tn_digest

import (
	"context"
	"sync"

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
	e.scheduler = s
}

func (e *Extension) SetReloadIntervalBlocks(v int64) { e.reloadIntervalBlocks = v }
func (e *Extension) ReloadIntervalBlocks() int64     { return e.reloadIntervalBlocks }
func (e *Extension) SetLastCheckedHeight(h int64)    { e.lastCheckedHeight = h }
func (e *Extension) LastCheckedHeight() int64        { return e.lastCheckedHeight }

func (e *Extension) SetBroadcaster(b TxBroadcaster) { e.broadcaster = b }
func (e *Extension) Broadcaster() TxBroadcaster     { return e.broadcaster }
func (e *Extension) SetNodeSigner(s auth.Signer)    { e.nodeSigner = s }
func (e *Extension) NodeSigner() auth.Signer        { return e.nodeSigner }

// Close stops background jobs.
func (e *Extension) Close() {
	if e.scheduler != nil {
		_ = e.scheduler.Stop()
	}
}
