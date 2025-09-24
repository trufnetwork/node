package tn_vacuum

import (
	"context"
	"fmt"
	"sync"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
)

type digestCoupledTrigger struct {
	mu     sync.RWMutex
	fire   func(context.Context, FireOpts) error
	logger log.Logger
}

func newDigestTrigger(logger log.Logger) *digestCoupledTrigger {
	return &digestCoupledTrigger{logger: logger}
}

func (t *digestCoupledTrigger) Kind() string { return TriggerDigestCoupled }

func (t *digestCoupledTrigger) Configure(ctx context.Context, cfg TriggerConfig, fire func(context.Context, FireOpts) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.logger = t.logger.New("trigger.digest")
	t.fire = fire
	t.logger.Info("digest trigger configured")
	return nil
}

func (t *digestCoupledTrigger) Start(ctx context.Context) error {
	t.logger.Info("digest trigger start - waiting for notifications")
	return nil
}

func (t *digestCoupledTrigger) Stop(ctx context.Context) error {
	t.logger.Info("digest trigger stop")
	return nil
}

func (t *digestCoupledTrigger) OnLeaderChange(ctx context.Context, isLeader bool) error {
	if !isLeader {
		t.logger.Debug("digest trigger paused - not leader")
	}
	return nil
}

func (t *digestCoupledTrigger) OnEndBlock(ctx context.Context, block *common.BlockContext) error {
	return nil
}

func (t *digestCoupledTrigger) NotifyDigestComplete(ctx context.Context, reason string) error {
	t.mu.RLock()
	fire := t.fire
	t.mu.RUnlock()
	if fire == nil {
		return fmt.Errorf("digest trigger not configured")
	}
	return fire(ctx, FireOpts{Reason: reason})
}
