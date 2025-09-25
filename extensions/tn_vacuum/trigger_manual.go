package tn_vacuum

import (
	"context"
	"fmt"
	"sync"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
)

type manualTrigger struct {
	mu     sync.RWMutex
	fire   func(context.Context, FireOpts) error
	logger log.Logger
}

func newManualTrigger(logger log.Logger) *manualTrigger {
	return &manualTrigger{logger: logger}
}

func (t *manualTrigger) Kind() string { return TriggerManual }

func (t *manualTrigger) Configure(ctx context.Context, cfg TriggerConfig, fire func(context.Context, FireOpts) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.logger = t.logger.New("trigger.manual")
	t.fire = fire
	t.logger.Info("manual trigger configured")
	return nil
}

func (t *manualTrigger) Start(ctx context.Context) error {
	t.logger.Info("manual trigger ready")
	return nil
}

func (t *manualTrigger) Stop(ctx context.Context) error {
	t.logger.Info("manual trigger stopped")
	return nil
}

func (t *manualTrigger) OnLeaderChange(ctx context.Context, isLeader bool) error {
	if !isLeader {
		t.logger.Debug("manual trigger idle - not leader")
	}
	return nil
}

func (t *manualTrigger) OnEndBlock(ctx context.Context, block *common.BlockContext) error {
	return nil
}

func (t *manualTrigger) Fire(ctx context.Context, reason string) error {
	t.mu.RLock()
	fire := t.fire
	t.mu.RUnlock()
	if fire == nil {
		return fmt.Errorf("manual trigger not configured")
	}
	return fire(ctx, FireOpts{Reason: reason})
}
