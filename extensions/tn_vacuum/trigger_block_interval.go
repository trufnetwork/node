package tn_vacuum

import (
	"context"
	"fmt"
	"sync"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
)

type blockIntervalTrigger struct {
	mu        sync.Mutex
	fire      func(context.Context, FireOpts) error
	logger    log.Logger
	interval  int64
	lastFired int64
}

func newBlockIntervalTrigger(logger log.Logger) *blockIntervalTrigger {
	return &blockIntervalTrigger{logger: logger}
}

func (t *blockIntervalTrigger) Kind() string { return TriggerBlockInterval }

func (t *blockIntervalTrigger) Configure(ctx context.Context, cfg TriggerConfig, fire func(context.Context, FireOpts) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.logger = t.logger.New("trigger.block_interval")
	if cfg.BlockInterval < minBlockInterval {
		cfg.BlockInterval = minBlockInterval
	}
	if cfg.BlockInterval == 0 {
		cfg.BlockInterval = 100
	}
	t.interval = cfg.BlockInterval
	t.fire = fire
	t.lastFired = 0
	t.logger.Info("block interval trigger configured", "interval", t.interval)
	return nil
}

func (t *blockIntervalTrigger) Start(ctx context.Context) error {
	t.logger.Info("block interval trigger active")
	return nil
}

func (t *blockIntervalTrigger) Stop(ctx context.Context) error {
	t.logger.Info("block interval trigger stopped")
	return nil
}

func (t *blockIntervalTrigger) OnLeaderChange(ctx context.Context, isLeader bool) error {
	if !isLeader {
		t.logger.Debug("block interval trigger paused")
	}
	return nil
}

func (t *blockIntervalTrigger) OnEndBlock(ctx context.Context, block *common.BlockContext) error {
	if block == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.fire == nil {
		return nil
	}
	height := block.Height
	if t.lastFired == 0 || height-t.lastFired >= t.interval {
		if err := t.fire(ctx, FireOpts{Reason: fmt.Sprintf("block_interval:%d", height)}); err != nil {
			t.logger.Warn("block interval trigger failed", "error", err)
		} else {
			t.lastFired = height
		}
	}
	return nil
}
