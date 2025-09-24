package tn_vacuum

import (
	"context"
	"sync"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
)

type cronTrigger struct {
	mu      sync.Mutex
	fire    func(context.Context, FireOpts) error
	logger  log.Logger
	running bool
	cancel  context.CancelFunc
}

func newCronTrigger(logger log.Logger) *cronTrigger {
	return &cronTrigger{logger: logger}
}

func (t *cronTrigger) Kind() string { return TriggerCron }

func (t *cronTrigger) Configure(ctx context.Context, cfg TriggerConfig, fire func(context.Context, FireOpts) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.logger = t.logger.New("trigger.cron")
	t.fire = fire
	t.logger.Info("cron trigger configured", "schedule", cfg.CronSchedule)
	return nil
}

func (t *cronTrigger) Start(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running {
		return nil
	}
	loopCtx, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	t.running = true
	go t.loop(loopCtx)
	t.logger.Info("cron trigger loop started (stub)")
	return nil
}

func (t *cronTrigger) loop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.mu.Lock()
			fire := t.fire
			t.mu.Unlock()
			if fire != nil {
				if err := fire(ctx, FireOpts{Reason: "cron_stub"}); err != nil {
					t.logger.Warn("cron trigger fire failed", "error", err)
				}
			}
		}
	}
}

func (t *cronTrigger) Stop(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.running {
		return nil
	}
	if t.cancel != nil {
		t.cancel()
	}
	t.running = false
	t.logger.Info("cron trigger stopped")
	return nil
}

func (t *cronTrigger) OnLeaderChange(ctx context.Context, isLeader bool) error {
	if !isLeader {
		t.logger.Debug("cron trigger paused")
	}
	return nil
}

func (t *cronTrigger) OnEndBlock(ctx context.Context, block *common.BlockContext) error {
	return nil
}
