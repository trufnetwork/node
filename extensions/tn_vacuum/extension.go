package tn_vacuum

import (
	"context"
	"sync"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
)

type Extension struct {
	mu                   sync.RWMutex
	logger               log.Logger
	service              *common.Service
	config               Config
	trigger              Trigger
	mechanism            Mechanism
	runner               *Runner
	isLeader             bool
	reloadIntervalBlocks int64
	lastConfigHeight     int64
}

var (
	extInstance *Extension
	once        sync.Once
)

func GetExtension() *Extension {
	once.Do(func() {
		extInstance = &Extension{
			logger:               log.New(log.WithLevel(log.LevelInfo)),
			reloadIntervalBlocks: defaultReloadBlocks,
		}
	})
	return extInstance
}

func SetExtension(e *Extension) {
	extInstance = e
}

func ResetForTest() {
	once = sync.Once{}
	extInstance = nil
}

func (e *Extension) Logger() log.Logger {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.logger
}

func (e *Extension) setLogger(l log.Logger) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logger = l
}

func (e *Extension) Service() *common.Service {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.service
}

func (e *Extension) setService(s *common.Service) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.service = s
}

func (e *Extension) Config() Config {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.config
}

func (e *Extension) setConfig(cfg Config) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.config = cfg
}

func (e *Extension) setMechanism(m Mechanism) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mechanism = m
}

func (e *Extension) Mechanism() Mechanism {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.mechanism
}

func (e *Extension) setTrigger(t Trigger) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.trigger = t
}

func (e *Extension) Trigger() Trigger {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.trigger
}

func (e *Extension) ensureRunner() *Runner {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runner == nil {
		e.runner = &Runner{logger: e.logger}
	}
	return e.runner
}

func (e *Extension) setLeader(v bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.isLeader = v
}

func (e *Extension) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

func (e *Extension) SetReloadIntervalBlocks(v int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reloadIntervalBlocks = v
}

func (e *Extension) ReloadIntervalBlocks() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.reloadIntervalBlocks
}

func (e *Extension) SetLastConfigHeight(h int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastConfigHeight = h
}

func (e *Extension) LastConfigHeight() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastConfigHeight
}

func (e *Extension) Close(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.trigger != nil {
		_ = e.trigger.Stop(ctx)
		e.trigger = nil
	}
	if e.mechanism != nil {
		_ = e.mechanism.Close(ctx)
		e.mechanism = nil
	}
	e.runner = nil
}

func (e *Extension) reconfigure(ctx context.Context, cfg Config, deps MechanismDeps) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.trigger != nil {
		_ = e.trigger.Stop(ctx)
	}
	if e.mechanism != nil {
		_ = e.mechanism.Close(ctx)
	}

	mech := newMechanism()
	if err := mech.Prepare(ctx, deps); err != nil {
		return err
	}

	trig, err := newTrigger(cfg.Trigger.Kind)
	if err != nil {
		return err
	}
	fire := func(callCtx context.Context, opts FireOpts) error {
		return e.ensureRunner().Execute(callCtx, RunnerArgs{Mechanism: mech, Trigger: trig, Logger: e.logger, Reason: opts.Reason})
	}
	if err := trig.Configure(ctx, cfg.Trigger, fire); err != nil {
		return err
	}

	e.reloadIntervalBlocks = cfg.ReloadIntervalBlocks
	e.config = cfg
	e.mechanism = mech
	e.trigger = trig
	e.runner = &Runner{logger: e.logger}

	return nil
}

func (e *Extension) startTriggerIfLeader(ctx context.Context) {
	e.mu.RLock()
	trig := e.trigger
	cfg := e.config
	leader := e.isLeader
	e.mu.RUnlock()
	if !leader || trig == nil || !cfg.Enabled {
		return
	}
	_ = trig.Start(ctx)
}
