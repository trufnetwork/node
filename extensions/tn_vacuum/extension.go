package tn_vacuum

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_vacuum/metrics"
)

type Extension struct {
	mu         sync.RWMutex
	logger     log.Logger
	service    *common.Service
	config     Config
	mechanism  Mechanism
	runner     *Runner
	state      runState
	stateStore stateStore
	now        func() time.Time
	metrics    metrics.MetricsRecorder
}

var (
	extInstance *Extension
	once        sync.Once
)

func GetExtension() *Extension {
	once.Do(func() {
		logger := log.New(log.WithLevel(log.LevelInfo))
		extInstance = &Extension{
			logger:  logger,
			metrics: metrics.NewMetricsRecorder(logger),
			now:     time.Now,
		}
	})
	return extInstance
}

func SetExtension(e *Extension) {
	if e != nil && e.now == nil {
		e.now = time.Now
	}
	extInstance = e
}

func ResetForTest() {
	once = sync.Once{}
	extInstance = nil
}

// setStateStore overrides the persistent state backend. Tests use this to
// inject a stub without touching a real database connection.
func (e *Extension) setStateStore(store stateStore) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stateStore = store
}

// setNowFunc overrides the clock used for persisted timestamps. Tests provide
// deterministic values through this hook.
func (e *Extension) setNowFunc(now func() time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.now = now
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
	e.metrics = metrics.NewMetricsRecorder(l)
}

func (e *Extension) setService(s *common.Service) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.service = s
}

// initializeState prepares the persistence backend and loads the last run
// information from disk. It is safe to call multiple times; the underlying
// operations are idempotent.
func (e *Extension) initializeState(ctx context.Context, db sql.DB) {
	e.mu.Lock()
	if e.stateStore == nil {
		if db == nil {
			e.mu.Unlock()
			return
		}
		e.stateStore = newPGStateStore(db, e.logger)
	}
	store := e.stateStore
	metricsRecorder := e.metrics
	logger := e.logger
	e.mu.Unlock()

	if store == nil {
		return
	}

	if err := store.Ensure(ctx); err != nil {
		logger.Warn("failed to prepare tn_vacuum state store", "error", err)
		return
	}

	state, ok, err := store.Load(ctx)
	if err != nil {
		logger.Warn("failed to load tn_vacuum state", "error", err)
		return
	}
	if !ok {
		return
	}

	e.mu.Lock()
	e.state = state
	e.mu.Unlock()

	if metricsRecorder != nil {
		metricsRecorder.RecordLastRunHeight(ctx, state.LastRunHeight)
	}
}

func (e *Extension) configure(ctx context.Context, cfg Config) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.mechanism != nil {
		_ = e.mechanism.Close(ctx)
		e.mechanism = nil
	}

	e.config = cfg

	if !cfg.Enabled {
		return nil
	}

	mech := newMechanism()
	deps := MechanismDeps{Logger: e.logger, DB: dbConnFromService(e.service)}
	if err := mech.Prepare(ctx, deps); err != nil {
		return err
	}

	e.mechanism = mech
	e.runner = &Runner{logger: e.logger}
	return nil
}

func (e *Extension) maybeRun(ctx context.Context, blockHeight int64) {
	if blockHeight <= 0 {
		return
	}

	e.mu.RLock()
	cfg := e.config
	mech := e.mechanism
	runner := e.runner
	state := e.state
	logger := e.logger
	svc := e.service
	metricsRecorder := e.metrics
	nowFn := e.now
	e.mu.RUnlock()

	if !cfg.Enabled || mech == nil || runner == nil {
		return
	}

	if state.LastRunHeight != 0 {
		if blockHeight <= state.LastRunHeight {
			return
		}
		if blockHeight-state.LastRunHeight < cfg.BlockInterval {
			if metricsRecorder != nil {
				metricsRecorder.RecordVacuumSkipped(ctx, "block_interval_not_met")
			}
			return
		}
	}

	reason := fmt.Sprintf("block_interval:%d", blockHeight)
	err := runner.Execute(ctx, RunnerArgs{
		Mechanism: mech,
		Logger:    logger,
		Reason:    reason,
		DB:        dbConnFromService(svc),
		Metrics:   metricsRecorder,
	})
	if err != nil {
		logger.Warn("vacuum run failed", "error", err, "height", blockHeight, "reason", reason)
		return
	}

	newState := runState{LastRunHeight: blockHeight}
	if nowFn != nil {
		newState.LastRunAt = nowFn().UTC()
	}

	var store stateStore
	e.mu.Lock()
	if blockHeight > e.state.LastRunHeight {
		e.state = newState
		store = e.stateStore
	}
	e.mu.Unlock()

	if store != nil {
		if err := store.Save(ctx, newState); err != nil {
			logger.Warn("failed to persist tn_vacuum state", "error", err)
		}
	}
	if metricsRecorder != nil {
		metricsRecorder.RecordLastRunHeight(ctx, blockHeight)
	}
}

func (e *Extension) Close(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mechanism != nil {
		_ = e.mechanism.Close(ctx)
		e.mechanism = nil
	}
	e.runner = nil
}

func dbConnFromService(service *common.Service) DBConnConfig {
	if service == nil || service.LocalConfig == nil {
		return DBConnConfig{}
	}
	db := service.LocalConfig.DB
	return DBConnConfig{
		Host:     db.Host,
		Port:     db.Port,
		User:     db.User,
		Password: db.Pass,
		Database: db.DBName,
	}
}
