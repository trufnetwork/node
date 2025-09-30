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
	mu            sync.RWMutex
	logger        log.Logger
	service       *common.Service
	config        Config
	mechanism     Mechanism
	runner        *Runner
	state         runState
	stateStore    stateStore
	now           func() time.Time
	metrics       metrics.MetricsRecorder
	runQueue      chan runRequest
	workerCtx     context.Context
	workerCancel  context.CancelFunc
	workerWG      sync.WaitGroup
	runInProgress bool
}

var (
	extInstance *Extension
	once        sync.Once
)

type runRequest struct {
	height       int64
	reason       string
	dbConfig     DBConnConfig
	triggeredAt  time.Time
	PgRepackJobs int
}

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
	if extInstance != nil {
		extInstance.Close(context.Background())
	}
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

// startWorkerLocked spins up the background worker responsible for consuming
// queued run requests. The caller must hold e.mu.
func (e *Extension) startWorkerLocked(parent context.Context) {
	if e.runQueue != nil {
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	e.workerCtx = ctx
	e.workerCancel = cancel
	e.runQueue = make(chan runRequest, 1)
	e.workerWG.Add(1)
	runQueue := e.runQueue
	eworker := e
	go func() {
		defer eworker.workerWG.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case req, ok := <-runQueue:
				if !ok {
					return
				}
				eworker.processRun(ctx, req)
			}
		}
	}()
}

// initializeState prepares the persistence backend and loads the last run
// information from disk. It is safe to call multiple times; the underlying
// operations are idempotent.
func (e *Extension) initializeState(ctx context.Context, db sql.DB) error {
	e.mu.RLock()
	store := e.stateStore
	service := e.service
	logger := e.logger
	metricsRecorder := e.metrics
	e.mu.RUnlock()

	if store == nil {
		cfg := DBConnConfig{}
		if service != nil {
			cfg = dbConnFromService(service)
		}
		if cfg.Database == "" {
			return fmt.Errorf("tn_vacuum state persistence requires database name")
		}

		newStore, err := newPGStateStore(ctx, cfg, logger)
		if err != nil {
			return fmt.Errorf("initialize tn_vacuum state store: %w", err)
		}

		e.mu.Lock()
		if e.stateStore == nil {
			e.stateStore = newStore
			store = newStore
			metricsRecorder = e.metrics
		} else {
			store = e.stateStore
			newStore.Close()
		}
		e.mu.Unlock()
	}

	if store == nil {
		return fmt.Errorf("tn_vacuum state store unavailable")
	}

	if err := store.Ensure(ctx); err != nil {
		return fmt.Errorf("prepare tn_vacuum state store: %w", err)
	}

	state, ok, err := store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load tn_vacuum state: %w", err)
	}
	if !ok {
		return nil
	}

	e.mu.Lock()
	e.state = state
	e.mu.Unlock()

	if metricsRecorder != nil {
		metricsRecorder.RecordLastRunHeight(ctx, state.LastRunHeight)
	}

	return nil
}

// processRun executes a scheduled vacuum request on the worker goroutine. It
// assumes only a single worker is active at a time so no additional locking is
// required outside of bookkeeping updates.
func (e *Extension) processRun(ctx context.Context, req runRequest) {
	e.mu.Lock()
	mech := e.mechanism
	runner := e.runner
	logger := e.logger
	metricsRecorder := e.metrics
	store := e.stateStore
	nowFn := e.now
	config := e.config
	e.runInProgress = true
	e.mu.Unlock()

	if !config.Enabled || mech == nil || runner == nil {
		e.mu.Lock()
		e.runInProgress = false
		e.mu.Unlock()
		return
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	err := runner.Execute(runCtx, RunnerArgs{
		Mechanism:    mech,
		Logger:       logger,
		Reason:       req.reason,
		DB:           req.dbConfig,
		Metrics:      metricsRecorder,
		PgRepackJobs: req.PgRepackJobs,
	})

	if err != nil {
		logger.Warn("vacuum run failed", "error", err, "height", req.height, "reason", req.reason)
		e.mu.Lock()
		e.runInProgress = false
		e.mu.Unlock()
		return
	}

	newState := runState{LastRunHeight: req.height}
	if nowFn != nil {
		newState.LastRunAt = nowFn().UTC()
	}

	if store != nil {
		if err := store.Save(runCtx, newState); err != nil {
			logger.Warn("failed to persist tn_vacuum state", "error", err)
		}
	}
	if metricsRecorder != nil {
		metricsRecorder.RecordLastRunHeight(runCtx, req.height)
	}

	e.mu.Lock()
	if req.height > e.state.LastRunHeight {
		e.state = newState
	}
	e.runInProgress = false
	e.mu.Unlock()
}

// enqueueRun places a run request on the worker queue if no job is already
// pending or in progress. It returns false when the worker is busy so callers
// can record a skip metric.
func (e *Extension) enqueueRun(ctx context.Context, req runRequest) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runQueue == nil {
		e.startWorkerLocked(ctx)
	}
	if e.runInProgress {
		return false
	}
	if len(e.runQueue) > 0 {
		return false
	}
	select {
	case e.runQueue <- req:
		return true
	default:
		return false
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
	if deps.DB.Database == "" {
		return fmt.Errorf("tn_vacuum requires database name in configuration")
	}
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
	triggeredAt := time.Now()
	if nowFn != nil {
		triggeredAt = nowFn()
	}
	req := runRequest{
		height:       blockHeight,
		reason:       reason,
		dbConfig:     dbConnFromService(svc),
		triggeredAt:  triggeredAt,
		PgRepackJobs: cfg.PgRepackJobs,
	}

	if !e.enqueueRun(ctx, req) {
		if metricsRecorder != nil {
			metricsRecorder.RecordVacuumSkipped(ctx, "worker_busy")
		}
		logger.Debug("vacuum run already queued or in progress", "height", blockHeight, "reason", reason)
	}
}

func (e *Extension) Close(ctx context.Context) {
	e.mu.Lock()
	mech := e.mechanism
	e.mechanism = nil
	e.runner = nil
	store := e.stateStore
	e.stateStore = nil
	queue := e.runQueue
	e.runQueue = nil
	cancel := e.workerCancel
	e.workerCancel = nil
	e.workerCtx = nil
	e.mu.Unlock()

	if mech != nil {
		_ = mech.Close(ctx)
	}
	if store != nil {
		store.Close()
	}
	if cancel != nil {
		cancel()
	}
	if queue != nil {
		close(queue)
	}
	e.workerWG.Wait()
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
