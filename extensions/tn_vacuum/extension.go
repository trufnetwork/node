package tn_vacuum

import (
	"context"
	"fmt"
	"sync"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/node/extensions/tn_vacuum/metrics"
)

type Extension struct {
	mu            sync.RWMutex
	logger        log.Logger
	service       *common.Service
	config        Config
	mechanism     Mechanism
	runner        *Runner
	lastRunHeight int64
	metrics       metrics.MetricsRecorder
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
	e.metrics = metrics.NewMetricsRecorder(l)
}

func (e *Extension) setService(s *common.Service) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.service = s
}

func (e *Extension) configure(ctx context.Context, cfg Config) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.mechanism != nil {
		_ = e.mechanism.Close(ctx)
		e.mechanism = nil
	}

	e.config = cfg
	e.lastRunHeight = 0

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
	last := e.lastRunHeight
	logger := e.logger
	svc := e.service
	metricsRecorder := e.metrics
	e.mu.RUnlock()

	if !cfg.Enabled || mech == nil || runner == nil {
		return
	}

	if last != 0 && blockHeight-last < cfg.BlockInterval {
		if metricsRecorder != nil {
			metricsRecorder.RecordVacuumSkipped(ctx, "block_interval_not_met")
		}
		return
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

	e.mu.Lock()
	if blockHeight > e.lastRunHeight {
		e.lastRunHeight = blockHeight
		if metricsRecorder != nil {
			metricsRecorder.RecordLastRunHeight(ctx, blockHeight)
		}
	}
	e.mu.Unlock()
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
