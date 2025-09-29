package tn_vacuum

import (
	"context"
	"time"

	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/node/extensions/tn_vacuum/metrics"
)

type Runner struct {
	logger log.Logger
}

type RunnerArgs struct {
	Mechanism Mechanism
	Logger    log.Logger
	Reason    string
	DB        DBConnConfig
	Metrics   metrics.MetricsRecorder
}

func (r *Runner) Execute(ctx context.Context, args RunnerArgs) error {
	if args.Mechanism == nil {
		return nil
	}
	logger := r.logger
	if logger == nil {
		logger = args.Logger
	}

	mechanismName := args.Mechanism.Name()

	if logger != nil {
		logger.Info("vacuum runner executing", "mechanism", mechanismName, "reason", args.Reason)
	}
	if args.Metrics != nil {
		args.Metrics.RecordVacuumStart(ctx, mechanismName)
	}

	report, err := args.Mechanism.Run(ctx, RunRequest{Reason: args.Reason, DB: args.DB})
	if err != nil {
		if logger != nil {
			logger.Warn("vacuum runner failed", "error", err)
		}
		if args.Metrics != nil {
			args.Metrics.RecordVacuumError(ctx, mechanismName, metrics.ClassifyError(err))
		}
		return err
	}

	if logger != nil {
		fields := []any{"mechanism", mechanismName}
		if report != nil {
			fields = append(fields, "duration", report.Duration, "tables", report.TablesProcessed)
		}
		logger.Info("vacuum runner completed", fields...)
	}

	if args.Metrics != nil {
		var duration time.Duration
		tables := 0
		if report != nil {
			duration = report.Duration
			tables = report.TablesProcessed
		}
		args.Metrics.RecordVacuumComplete(ctx, mechanismName, duration, tables)
	}

	return nil
}
