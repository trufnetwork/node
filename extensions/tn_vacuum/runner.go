package tn_vacuum

import (
	"context"

	"github.com/trufnetwork/kwil-db/core/log"
)

type Runner struct {
	logger log.Logger
}

type RunnerArgs struct {
	Mechanism Mechanism
	Trigger   Trigger
	Logger    log.Logger
	Reason    string
}

func (r *Runner) Execute(ctx context.Context, args RunnerArgs) error {
	if args.Mechanism == nil {
		return nil
	}
	logger := r.logger
	if logger == nil {
		logger = args.Logger
	}
	if logger != nil {
		logger.Info("vacuum runner executing", "mechanism", args.Mechanism.Name(), "reason", args.Reason)
	}
	_, err := args.Mechanism.Run(ctx, RunRequest{Reason: args.Reason})
	if err != nil {
		if logger != nil {
			logger.Warn("vacuum runner failed", "error", err)
		}
		return err
	}
	if logger != nil {
		logger.Info("vacuum runner completed", "mechanism", args.Mechanism.Name())
	}
	return nil
}
