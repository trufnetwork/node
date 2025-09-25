package tn_vacuum

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
)

type FireOpts struct {
	Reason string
}

type Trigger interface {
	Configure(ctx context.Context, cfg TriggerConfig, fire func(context.Context, FireOpts) error) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	OnLeaderChange(ctx context.Context, isLeader bool) error
	OnEndBlock(ctx context.Context, block *common.BlockContext) error
	Kind() string
}

func newTrigger(kind string) (Trigger, error) {
	baseLogger := log.New(log.WithLevel(log.LevelInfo))
	switch kind {
	case TriggerDigestCoupled:
		return newDigestTrigger(baseLogger), nil
	case TriggerBlockInterval:
		return newBlockIntervalTrigger(baseLogger), nil
	case TriggerCron:
		return newCronTrigger(baseLogger), nil
	case TriggerManual:
		return newManualTrigger(baseLogger), nil
	default:
		return nil, fmt.Errorf("unsupported trigger %q", kind)
	}
}
