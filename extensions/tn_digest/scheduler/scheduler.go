package scheduler

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/go-co-op/gocron"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/node/extensions/tn_digest/internal"
)

type txBroadcaster interface {
	BroadcastTx(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error)
}

type DigestScheduler struct {
	kwilService *common.Service
	logger      log.Logger
	engineOps   *internal.EngineOperations
	cron        *gocron.Scheduler
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex

	broadcaster txBroadcaster
	signer      auth.Signer
}

type NewDigestSchedulerParams struct {
	Service   *common.Service
	Logger    log.Logger
	EngineOps *internal.EngineOperations
	Signer    auth.Signer
	Tx        txBroadcaster
}

func NewDigestScheduler(params NewDigestSchedulerParams) *DigestScheduler {
	return &DigestScheduler{
		kwilService: params.Service,
		logger:      params.Logger.New("scheduler"),
		engineOps:   params.EngineOps,
		cron:        gocron.NewScheduler(time.UTC),
		broadcaster: params.Tx,
		signer:      params.Signer,
	}
}

func (s *DigestScheduler) SetSigner(sig auth.Signer) {
	s.mu.Lock()
	s.signer = sig
	s.mu.Unlock()
}

// Start registers a single cron job with the provided cron expression.
func (s *DigestScheduler) Start(ctx context.Context, cronExpr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Cancel any previous context to avoid leaks on restarts.
	if s.cancel != nil {
		s.cancel()
	}
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Clear any existing jobs to avoid duplicates on (re)start
	s.cron.Clear()

	// Capture the current context to avoid races if Start is called again.
	jobCtx := s.ctx
	jobFunc := func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("panic in digest job", "panic", r, "stack", string(debug.Stack()))
			}
		}()

		if s.engineOps == nil || s.broadcaster == nil || s.signer == nil || s.kwilService == nil || s.kwilService.GenesisConfig == nil {
			s.logger.Warn("digest job prerequisites missing; skipping run")
			return
		}
		chainID := s.kwilService.GenesisConfig.ChainID
		if err := s.engineOps.BuildAndBroadcastAutoDigestTx(jobCtx, chainID, s.signer, s.broadcaster.BroadcastTx); err != nil {
			s.logger.Warn("auto_digest broadcast failed", "error", err)
			return
		}
		s.logger.Info("auto_digest tx broadcasted")
	}

	if j, err := s.cron.Cron(cronExpr).Do(jobFunc); err != nil {
		// Fallback for schedules that include seconds.
		if j2, err2 := s.cron.CronWithSeconds(cronExpr).Do(jobFunc); err2 != nil {
			return fmt.Errorf("register digest job: %w", err)
		} else {
			j2.SingletonMode()
		}
	} else {
		// Prevent overlapping runs.
		j.SingletonMode()
	}

	s.cron.StartAsync()
	s.logger.Info("digest scheduler started", "schedule", cronExpr)
	return nil
}

func (s *DigestScheduler) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cron.Stop()
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("digest scheduler stopped")
	return nil
}

// RunOnce executes the digest job payload once (for tests and manual triggering).
func (s *DigestScheduler) RunOnce(ctx context.Context) error {
	if s.broadcaster == nil || s.signer == nil || s.kwilService == nil || s.kwilService.GenesisConfig == nil {
		return fmt.Errorf("missing prerequisites to run digest once")
	}
	chainID := s.kwilService.GenesisConfig.ChainID
	return s.engineOps.BuildAndBroadcastAutoDigestTx(ctx, chainID, s.signer, s.broadcaster.BroadcastTx)
}
