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

	// Use scheduler context for job execution to enable cancellation on leadership loss
	jobFunc := func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("panic in digest job", "panic", r, "stack", string(debug.Stack()))
			}
		}()

		// Use the scheduler's context so Stop() cancels the drain loop
		jobCtx := s.ctx

		// Snapshot dependencies under lock to avoid races with setters.
		s.mu.Lock()
		engineOps := s.engineOps
		broadcaster := s.broadcaster
		signer := s.signer
		kwilService := s.kwilService
		s.mu.Unlock()

		if engineOps == nil || broadcaster == nil || signer == nil || kwilService == nil || kwilService.GenesisConfig == nil {
			s.logger.Warn("digest job prerequisites missing; skipping run")
			return
		}
		chainID := kwilService.GenesisConfig.ChainID

		// Implement drain mode: run auto_digest repeatedly until has_more=false
		s.logger.Info("starting digest drain mode",
			"delete_cap", DigestDeleteCap,
			"expected_records", DigestExpectedRecordsPerStream,
			"preserve_days", DigestPreservePastDays,
			"max_runs", DrainMaxRuns)

		runs := 0
		consecutiveFailures := 0
		totalProcessedDays := 0
		totalDeletedRows := 0

		for runs < DrainMaxRuns {
			// Check for cancellation
			select {
			case <-jobCtx.Done():
				s.logger.Info("digest drain canceled", "runs_completed", runs)
				return
			default:
			}

			runs++

			// Use retry-aware broadcast method with fresh nonce refetch on each attempt
			result, err := engineOps.BroadcastAutoDigestWithArgsAndRetry(
				jobCtx,
				chainID,
				signer,
				broadcaster.BroadcastTx,
				DigestDeleteCap,
				DigestExpectedRecordsPerStream,
				DigestPreservePastDays,
				3, // maxRetries = 3 attempts per run
			)

			if err != nil {
				consecutiveFailures++
				s.logger.Warn("auto_digest broadcast failed after retries",
					"run", runs,
					"consecutive_failures", consecutiveFailures,
					"error", err)

				if consecutiveFailures >= DrainMaxConsecutiveFailures {
					s.logger.Error("too many consecutive failures, aborting drain",
						"consecutive_failures", consecutiveFailures,
						"max_allowed", DrainMaxConsecutiveFailures)
					return
				}
			} else {
				consecutiveFailures = 0
				// Update cumulative totals
				totalProcessedDays += result.ProcessedDays
				totalDeletedRows += result.TotalDeletedRows

				s.logger.Info("digest run completed",
					"run", runs,
					"processed_days", result.ProcessedDays,
					"deleted_rows", result.TotalDeletedRows,
					"has_more", result.HasMoreToDelete,
					"cumulative_processed", totalProcessedDays,
					"cumulative_deleted", totalDeletedRows)

				// Check if we're done
				if !result.HasMoreToDelete {
					s.logger.Info("digest drain completed successfully",
						"total_runs", runs,
						"total_processed_days", totalProcessedDays,
						"total_deleted_rows", totalDeletedRows)
					return
				}
			}

			// Sleep between runs, but allow cancellation
			select {
			case <-jobCtx.Done():
				s.logger.Info("digest drain canceled during sleep", "runs_completed", runs)
				return
			case <-time.After(DrainRunDelay):
				// Continue to next run
			}
		}

		s.logger.Info("digest drain reached max runs",
			"max_runs", DrainMaxRuns,
			"runs_completed", runs)
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
	if s.engineOps == nil || s.broadcaster == nil || s.signer == nil || s.kwilService == nil || s.kwilService.GenesisConfig == nil {
		return fmt.Errorf("missing prerequisites to run digest once")
	}
	chainID := s.kwilService.GenesisConfig.ChainID
	return s.engineOps.BuildAndBroadcastAutoDigestTx(ctx, chainID, s.signer, s.broadcaster.BroadcastTx)
}
