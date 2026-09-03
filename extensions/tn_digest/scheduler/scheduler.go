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

	// The duplicate prune sweep runs on its own cron and its own context, because
	// duplicate_prune_config carries its own enabled flag and its own schedule. A
	// digest config change stops and restarts the digest cron; sharing one would
	// make that cancel a prune drain halfway through, and the other way round.
	pruneCron   *gocron.Scheduler
	pruneCtx    context.Context
	pruneCancel context.CancelFunc

	// drainSlot holds one token and serialises the two drains. They broadcast from
	// the same signer account, so two in flight would take the same nonce and one
	// would lose; and both delete from primitive_events, so keeping them apart also
	// keeps a block from carrying two capped deletes. Both default schedules are
	// six-hourly, so without this they would contend on every firing.
	drainSlot chan struct{}

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
		pruneCron:   gocron.NewScheduler(time.UTC),
		drainSlot:   make(chan struct{}, 1),
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

		// One drain at a time; see drainSlot.
		if !s.acquireDrainSlot(jobCtx) {
			s.logger.Info("digest drain canceled while waiting for the duplicate prune drain")
			return
		}
		defer s.releaseDrainSlot()

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

					// After digest drain, trim order events (best-effort, non-fatal)
					s.trimOrderEvents(jobCtx, chainID, engineOps, signer, broadcaster)
					// Then trim high-volume transaction-event ledger rows (best-effort, non-fatal)
					s.trimTransactionEvents(jobCtx, chainID, engineOps, signer, broadcaster)
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

		// After digest drain, trim order events (best-effort, non-fatal)
		s.trimOrderEvents(jobCtx, chainID, engineOps, signer, broadcaster)
		// Then trim high-volume transaction-event ledger rows (best-effort, non-fatal)
		s.trimTransactionEvents(jobCtx, chainID, engineOps, signer, broadcaster)
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

// Stop stops the digest cron and cancels its drain. It deliberately leaves the
// duplicate prune cron running: the two configurations are independent, and the
// extension restarts the digest cron whenever digest_config changes.
//
// The cancel comes first and neither call happens under the mutex, because
// gocron's Stop waits for a running job to return and this job only returns when
// its context is done. Cancelling afterwards would wait forever, and holding the
// mutex across the wait would block the job in the snapshot it takes on entry.
func (s *DigestScheduler) Stop() error {
	s.mu.Lock()
	cancel := s.cancel
	cron := s.cron
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if cron != nil {
		cron.Stop()
	}
	s.logger.Info("digest scheduler stopped")
	return nil
}

// slotChan returns the drain slot, creating it if the scheduler was built as a
// struct literal rather than through the constructor. Selecting on a nil channel
// blocks forever, so this cannot trust the constructor to have run.
func (s *DigestScheduler) slotChan() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.drainSlot == nil {
		s.drainSlot = make(chan struct{}, 1)
	}
	return s.drainSlot
}

// acquireDrainSlot blocks until the other drain finishes or ctx is done, and
// reports whether it got the slot. Waiting rather than skipping is deliberate:
// digest and prune ship with the same six-hourly default, so a firing that
// skipped on contention would skip every time.
func (s *DigestScheduler) acquireDrainSlot(ctx context.Context) bool {
	slot := s.slotChan()
	select {
	case slot <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// tryAcquireDrainSlot takes the slot only if it is free. It is for the one-off
// entry points, which need the same protection against two transactions taking
// the same nonce but should not disappear into a drain that can hold the slot for
// the better part of two hours.
func (s *DigestScheduler) tryAcquireDrainSlot() bool {
	slot := s.slotChan()
	select {
	case slot <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *DigestScheduler) releaseDrainSlot() {
	s.mu.Lock()
	slot := s.drainSlot
	s.mu.Unlock()
	if slot == nil {
		return
	}
	select {
	case <-slot:
	default:
	}
}

// StartPrune registers the duplicate prune sweep on its own cron expression.
//
// The extension calls this only when duplicate_prune_config.enabled is true, and
// that column ships false. Nothing on a network prunes until an operator sets it
// through a signed exec-sql.
func (s *DigestScheduler) StartPrune(ctx context.Context, cronExpr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pruneCancel != nil {
		s.pruneCancel()
	}
	s.pruneCtx, s.pruneCancel = context.WithCancel(ctx)

	if s.pruneCron == nil {
		s.pruneCron = gocron.NewScheduler(time.UTC)
	}
	s.pruneCron.Clear()

	jobCtx := s.pruneCtx
	jobFunc := func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("panic in duplicate prune job", "panic", r, "stack", string(debug.Stack()))
			}
		}()
		s.runPruneDrain(jobCtx)
	}

	if j, err := s.pruneCron.Cron(cronExpr).Do(jobFunc); err != nil {
		// Fallback for schedules that include seconds.
		if j2, err2 := s.pruneCron.CronWithSeconds(cronExpr).Do(jobFunc); err2 != nil {
			return fmt.Errorf("register duplicate prune job: %w", err)
		} else {
			j2.SingletonMode()
		}
	} else {
		j.SingletonMode()
	}

	s.pruneCron.StartAsync()
	s.logger.Info("duplicate prune scheduler started", "schedule", cronExpr)
	return nil
}

// Running reports whether the digest cron is scheduled, and PruneRunning does the
// same for the duplicate prune sweep. The two crons are independent, so an
// operator or a test that wants to know one is up cannot infer it from the other.
func (s *DigestScheduler) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cron != nil && s.cron.IsRunning()
}

func (s *DigestScheduler) PruneRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pruneCron != nil && s.pruneCron.IsRunning()
}

// StopPrune stops the duplicate prune cron and cancels a drain in flight. Same
// ordering as Stop, and here it matters more: a sweep can sit for minutes waiting
// on the drain slot, and only its context releases it.
func (s *DigestScheduler) StopPrune() error {
	s.mu.Lock()
	cancel := s.pruneCancel
	cron := s.pruneCron
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if cron != nil {
		cron.Stop()
	}
	s.logger.Info("duplicate prune scheduler stopped")
	return nil
}

// runPruneDrain broadcasts auto_prune_duplicates until the sweep finishes a pass
// over every primitive stream, the run budget is spent, or the context is done.
//
// Unlike digest, finishing early is the exception rather than the rule. The sweep
// is cyclic and has_more_to_delete reports "the cursor has not reached the end of
// a pass", so on a network with more streams than one firing can visit the loop
// runs to PruneDrainMaxRuns every time. That is why an empty run gets the short
// delay: after the backlog is gone every run is an empty one.
func (s *DigestScheduler) runPruneDrain(ctx context.Context) {
	s.mu.Lock()
	engineOps := s.engineOps
	broadcaster := s.broadcaster
	signer := s.signer
	kwilService := s.kwilService
	s.mu.Unlock()

	if engineOps == nil || broadcaster == nil || signer == nil || kwilService == nil || kwilService.GenesisConfig == nil {
		s.logger.Warn("duplicate prune job prerequisites missing; skipping run")
		return
	}
	chainID := kwilService.GenesisConfig.ChainID

	// One drain at a time; see drainSlot.
	if !s.acquireDrainSlot(ctx) {
		s.logger.Info("duplicate prune canceled while waiting for the digest drain")
		return
	}
	defer s.releaseDrainSlot()

	s.logger.Info("starting duplicate prune drain",
		"delete_cap", PruneDeleteCap,
		"stream_batch_size", PruneStreamBatchSize,
		"max_runs", PruneDrainMaxRuns)

	runs := 0
	consecutiveFailures := 0
	totalSweptStreams := 0
	totalEventTimes := 0
	totalRows := 0

	for runs < PruneDrainMaxRuns {
		select {
		case <-ctx.Done():
			s.logger.Info("duplicate prune drain canceled", "runs_completed", runs)
			return
		default:
		}

		runs++

		result, err := engineOps.BroadcastAutoPruneDuplicatesWithRetry(
			ctx,
			chainID,
			signer,
			broadcaster.BroadcastTx,
			PruneDeleteCap,
			PruneStreamBatchSize,
			3, // maxRetries = 3 attempts per run
		)

		delay := PruneIdleRunDelay
		if err != nil {
			consecutiveFailures++
			s.logger.Warn("auto_prune_duplicates broadcast failed after retries",
				"run", runs,
				"consecutive_failures", consecutiveFailures,
				"error", err)

			if consecutiveFailures >= PruneDrainMaxConsecutiveFailures {
				s.logger.Error("too many consecutive failures, aborting duplicate prune drain",
					"consecutive_failures", consecutiveFailures,
					"max_allowed", PruneDrainMaxConsecutiveFailures)
				return
			}
			delay = PruneDrainRunDelay
		} else {
			consecutiveFailures = 0
			totalSweptStreams += result.SweptStreams
			totalEventTimes += result.DeletedEventTimes
			totalRows += result.DeletedRows

			s.logger.Info("duplicate prune run completed",
				"run", runs,
				"swept_streams", result.SweptStreams,
				"deleted_event_times", result.DeletedEventTimes,
				"deleted_rows", result.DeletedRows,
				"has_more", result.HasMoreToDelete,
				"cumulative_swept", totalSweptStreams,
				"cumulative_deleted_rows", totalRows)

			if !result.HasMoreToDelete {
				s.logger.Info("duplicate prune pass completed",
					"total_runs", runs,
					"total_swept_streams", totalSweptStreams,
					"total_deleted_event_times", totalEventTimes,
					"total_deleted_rows", totalRows)
				return
			}

			if result.DeletedRows > 0 {
				delay = PruneDrainRunDelay
			}
		}

		select {
		case <-ctx.Done():
			s.logger.Info("duplicate prune drain canceled during sleep", "runs_completed", runs)
			return
		case <-time.After(delay):
		}
	}

	s.logger.Info("duplicate prune drain reached max runs",
		"max_runs", PruneDrainMaxRuns,
		"runs_completed", runs,
		"total_swept_streams", totalSweptStreams,
		"total_deleted_event_times", totalEventTimes,
		"total_deleted_rows", totalRows)
}

// trimOrderEvents runs the trim_order_events action in a drain loop (best-effort).
// Called after digest drain completes. Failures are logged but do not fail the digest job.
func (s *DigestScheduler) trimOrderEvents(
	ctx context.Context,
	chainID string,
	engineOps *internal.EngineOperations,
	signer auth.Signer,
	broadcaster txBroadcaster,
) {
	s.logger.Info("starting order event trim")

	for run := 0; run < TrimOrderEventsMaxRuns; run++ {
		select {
		case <-ctx.Done():
			s.logger.Info("order event trim canceled", "runs_completed", run)
			return
		default:
		}

		result, err := engineOps.BroadcastTrimOrderEventsWithRetry(
			ctx, chainID, signer, broadcaster.BroadcastTx,
			TrimOrderEventsPreserveBlocks,
			TrimOrderEventsDeleteCap,
			3, // maxRetries
		)
		if err != nil {
			s.logger.Warn("trim_order_events failed (non-fatal)", "run", run, "error", err)
			return
		}

		s.logger.Info("trim_order_events run completed",
			"run", run,
			"deleted", result.Deleted,
			"remaining", result.Remaining,
			"has_more", result.HasMore)

		if !result.HasMore {
			return
		}

		// Brief delay between trim runs
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}

	s.logger.Info("order event trim reached max runs", "max_runs", TrimOrderEventsMaxRuns)
}

// trimTransactionEvents runs the trim_transaction_events action in a drain loop
// (best-effort). Called after the order-event trim completes. Gated by
// TrimTxEventsEnabled so pruning stays off until deliberately activated (after
// the Trufscan indexer fallback is live). Failures are logged but do not fail
// the digest job.
func (s *DigestScheduler) trimTransactionEvents(
	ctx context.Context,
	chainID string,
	engineOps *internal.EngineOperations,
	signer auth.Signer,
	broadcaster txBroadcaster,
) {
	if !TrimTxEventsEnabled {
		return
	}

	s.logger.Info("starting transaction event trim")

	for run := 0; run < TrimTxEventsMaxRuns; run++ {
		select {
		case <-ctx.Done():
			s.logger.Info("transaction event trim canceled", "runs_completed", run)
			return
		default:
		}

		result, err := engineOps.BroadcastTrimTransactionEventsWithRetry(
			ctx, chainID, signer, broadcaster.BroadcastTx,
			TrimTxEventsPreserveBlocks,
			TrimTxEventsDeleteCap,
			3, // maxRetries
		)
		if err != nil {
			s.logger.Warn("trim_transaction_events failed (non-fatal)", "run", run, "error", err)
			return
		}

		s.logger.Info("trim_transaction_events run completed",
			"run", run,
			"deleted", result.Deleted,
			"remaining", result.Remaining,
			"has_more", result.HasMore)

		if !result.HasMore {
			return
		}

		// Brief delay between trim runs
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}

	s.logger.Info("transaction event trim reached max runs", "max_runs", TrimTxEventsMaxRuns)
}

// RunOnce executes the digest job payload once (for tests and manual triggering).
func (s *DigestScheduler) RunOnce(ctx context.Context) error {
	if s.engineOps == nil || s.broadcaster == nil || s.signer == nil || s.kwilService == nil || s.kwilService.GenesisConfig == nil {
		return fmt.Errorf("missing prerequisites to run digest once")
	}
	chainID := s.kwilService.GenesisConfig.ChainID
	return s.engineOps.BuildAndBroadcastAutoDigestTx(ctx, chainID, s.signer, s.broadcaster.BroadcastTx)
}

// RunPruneOnce broadcasts a single auto_prune_duplicates batch (for tests and
// manual triggering).
//
// It takes the drain slot, because it broadcasts from the same signer account as
// the scheduled drains and would otherwise read the same nonce. It does not wait
// for it: a caller asking for one batch wants an answer, and a drain holds the
// slot for as long as it runs. Refusing says which of the two happened, where a
// nonce collision would only show up as a retry in the logs.
func (s *DigestScheduler) RunPruneOnce(ctx context.Context) (*internal.PruneTxResult, error) {
	if s.engineOps == nil || s.broadcaster == nil || s.signer == nil || s.kwilService == nil || s.kwilService.GenesisConfig == nil {
		return nil, fmt.Errorf("missing prerequisites to run duplicate prune once")
	}
	if !s.tryAcquireDrainSlot() {
		return nil, fmt.Errorf("a digest or duplicate prune drain is already running; retry once it finishes")
	}
	defer s.releaseDrainSlot()

	chainID := s.kwilService.GenesisConfig.ChainID
	return s.engineOps.BroadcastAutoPruneDuplicatesWithRetry(
		ctx, chainID, s.signer, s.broadcaster.BroadcastTx,
		PruneDeleteCap, PruneStreamBatchSize, 3,
	)
}
