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
	"github.com/trufnetwork/node/extensions/tn_settlement/internal"
)

type txBroadcaster interface {
	BroadcastTx(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error)
}

// ProcessingGuard provides atomic processing state management to prevent
// overlapping settlement runs (which cause nonce conflicts).
type ProcessingGuard interface {
	// TryStartProcessing atomically tries to start processing.
	// Returns true if processing was started (no other run in progress).
	// Returns false if another run is already in progress.
	TryStartProcessing() bool
	// SetProcessing sets the processing state (used to clear flag when done).
	SetProcessing(v bool)
}

// EngineOps defines the operations needed by the settlement scheduler.
// This interface allows for mocking in tests.
type EngineOps interface {
	FindUnsettledMarkets(ctx context.Context, limit int) ([]*internal.UnsettledMarket, error)
	AttestationExists(ctx context.Context, marketHash []byte) (bool, error)
	RequestAttestationForMarket(ctx context.Context, chainID string, signer auth.Signer, broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error), market *internal.UnsettledMarket) error
	BroadcastSettleMarketWithRetry(ctx context.Context, chainID string, signer auth.Signer, broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error), queryID int, maxRetries int) error
}

type SettlementScheduler struct {
	kwilService *common.Service
	logger      log.Logger
	engineOps   EngineOps
	cron        *gocron.Scheduler
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex

	broadcaster txBroadcaster
	signer      auth.Signer

	maxMarketsPerRun int
	retryAttempts    int

	// processingGuard prevents overlapping settlement runs (nonce conflicts)
	processingGuard ProcessingGuard
}

type NewSettlementSchedulerParams struct {
	Service          *common.Service
	Logger           log.Logger
	EngineOps        EngineOps
	Signer           auth.Signer
	Tx               txBroadcaster
	MaxMarketsPerRun int
	RetryAttempts    int
	ProcessingGuard  ProcessingGuard // Optional: prevents overlapping runs
}

func NewSettlementScheduler(params NewSettlementSchedulerParams) *SettlementScheduler {
	maxMarkets := params.MaxMarketsPerRun
	if maxMarkets <= 0 {
		maxMarkets = MaxMarketsPerRun
	}
	retries := params.RetryAttempts
	if retries <= 0 {
		retries = MaxRetryAttempts
	}

	return &SettlementScheduler{
		kwilService:      params.Service,
		logger:           params.Logger.New("settlement_scheduler"),
		engineOps:        params.EngineOps,
		cron:             gocron.NewScheduler(time.UTC),
		broadcaster:      params.Tx,
		signer:           params.Signer,
		maxMarketsPerRun: maxMarkets,
		retryAttempts:    retries,
		processingGuard:  params.ProcessingGuard,
	}
}

func (s *SettlementScheduler) SetSigner(sig auth.Signer) {
	s.mu.Lock()
	s.signer = sig
	s.mu.Unlock()
}

// Start registers a single cron job with the provided cron expression
func (s *SettlementScheduler) Start(ctx context.Context, cronExpr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Cancel any previous context to avoid leaks on restarts
	if s.cancel != nil {
		s.cancel()
	}
	// Use Background context instead of the passed-in context to ensure
	// the scheduler's jobs continue running even if the caller's context
	// (e.g., a block processing context) is canceled
	s.ctx, s.cancel = context.WithCancel(context.Background())

	// Clear any existing jobs to avoid duplicates on (re)start
	s.cron.Clear()

	// Use scheduler context for job execution to enable cancellation on leadership loss
	jobFunc := func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("panic in settlement job", "panic", r, "stack", string(debug.Stack()))
			}
		}()

		// Check processing guard to prevent overlapping runs (prevents nonce conflicts)
		// This is needed because scheduler restart (e.g., on config change) can cause
		// the previous job to still be running when a new scheduler starts.
		s.mu.Lock()
		guard := s.processingGuard
		s.mu.Unlock()

		if guard != nil {
			if !guard.TryStartProcessing() {
				s.logger.Warn("skipping settlement job - previous run still in progress")
				return
			}
			// Ensure we clear the processing flag when done
			defer guard.SetProcessing(false)
		}

		// Snapshot dependencies under lock to avoid races with setters
		s.mu.Lock()
		jobCtx := s.ctx
		engineOps := s.engineOps
		broadcaster := s.broadcaster
		signer := s.signer
		kwilService := s.kwilService
		maxMarkets := s.maxMarketsPerRun
		retries := s.retryAttempts
		s.mu.Unlock()

		if engineOps == nil || broadcaster == nil || signer == nil || kwilService == nil || kwilService.GenesisConfig == nil {
			s.logger.Warn("settlement job prerequisites missing; skipping run")
			return
		}
		chainID := kwilService.GenesisConfig.ChainID

		s.logger.Info("starting settlement job",
			"max_markets", maxMarkets,
			"retry_attempts", retries)

		// Query for unsettled markets
		markets, err := engineOps.FindUnsettledMarkets(jobCtx, maxMarkets)
		if err != nil {
			s.logger.Error("failed to query unsettled markets", "error", err)
			return
		}

		if len(markets) == 0 {
			s.logger.Debug("no unsettled markets found")
			return
		}

		s.logger.Info("found unsettled markets", "count", len(markets))

		// Attempt to settle each market
		settled := 0
		failed := 0
		skipped := 0

		for _, market := range markets {
			// Check for cancellation
			select {
			case <-jobCtx.Done():
				s.logger.Info("settlement job cancelled",
					"settled", settled,
					"failed", failed,
					"skipped", skipped)
				return
			default:
			}

			// Check if attestation exists and is signed
			hasAttestation, err := engineOps.AttestationExists(jobCtx, market.Hash)
			if err != nil {
				s.logger.Warn("failed to check attestation",
					"query_id", market.ID,
					"error", err)
				failed++
				continue
			}
			if !hasAttestation {
				// Request attestation for this market
				s.logger.Info("attestation not available, requesting attestation",
					"query_id", market.ID,
					"settle_time", market.SettleTime)

				if err := engineOps.RequestAttestationForMarket(jobCtx, chainID, signer, broadcaster.BroadcastTx, market); err != nil {
					s.logger.Warn("failed to request attestation",
						"query_id", market.ID,
						"error", err)
					failed++
				} else {
					s.logger.Info("attestation requested successfully",
						"query_id", market.ID)
					skipped++ // Will settle on next run after signing
				}
				continue
			}

			// Broadcast settle_market transaction with retry
			err = engineOps.BroadcastSettleMarketWithRetry(
				jobCtx,
				chainID,
				signer,
				broadcaster.BroadcastTx,
				market.ID,
				retries,
			)

			if err != nil {
				s.logger.Warn("failed to settle market after retries",
					"query_id", market.ID,
					"settle_time", market.SettleTime,
					"error", err)
				failed++
			} else {
				s.logger.Info("market settled successfully", "query_id", market.ID)
				settled++
			}
		}

		s.logger.Info("settlement job completed",
			"total_markets", len(markets),
			"settled", settled,
			"failed", failed,
			"skipped", skipped)
	}

	if j, err := s.cron.Cron(cronExpr).Do(jobFunc); err != nil {
		// Fallback for schedules that include seconds
		if j2, err2 := s.cron.CronWithSeconds(cronExpr).Do(jobFunc); err2 != nil {
			return fmt.Errorf("register settlement job: %w", err2)
		} else {
			j2.SingletonMode()
		}
	} else {
		// Prevent overlapping runs
		j.SingletonMode()
	}

	s.cron.StartAsync()
	s.logger.Info("settlement scheduler started", "schedule", cronExpr)
	return nil
}

func (s *SettlementScheduler) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cron.Stop()
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("settlement scheduler stopped")
	return nil
}

// RunOnce executes the settlement job payload once (for tests and manual triggering)
func (s *SettlementScheduler) RunOnce(ctx context.Context) error {
	// Check processing guard to prevent overlapping runs
	s.mu.Lock()
	guard := s.processingGuard
	s.mu.Unlock()

	if guard != nil {
		if !guard.TryStartProcessing() {
			return fmt.Errorf("settlement run already in progress")
		}
		defer guard.SetProcessing(false)
	}

	s.mu.Lock()
	engineOps := s.engineOps
	broadcaster := s.broadcaster
	signer := s.signer
	kwilService := s.kwilService
	maxMarkets := s.maxMarketsPerRun
	retries := s.retryAttempts
	s.mu.Unlock()

	if engineOps == nil || broadcaster == nil || signer == nil || kwilService == nil || kwilService.GenesisConfig == nil {
		return fmt.Errorf("missing prerequisites to run settlement once")
	}
	chainID := kwilService.GenesisConfig.ChainID

	markets, err := engineOps.FindUnsettledMarkets(ctx, maxMarkets)
	if err != nil {
		return fmt.Errorf("query unsettled markets: %w", err)
	}

	for _, market := range markets {
		hasAttestation, err := engineOps.AttestationExists(ctx, market.Hash)
		if err != nil {
			s.logger.Error("failed to check attestation existence",
				"query_id", market.ID,
				"error", err)
			return fmt.Errorf("check attestation for market %d: %w", market.ID, err)
		}
		if !hasAttestation {
			// Request attestation for this market
			if err := engineOps.RequestAttestationForMarket(ctx, chainID, signer, broadcaster.BroadcastTx, market); err != nil {
				s.logger.Warn("failed to request attestation in RunOnce",
					"query_id", market.ID,
					"error", err)
			}
			continue
		}

		err = engineOps.BroadcastSettleMarketWithRetry(
			ctx,
			chainID,
			signer,
			broadcaster.BroadcastTx,
			market.ID,
			retries,
		)
		if err != nil {
			return fmt.Errorf("settle market %d: %w", market.ID, err)
		}
	}

	return nil
}
