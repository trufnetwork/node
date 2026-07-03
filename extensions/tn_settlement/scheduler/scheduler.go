package scheduler

import (
	"context"
	"errors"
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

	// quarantine tracks query_ids whose settlement permanently failed, each
	// mapped to the time after which the scheduler may re-probe it once. Such a
	// market is skipped until then so a deterministically-failing settle_market
	// tx is not re-broadcast every poll (which burns nonces and commits failed
	// txs to blocks). In-memory and leader-local: on restart/leadership change it
	// re-probes once and re-quarantines if still permanent. Protected by quarantineMu.
	quarantine   map[int]time.Time
	quarantineMu sync.Mutex
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
		quarantine:       make(map[int]time.Time),
	}
}

// isQuarantined reports whether a market is currently quarantined after a
// permanent settlement failure. Once past its re-probe deadline it returns false
// so exactly one attempt is allowed through — the entry is kept until that
// attempt either succeeds (clearQuarantine) or fails permanently again
// (quarantineMarket re-arms the cooldown).
func (s *SettlementScheduler) isQuarantined(queryID int) bool {
	s.quarantineMu.Lock()
	defer s.quarantineMu.Unlock()
	reprobeAt, ok := s.quarantine[queryID]
	if !ok {
		return false
	}
	return time.Now().Before(reprobeAt)
}

// quarantineMarket flags a market as permanently failing and (re)arms its
// re-probe cooldown.
func (s *SettlementScheduler) quarantineMarket(queryID int) {
	s.quarantineMu.Lock()
	defer s.quarantineMu.Unlock()
	s.quarantine[queryID] = time.Now().Add(PermanentFailureReprobeCooldown)
}

// clearQuarantine removes a market's quarantine entry (called when it settles).
func (s *SettlementScheduler) clearQuarantine(queryID int) {
	s.quarantineMu.Lock()
	defer s.quarantineMu.Unlock()
	delete(s.quarantine, queryID)
}

// quarantineCount returns the number of quarantine entries. It bounds how many
// quarantined rows a settlement query might return, so runSettlementCycle can
// over-fetch by this amount and still process up to maxMarkets non-quarantined
// markets.
func (s *SettlementScheduler) quarantineCount() int {
	s.quarantineMu.Lock()
	defer s.quarantineMu.Unlock()
	return len(s.quarantine)
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

		settled, failed, skipped, err := s.runSettlementCycle(
			jobCtx, engineOps, broadcaster, signer, chainID, maxMarkets, retries)
		if err != nil {
			// Query error and cancellation are already logged inside the cycle.
			return
		}

		// Only emit the completion summary when markets were actually processed;
		// an idle poll already logged a Debug line and would otherwise spam an
		// Info line every poll interval.
		if settled+failed+skipped > 0 {
			s.logger.Info("settlement job completed",
				"total_markets", settled+failed+skipped,
				"settled", settled,
				"failed", failed,
				"skipped", skipped)
		}
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

// runSettlementCycle performs one settlement pass: find unsettled markets, and
// for each ensure a signed attestation exists then broadcast settle_market.
// Markets whose settlement PERMANENTLY fails (ErrPermanentSettleFailure — e.g. an
// immutable, malformed attestation) are quarantined and skipped on later cycles
// so the scheduler stops re-broadcasting a tx that reverts identically forever
// (which burns nonces and commits failed txs to blocks). Returns per-cycle
// counts; err is non-nil only for a query failure or context cancellation.
func (s *SettlementScheduler) runSettlementCycle(
	ctx context.Context,
	engineOps EngineOps,
	broadcaster txBroadcaster,
	signer auth.Signer,
	chainID string,
	maxMarkets, retries int,
) (settled, failed, skipped int, err error) {
	// Over-fetch by the number of quarantined markets. FindUnsettledMarkets returns
	// oldest-first, so a quarantined older market would otherwise fill a limited page
	// ahead of a newer, settleable one and starve it. Requesting maxMarkets plus the
	// quarantine count guarantees the page still holds up to maxMarkets non-quarantined
	// markets (len(quarantine) is a safe upper bound on how many rows we may skip).
	markets, err := engineOps.FindUnsettledMarkets(ctx, maxMarkets+s.quarantineCount())
	if err != nil {
		s.logger.Error("failed to query unsettled markets", "error", err)
		return 0, 0, 0, fmt.Errorf("query unsettled markets: %w", err)
	}

	if len(markets) == 0 {
		s.logger.Debug("no unsettled markets found")
		return 0, 0, 0, nil
	}

	s.logger.Info("found unsettled markets", "count", len(markets))

	// processed counts the non-quarantined markets handled this cycle — it is the
	// maxMarkets budget. Quarantine skips do NOT consume it, so a quarantined market
	// cannot crowd out settleable ones.
	processed := 0
	for _, market := range markets {
		// Stop once the budget of non-quarantined markets is met; any remaining
		// fetched rows are only the over-fetch headroom.
		if processed >= maxMarkets {
			break
		}

		// Check for cancellation (e.g. leadership loss / shutdown)
		select {
		case <-ctx.Done():
			s.logger.Info("settlement cycle cancelled",
				"settled", settled,
				"failed", failed,
				"skipped", skipped)
			return settled, failed, skipped, ctx.Err()
		default:
		}

		// Skip markets quarantined by a prior permanent failure until their
		// re-probe deadline. A quarantine skip does not consume the budget.
		if s.isQuarantined(market.ID) {
			s.logger.Warn("skipping market quarantined after a permanent settlement failure; awaiting manual intervention",
				"query_id", market.ID)
			skipped++
			continue
		}

		processed++

		// Check if a signed attestation exists
		hasAttestation, attErr := engineOps.AttestationExists(ctx, market.Hash)
		if attErr != nil {
			s.logger.Warn("failed to check attestation",
				"query_id", market.ID,
				"error", attErr)
			failed++
			continue
		}
		if !hasAttestation {
			// Request attestation for this market
			s.logger.Info("attestation not available, requesting attestation",
				"query_id", market.ID,
				"settle_time", market.SettleTime)

			if reqErr := engineOps.RequestAttestationForMarket(ctx, chainID, signer, broadcaster.BroadcastTx, market); reqErr != nil {
				s.logger.Warn("failed to request attestation",
					"query_id", market.ID,
					"error", reqErr)
				failed++
			} else {
				s.logger.Info("attestation requested successfully",
					"query_id", market.ID)
				skipped++ // Will settle on next run after signing
			}
			continue
		}

		// Broadcast settle_market transaction with retry
		settleErr := engineOps.BroadcastSettleMarketWithRetry(
			ctx,
			chainID,
			signer,
			broadcaster.BroadcastTx,
			market.ID,
			retries,
		)
		switch {
		case settleErr == nil:
			s.logger.Info("market settled successfully", "query_id", market.ID)
			s.clearQuarantine(market.ID)
			settled++
		case errors.Is(settleErr, internal.ErrPermanentSettleFailure):
			// Deterministic failure — will recur identically. Quarantine so it is
			// not re-broadcast every poll, and flag it for manual intervention.
			s.quarantineMarket(market.ID)
			s.logger.Error("market permanently unsettleable; quarantined and flagged for manual intervention",
				"query_id", market.ID,
				"settle_time", market.SettleTime,
				"reprobe_after", PermanentFailureReprobeCooldown,
				"error", settleErr)
			failed++
		default:
			s.logger.Warn("failed to settle market after retries",
				"query_id", market.ID,
				"settle_time", market.SettleTime,
				"error", settleErr)
			failed++
		}
	}

	return settled, failed, skipped, nil
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

	_, _, _, err := s.runSettlementCycle(ctx, engineOps, broadcaster, signer, chainID, maxMarkets, retries)
	return err
}
