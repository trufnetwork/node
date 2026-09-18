package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"

	"github.com/trufnetwork/node/extensions/tn_settlement/internal"
)

// =============================================================================
// Mock implementations for testing
// =============================================================================

type mockPublicKey struct {
	data []byte
}

func (m *mockPublicKey) Bytes() []byte {
	return m.data
}

func (m *mockPublicKey) Equals(other crypto.Key) bool {
	otherPub, ok := other.(*mockPublicKey)
	if !ok {
		return false
	}
	if len(m.data) != len(otherPub.data) {
		return false
	}
	for i := range m.data {
		if m.data[i] != otherPub.data[i] {
			return false
		}
	}
	return true
}

func (m *mockPublicKey) Verify(data []byte, sig []byte) (bool, error) {
	return true, nil
}

func (m *mockPublicKey) Type() crypto.KeyType {
	return crypto.KeyTypeSecp256k1
}

type mockTxBroadcaster struct{}

func (m *mockTxBroadcaster) BroadcastTx(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error) {
	return ktypes.Hash{1, 2, 3}, &ktypes.TxResult{
		Code: uint32(ktypes.CodeOk),
		Log:  "settled",
	}, nil
}

type mockSigner struct{}

func (m *mockSigner) Sign(msg []byte) (*auth.Signature, error) {
	return &auth.Signature{
		Data: msg,
		Type: "secp256k1_ep",
	}, nil
}

func (m *mockSigner) Identity() []byte {
	return []byte("mock-signer")
}

func (m *mockSigner) AuthType() string {
	return "secp256k1_ep"
}

func (m *mockSigner) CompactID() []byte {
	return []byte("compact-id")
}

func (m *mockSigner) PubKey() crypto.PublicKey {
	// Return a mock public key
	mockPubKeyData := make([]byte, 32)
	for i := range mockPubKeyData {
		mockPubKeyData[i] = byte(i)
	}
	return &mockPublicKey{data: mockPubKeyData}
}

// mockEngineOps implements EngineOps interface for testing.
// It signals when methods are called to verify job execution.
// It also checks context cancellation to ensure the scheduler uses its own context.
//
// The zero value preserves the lifecycle tests' behavior (no markets, no
// attestation, settle succeeds as a no-op). The optional fields drive the
// settlement-flow tests: markets to return, whether a signed attestation exists,
// the error BroadcastSettleMarketWithRetry returns, and a record of the query_ids
// it was called with (to assert a quarantined market is not re-broadcast).
type mockEngineOps struct {
	t                      *testing.T
	onFindUnsettledMarkets func()

	markets           []*internal.UnsettledMarket
	attestationExists bool
	settleErr         error
	settleCalls       []int

	// attestedFor answers AttestationExists per market id when set, so a cycle
	// can mix markets that still need capturing with markets ready to settle.
	attestedFor map[int]bool
	// requestErrFor fails the capture of one market id.
	requestErrFor map[int]error

	beginCycleCalls int
	requestCalls    []int
	// calls records every broadcast the cycle made, in order, as "capture:<id>"
	// or "settle:<id>". It is what proves no settlement landed between two
	// captures.
	calls []string
}

func (m *mockEngineOps) FindUnsettledMarkets(ctx context.Context, limit int) ([]*internal.UnsettledMarket, error) {
	// Check if context is canceled - this would indicate the bug regressed
	// (scheduler passing parent context instead of its own internal context)
	if ctx.Err() != nil {
		if m.t != nil {
			m.t.Fatalf("FindUnsettledMarkets called with canceled context: %v - scheduler should use its own internal context", ctx.Err())
		}
		return nil, ctx.Err()
	}
	if m.onFindUnsettledMarkets != nil {
		m.onFindUnsettledMarkets()
	}
	// Mirror the real query's LIMIT: oldest-first, truncated to `limit`.
	markets := m.markets
	if limit >= 0 && len(markets) > limit {
		markets = markets[:limit]
	}
	return markets, nil
}

func (m *mockEngineOps) AttestationExists(ctx context.Context, marketHash []byte) (bool, error) {
	if m.attestedFor != nil && len(marketHash) > 0 {
		return m.attestedFor[int(marketHash[0])], nil
	}
	return m.attestationExists, nil
}

func (m *mockEngineOps) BeginCycle() {
	m.beginCycleCalls++
}

func (m *mockEngineOps) RequestAttestationForMarket(ctx context.Context, chainID string, signer auth.Signer, broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error), market *internal.UnsettledMarket) error {
	if err, ok := m.requestErrFor[market.ID]; ok {
		return err
	}
	m.requestCalls = append(m.requestCalls, market.ID)
	m.calls = append(m.calls, fmt.Sprintf("capture:%d", market.ID))
	return nil
}

func (m *mockEngineOps) BroadcastSettleMarketWithRetry(ctx context.Context, chainID string, signer auth.Signer, broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error), queryID int, maxRetries int) error {
	m.settleCalls = append(m.settleCalls, queryID)
	m.calls = append(m.calls, fmt.Sprintf("settle:%d", queryID))
	return m.settleErr
}

// =============================================================================
// Test: Permanent settlement failure quarantines the market
// =============================================================================

// TestRunSettlementCycle_PermanentFailureQuarantinesMarket asserts the fix for
// the infinite-retry bug: a market whose settlement fails permanently
// (ErrPermanentSettleFailure, e.g. a malformed immutable attestation) is
// attempted once, quarantined, then SKIPPED on the next cycle instead of being
// re-broadcast every poll (which burned nonces and spammed failed txs to blocks).
func TestRunSettlementCycle_PermanentFailureQuarantinesMarket(t *testing.T) {
	broadcaster := &mockTxBroadcaster{}
	signer := &mockSigner{}

	mockOps := &mockEngineOps{
		markets:           []*internal.UnsettledMarket{{ID: 368, Hash: []byte{0xab}, SettleTime: 1}},
		attestationExists: true,
		settleErr: fmt.Errorf("%w: transaction failed with code 65535: binary action result must be 32 bytes (abi-encoded bool), got 128",
			internal.ErrPermanentSettleFailure),
	}

	s := NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:          &common.Service{Logger: log.New()},
		Logger:           log.New(),
		EngineOps:        mockOps,
		Tx:               broadcaster,
		Signer:           signer,
		MaxMarketsPerRun: 10,
		RetryAttempts:    3,
	})

	ctx := context.Background()

	// Cycle 1: 368 fails permanently — attempted exactly once, then quarantined.
	settled, failed, skipped, err := s.runSettlementCycle(ctx, mockOps, broadcaster, signer, "test-chain", 10, 3)
	if err != nil {
		t.Fatalf("cycle 1 unexpected error: %v", err)
	}
	if settled != 0 || failed != 1 || skipped != 0 {
		t.Fatalf("cycle 1 counts = settled=%d failed=%d skipped=%d; want 0/1/0", settled, failed, skipped)
	}
	if len(mockOps.settleCalls) != 1 || mockOps.settleCalls[0] != 368 {
		t.Fatalf("cycle 1 expected exactly one settle attempt for 368, got %v", mockOps.settleCalls)
	}
	if !s.isQuarantined(368) {
		t.Fatal("market 368 should be quarantined after a permanent failure")
	}

	// Cycle 2: 368 is quarantined — skipped, and NOT re-broadcast.
	settled, failed, skipped, err = s.runSettlementCycle(ctx, mockOps, broadcaster, signer, "test-chain", 10, 3)
	if err != nil {
		t.Fatalf("cycle 2 unexpected error: %v", err)
	}
	if settled != 0 || failed != 0 || skipped != 1 {
		t.Fatalf("cycle 2 counts = settled=%d failed=%d skipped=%d; want 0/0/1", settled, failed, skipped)
	}
	if len(mockOps.settleCalls) != 1 {
		t.Fatalf("cycle 2 must not re-broadcast a quarantined market; settleCalls=%v", mockOps.settleCalls)
	}
}

// TestRunSettlementCycle_SuccessClearsQuarantine asserts a market that later
// settles (e.g. after an operator re-attests) has its quarantine cleared.
func TestRunSettlementCycle_SuccessClearsQuarantine(t *testing.T) {
	broadcaster := &mockTxBroadcaster{}
	signer := &mockSigner{}

	mockOps := &mockEngineOps{
		markets:           []*internal.UnsettledMarket{{ID: 42, Hash: []byte{0xcd}, SettleTime: 1}},
		attestationExists: true,
	}

	s := NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:          &common.Service{Logger: log.New()},
		Logger:           log.New(),
		EngineOps:        mockOps,
		Tx:               broadcaster,
		Signer:           signer,
		MaxMarketsPerRun: 10,
		RetryAttempts:    3,
	})

	// Pre-quarantine the market, then let it settle successfully after the
	// re-probe deadline by clearing the cooldown (simulate cooldown elapsed).
	s.quarantineMarket(42)
	s.quarantineMu.Lock()
	s.quarantine[42] = time.Now().Add(-time.Minute) // deadline in the past → re-probe allowed
	s.quarantineMu.Unlock()

	settled, failed, skipped, err := s.runSettlementCycle(context.Background(), mockOps, broadcaster, signer, "test-chain", 10, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if settled != 1 || failed != 0 || skipped != 0 {
		t.Fatalf("counts = settled=%d failed=%d skipped=%d; want 1/0/0", settled, failed, skipped)
	}
	if s.isQuarantined(42) {
		t.Fatal("a successful settlement must clear the quarantine entry")
	}
}

// TestRunSettlementCycle_QuarantinedMarketDoesNotStarveNewer asserts a quarantined
// older market does not consume the maxMarkets budget and starve a newer eligible
// market. With maxMarkets=1 and the (oldest-first) page truncating to the limit, the
// naive fetch would return only the quarantined market and never reach the newer one;
// over-fetching by the quarantine count keeps the newer market settleable.
func TestRunSettlementCycle_QuarantinedMarketDoesNotStarveNewer(t *testing.T) {
	broadcaster := &mockTxBroadcaster{}
	signer := &mockSigner{}

	older := &internal.UnsettledMarket{ID: 1, Hash: []byte{0x01}, SettleTime: 100}
	newer := &internal.UnsettledMarket{ID: 2, Hash: []byte{0x02}, SettleTime: 200}

	mockOps := &mockEngineOps{
		markets:           []*internal.UnsettledMarket{older, newer}, // oldest-first
		attestationExists: true,
	}

	s := NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:          &common.Service{Logger: log.New()},
		Logger:           log.New(),
		EngineOps:        mockOps,
		Tx:               broadcaster,
		Signer:           signer,
		MaxMarketsPerRun: 1,
		RetryAttempts:    3,
	})

	// The older market was quarantined by a prior permanent failure.
	s.quarantineMarket(older.ID)

	settled, failed, skipped, err := s.runSettlementCycle(context.Background(), mockOps, broadcaster, signer, "test-chain", 1, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if settled != 1 || failed != 0 || skipped != 1 {
		t.Fatalf("counts = settled=%d failed=%d skipped=%d; want 1/0/1 (older skipped, newer settled)", settled, failed, skipped)
	}
	// Only the newer market may be settled; the quarantined older one must be skipped.
	if len(mockOps.settleCalls) != 1 || mockOps.settleCalls[0] != newer.ID {
		t.Fatalf("expected exactly the newer market (%d) settled, got settleCalls=%v", newer.ID, mockOps.settleCalls)
	}
}

// =============================================================================
// Test: Scheduler Start/Stop
// =============================================================================

func TestSchedulerStartStop(t *testing.T) {
	service := &common.Service{
		Logger: log.New(),
	}

	broadcaster := &mockTxBroadcaster{}
	signer := &mockSigner{}

	// Create scheduler with nil engineOps (we won't execute jobs, just test lifecycle)
	scheduler := NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:          service,
		Logger:           log.New(),
		EngineOps:        nil, // nil is fine for lifecycle tests
		Tx:               broadcaster,
		Signer:           signer,
		MaxMarketsPerRun: 10,
		RetryAttempts:    3,
	})

	// Test start
	err := scheduler.Start(context.Background(), "* * * * * *") // Every second
	if err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}

	// Wait a bit to ensure scheduler is running
	time.Sleep(100 * time.Millisecond)

	// Test stop
	err = scheduler.Stop()
	if err != nil {
		t.Fatalf("Failed to stop scheduler: %v", err)
	}

	// Verify scheduler stopped by checking it can be restarted
	err = scheduler.Start(context.Background(), "* * * * * *")
	if err != nil {
		t.Fatalf("Failed to restart scheduler after stop: %v", err)
	}

	// Clean up
	err = scheduler.Stop()
	if err != nil {
		t.Fatalf("Failed to stop scheduler during cleanup: %v", err)
	}
}

// =============================================================================
// Test: Scheduler Context Cancellation
// =============================================================================

func TestSchedulerContextCancellation(t *testing.T) {
	service := &common.Service{
		Logger: log.New(),
	}

	broadcaster := &mockTxBroadcaster{}
	signer := &mockSigner{}

	scheduler := NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:          service,
		Logger:           log.New(),
		EngineOps:        nil,
		Tx:               broadcaster,
		Signer:           signer,
		MaxMarketsPerRun: 10,
		RetryAttempts:    3,
	})

	// Create cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Start scheduler with cancellable context
	err := scheduler.Start(ctx, "* * * * * *") // Every second
	if err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}

	// Wait a bit
	time.Sleep(500 * time.Millisecond)

	// Cancel context
	cancel()

	// Wait a bit for scheduler to process cancellation
	time.Sleep(100 * time.Millisecond)

	// Verify scheduler stopped gracefully
	err = scheduler.Stop()
	if err != nil {
		t.Logf("Stop returned error (expected after context cancel): %v", err)
	}

	t.Log("Scheduler handled context cancellation gracefully")
}

// =============================================================================
// Test: Scheduler Multiple Start Calls (Idempotency)
// =============================================================================

func TestSchedulerMultipleStarts(t *testing.T) {
	service := &common.Service{
		Logger: log.New(),
	}

	broadcaster := &mockTxBroadcaster{}
	signer := &mockSigner{}

	scheduler := NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:          service,
		Logger:           log.New(),
		EngineOps:        nil,
		Tx:               broadcaster,
		Signer:           signer,
		MaxMarketsPerRun: 10,
		RetryAttempts:    3,
	})

	// Start scheduler
	err := scheduler.Start(context.Background(), "* * * * * *")
	if err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}

	// Try to start again (should clear previous and restart)
	err = scheduler.Start(context.Background(), "*/2 * * * * *")
	if err != nil {
		t.Fatalf("Failed to restart scheduler with new schedule: %v", err)
	}

	// Stop should work without issues
	err = scheduler.Stop()
	if err != nil {
		t.Fatalf("Failed to stop scheduler: %v", err)
	}

	t.Log("Scheduler handled multiple start calls correctly")
}

// =============================================================================
// Test: Scheduler Schedule Validation
// =============================================================================

func TestSchedulerScheduleValidation(t *testing.T) {
	service := &common.Service{
		Logger: log.New(),
	}

	broadcaster := &mockTxBroadcaster{}
	signer := &mockSigner{}

	scheduler := NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:          service,
		Logger:           log.New(),
		EngineOps:        nil,
		Tx:               broadcaster,
		Signer:           signer,
		MaxMarketsPerRun: 10,
		RetryAttempts:    3,
	})

	// Test valid cron schedule (every 5 minutes)
	err := scheduler.Start(context.Background(), "*/5 * * * *")
	if err != nil {
		t.Errorf("Valid schedule '*/5 * * * *' should not error: %v", err)
	}
	scheduler.Stop()

	// Test valid cron schedule with seconds
	err = scheduler.Start(context.Background(), "*/30 * * * * *")
	if err != nil {
		t.Errorf("Valid schedule with seconds '*/30 * * * * *' should not error: %v", err)
	}
	scheduler.Stop()

	t.Log("Scheduler accepts valid cron schedules")
}

// =============================================================================
// Test: Scheduler Graceful Shutdown
// =============================================================================

func TestSchedulerGracefulShutdown(t *testing.T) {
	service := &common.Service{
		Logger: log.New(),
	}

	broadcaster := &mockTxBroadcaster{}
	signer := &mockSigner{}

	scheduler := NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:          service,
		Logger:           log.New(),
		EngineOps:        nil,
		Tx:               broadcaster,
		Signer:           signer,
		MaxMarketsPerRun: 10,
		RetryAttempts:    3,
	})

	// Start scheduler
	err := scheduler.Start(context.Background(), "* * * * * *")
	if err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}

	// Let it run for a bit
	time.Sleep(300 * time.Millisecond)

	// Stop scheduler (should complete without blocking)
	stopChan := make(chan error, 1)
	go func() {
		stopChan <- scheduler.Stop()
	}()

	// Verify stop completes within reasonable time
	select {
	case err := <-stopChan:
		if err != nil {
			t.Logf("Stop returned error: %v", err)
		}
		t.Log("Scheduler stopped gracefully")
	case <-time.After(2 * time.Second):
		t.Error("Scheduler Stop() blocked for too long (> 2 seconds)")
	}
}

// =============================================================================
// Test: Scheduler SetSigner (Thread Safety)
// =============================================================================

func TestSchedulerSetSigner(t *testing.T) {
	service := &common.Service{
		Logger: log.New(),
	}

	broadcaster := &mockTxBroadcaster{}
	signer1 := &mockSigner{}
	signer2 := &mockSigner{}

	scheduler := NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:          service,
		Logger:           log.New(),
		EngineOps:        nil,
		Tx:               broadcaster,
		Signer:           signer1,
		MaxMarketsPerRun: 10,
		RetryAttempts:    3,
	})

	// Start scheduler
	err := scheduler.Start(context.Background(), "* * * * * *")
	if err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}

	// Change signer while running (should be thread-safe)
	scheduler.SetSigner(signer2)

	// Should still work after signer change
	time.Sleep(100 * time.Millisecond)

	// Stop gracefully
	err = scheduler.Stop()
	if err != nil {
		t.Fatalf("Failed to stop scheduler after signer change: %v", err)
	}

	t.Log("Scheduler handled signer change correctly (thread-safe)")
}

// =============================================================================
// Test: Scheduler Job Runs After Parent Context Canceled
// This tests the bug fix where block context was passed to Start() and got
// canceled before cron jobs could run.
// =============================================================================

func TestSchedulerJobRunsAfterParentContextCanceled(t *testing.T) {
	// Create service with GenesisConfig to pass prerequisites check
	service := &common.Service{
		Logger: log.New(),
		GenesisConfig: &config.GenesisConfig{
			ChainID: "test-chain",
		},
	}

	broadcaster := &mockTxBroadcaster{}
	signer := &mockSigner{}

	// Track if job executed - use buffered channel to avoid blocking
	jobExecuted := make(chan struct{}, 1)

	// Create mock EngineOps that signals when FindUnsettledMarkets is called
	// The mock also checks that ctx is not canceled - if it is, the test fails
	// because the scheduler should use its own internal context, not the parent context
	mockOps := &mockEngineOps{
		t: t,
		onFindUnsettledMarkets: func() {
			// Signal that the job ran (non-blocking send)
			select {
			case jobExecuted <- struct{}{}:
			default:
			}
		},
	}

	scheduler := NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:          service,
		Logger:           log.New(),
		EngineOps:        mockOps, // Mock that signals when FindUnsettledMarkets is called
		Tx:               broadcaster,
		Signer:           signer,
		MaxMarketsPerRun: 10,
		RetryAttempts:    3,
	})

	// Simulate the bug: create a context that will be canceled immediately
	// (like a block processing context)
	blockCtx, cancelBlock := context.WithCancel(context.Background())

	// Start scheduler - with our fix, it should use its own internal context
	err := scheduler.Start(blockCtx, "* * * * * *") // Every second
	if err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}

	// Immediately cancel the "block" context (simulating block processing completion)
	cancelBlock()

	// Wait for the job to execute with a timeout
	// The old bug would cause "context canceled" errors and the job would never run
	select {
	case <-jobExecuted:
		t.Log("Scheduler job executed successfully even after parent context was canceled - bug fix verified!")
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout: scheduler job never executed after parent context was canceled - the bug fix may have regressed")
	}

	// Clean up
	err = scheduler.Stop()
	if err != nil {
		t.Fatalf("Failed to stop scheduler: %v", err)
	}
}

// =============================================================================
// Test: Scheduler Parameter Validation
// =============================================================================

func TestSchedulerParameterDefaults(t *testing.T) {
	service := &common.Service{
		Logger: log.New(),
	}

	broadcaster := &mockTxBroadcaster{}
	signer := &mockSigner{}

	// Create scheduler with zero/invalid parameters
	scheduler := NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:          service,
		Logger:           log.New(),
		EngineOps:        nil,
		Tx:               broadcaster,
		Signer:           signer,
		MaxMarketsPerRun: 0,  // Should use default
		RetryAttempts:    -1, // Should use default
	})

	// Scheduler should still work (using defaults)
	err := scheduler.Start(context.Background(), "* * * * * *")
	if err != nil {
		t.Fatalf("Failed to start scheduler with default parameters: %v", err)
	}

	err = scheduler.Stop()
	if err != nil {
		t.Fatalf("Failed to stop scheduler: %v", err)
	}

	t.Log("Scheduler applied default parameters correctly")
}

// =============================================================================
// Test: a cycle captures every market before it settles any
// =============================================================================

// mixedCycleScheduler builds a scheduler over a due set where some markets still
// need their query captured and others already have a signed attestation.
func mixedCycleScheduler(t *testing.T, ops *mockEngineOps) *SettlementScheduler {
	t.Helper()
	return NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:   &common.Service{GenesisConfig: &config.GenesisConfig{ChainID: "test-chain"}},
		Logger:    log.DiscardLogger,
		EngineOps: ops,
		Signer:    &mockSigner{},
		Tx:        &mockTxBroadcaster{},
	})
}

// T5. A market is one band of a ladder, and each capture freezes the query
// result at whatever block its transaction lands in. A settle_market waits for
// its commit, so letting one run between two captures puts them in different
// blocks — and a price landing in between makes two bands of one ladder resolve
// against two different values, signed and immutable.
//
// Every capture must therefore be issued before the first settlement, whatever
// order the due set arrives in.
func TestRunSettlementCycle_CapturesEverythingBeforeSettlingAnything(t *testing.T) {
	// Interleaved on purpose: settleable, ladder book, settleable, ladder book.
	ops := &mockEngineOps{
		markets: []*internal.UnsettledMarket{
			{ID: 1, Hash: []byte{1}, SettleTime: 1},
			{ID: 2, Hash: []byte{2}, SettleTime: 1},
			{ID: 3, Hash: []byte{3}, SettleTime: 1},
			{ID: 4, Hash: []byte{4}, SettleTime: 1},
		},
		attestedFor: map[int]bool{1: true, 3: true},
	}
	s := mixedCycleScheduler(t, ops)

	require.NoError(t, s.RunOnce(context.Background()))

	require.Equal(t, []string{
		"capture:2", "capture:4", "settle:1", "settle:3",
	}, ops.calls, "no settlement may land between two captures")
	require.Equal(t, 1, ops.beginCycleCalls,
		"the nonce counter is re-seeded once per cycle, before anything is broadcast")
}

// A settle-only cycle re-seeds too. A counter left over from the cycle that
// captured is stale the moment tn_digest or tn_attestation — which sign with the
// same node key — land a transaction between two polls.
func TestRunSettlementCycle_SettleOnlyCycleStillReseedsTheNonce(t *testing.T) {
	ops := &mockEngineOps{
		markets:     []*internal.UnsettledMarket{{ID: 1, Hash: []byte{1}, SettleTime: 1}},
		attestedFor: map[int]bool{1: true},
	}
	s := mixedCycleScheduler(t, ops)

	require.NoError(t, s.RunOnce(context.Background()))

	require.Equal(t, []string{"settle:1"}, ops.calls)
	require.Equal(t, 1, ops.beginCycleCalls)
}

// A failed capture ends the pass. Carrying on would broadcast the rest of a
// ladder against a nonce sequence that is no longer known to be good, and
// re-requesting the failed book on the next poll would capture it in a later
// block than its siblings — the split this ordering exists to prevent.
func TestRunSettlementCycle_AFailedCaptureAbandonsTheRest(t *testing.T) {
	ops := &mockEngineOps{
		markets: []*internal.UnsettledMarket{
			{ID: 1, Hash: []byte{1}, SettleTime: 1},
			{ID: 2, Hash: []byte{2}, SettleTime: 1},
			{ID: 3, Hash: []byte{3}, SettleTime: 1},
		},
		attestedFor:   map[int]bool{},
		requestErrFor: map[int]error{2: fmt.Errorf("invalid nonce")},
	}
	s := mixedCycleScheduler(t, ops)

	require.NoError(t, s.RunOnce(context.Background()))

	require.Equal(t, []int{1}, ops.requestCalls,
		"market 3 must not be captured after market 2 failed")
}
