package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

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
	return m.markets, nil
}

func (m *mockEngineOps) AttestationExists(ctx context.Context, marketHash []byte) (bool, error) {
	return m.attestationExists, nil
}

func (m *mockEngineOps) RequestAttestationForMarket(ctx context.Context, chainID string, signer auth.Signer, broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error), market *internal.UnsettledMarket) error {
	return nil
}

func (m *mockEngineOps) BroadcastSettleMarketWithRetry(ctx context.Context, chainID string, signer auth.Signer, broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error), queryID int, maxRetries int) error {
	m.settleCalls = append(m.settleCalls, queryID)
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
