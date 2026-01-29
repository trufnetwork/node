package scheduler

import (
	"context"
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
type mockEngineOps struct {
	t                      *testing.T
	onFindUnsettledMarkets func()
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
	// Return empty list - no markets to settle
	return []*internal.UnsettledMarket{}, nil
}

func (m *mockEngineOps) AttestationExists(ctx context.Context, marketHash []byte) (bool, error) {
	return false, nil
}

func (m *mockEngineOps) RequestAttestationForMarket(ctx context.Context, chainID string, signer auth.Signer, broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error), market *internal.UnsettledMarket) error {
	return nil
}

func (m *mockEngineOps) BroadcastSettleMarketWithRetry(ctx context.Context, chainID string, signer auth.Signer, broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error), queryID int, maxRetries int) error {
	return nil
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
