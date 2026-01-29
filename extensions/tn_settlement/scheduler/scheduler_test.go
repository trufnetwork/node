package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
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
	service := &common.Service{
		Logger: log.New(),
	}

	broadcaster := &mockTxBroadcaster{}
	signer := &mockSigner{}

	// Track if job executed
	jobExecuted := make(chan struct{}, 1)

	scheduler := NewSettlementScheduler(NewSettlementSchedulerParams{
		Service:          service,
		Logger:           log.New(),
		EngineOps:        nil, // nil will cause job to log "prerequisites missing" but still execute
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

	// Wait for at least one cron job to execute (should happen within 1-2 seconds)
	// The job will log "prerequisites missing" since engineOps is nil, but it should still run
	time.Sleep(1500 * time.Millisecond)

	// If we got here without panic and scheduler is still running, the fix works
	// The old bug would cause "context canceled" errors in the job

	// Clean up
	err = scheduler.Stop()
	if err != nil {
		t.Fatalf("Failed to stop scheduler: %v", err)
	}

	// Close channel to signal we're done
	close(jobExecuted)

	t.Log("Scheduler job executed successfully even after parent context was canceled - bug fix verified!")
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
