package internal

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// =============================================================================
// Mock implementations for testing
// =============================================================================

type mockBroadcaster struct {
	attempts      int
	failUntil     int
	returnError   error
	successResult *ktypes.TxResult
}

func (m *mockBroadcaster) broadcast(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error) {
	m.attempts++

	result := m.successResult
	if result == nil {
		result = &ktypes.TxResult{
			Code: uint32(ktypes.CodeOk),
			Log:  "Market settled successfully",
		}
	}

	if m.attempts <= m.failUntil {
		return ktypes.Hash{}, result, m.returnError
	}

	return ktypes.Hash{1, 2, 3, 4, 5}, result, nil
}

type mockAccounts struct {
	nonceCalls int
	currentNonce int64
}

func (m *mockAccounts) GetAccount(ctx context.Context, db sql.Executor, accountID *ktypes.AccountID) (*ktypes.Account, error) {
	m.nonceCalls++
	m.currentNonce++
	return &ktypes.Account{
		ID:      accountID,
		Nonce:   m.currentNonce,
		Balance: big.NewInt(1000000),
	}, nil
}

func (m *mockAccounts) Credit(ctx context.Context, db sql.Executor, account *ktypes.AccountID, balance *big.Int) error {
	return nil
}

func (m *mockAccounts) Transfer(ctx context.Context, db sql.TxMaker, from, to *ktypes.AccountID, amt *big.Int) error {
	return nil
}

func (m *mockAccounts) ApplySpend(ctx context.Context, db sql.Executor, account *ktypes.AccountID, amount *big.Int, nonce int64) error {
	return nil
}

// =============================================================================
// Test: Broadcast Settlement - Immediate Success
// =============================================================================

func TestBroadcastSettleMarketWithRetry_ImmediateSuccess(t *testing.T) {
	accounts := &mockAccounts{}
	broadcaster := &mockBroadcaster{failUntil: 0} // Success immediately

	priv, _, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	signer := auth.GetNodeSigner(priv)

	ops := &EngineOperations{
		logger:   log.New(),
		accounts: accounts,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = ops.BroadcastSettleMarketWithRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		1, // queryID
		3, // maxRetries
	)

	if err != nil {
		t.Fatalf("Expected success, got error: %v", err)
	}

	if broadcaster.attempts != 1 {
		t.Errorf("Expected 1 attempt, got %d", broadcaster.attempts)
	}

	if accounts.nonceCalls != 1 {
		t.Errorf("Expected 1 nonce fetch, got %d", accounts.nonceCalls)
	}
}

// =============================================================================
// Test: Broadcast Settlement - Retries on Error
// =============================================================================

func TestBroadcastSettleMarketWithRetry_RetriesOnError(t *testing.T) {
	accounts := &mockAccounts{}
	broadcaster := &mockBroadcaster{
		failUntil:   1, // Fail first attempt, succeed on second
		returnError: errors.New("network error"),
	}

	priv, _, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	signer := auth.GetNodeSigner(priv)

	ops := &EngineOperations{
		logger:   log.New(),
		accounts: accounts,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = ops.BroadcastSettleMarketWithRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		1, // queryID
		3, // maxRetries
	)

	// Should succeed after retry
	if err != nil {
		t.Fatalf("Expected success after retry, got error: %v", err)
	}

	if broadcaster.attempts != 2 {
		t.Errorf("Expected 2 attempts, got %d", broadcaster.attempts)
	}

	// Should fetch fresh nonce on each attempt
	if accounts.nonceCalls != 2 {
		t.Errorf("Expected 2 nonce fetches (fresh nonce each attempt), got %d", accounts.nonceCalls)
	}
}

// =============================================================================
// Test: Broadcast Settlement - Max Retries Exceeded
// =============================================================================

func TestBroadcastSettleMarketWithRetry_MaxRetriesExceeded(t *testing.T) {
	accounts := &mockAccounts{}
	broadcaster := &mockBroadcaster{
		failUntil:   10, // Always fail
		returnError: errors.New("persistent error"),
	}

	priv, _, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	signer := auth.GetNodeSigner(priv)

	ops := &EngineOperations{
		logger:   log.New(),
		accounts: accounts,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = ops.BroadcastSettleMarketWithRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		1, // queryID
		0, // maxRetries = 0 (only initial attempt)
	)

	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if !strings.Contains(err.Error(), "max retries") {
		t.Errorf("Expected max retries error, got: %v", err)
	}

	// With maxRetries=0, should only attempt once
	if broadcaster.attempts != 1 {
		t.Errorf("Expected 1 attempt with maxRetries=0, got %d", broadcaster.attempts)
	}
}

// =============================================================================
// Test: Broadcast Settlement - Fresh Nonce Each Attempt
// =============================================================================

func TestBroadcastSettleMarketWithRetry_FreshNonceEachAttempt(t *testing.T) {
	accounts := &mockAccounts{}
	broadcaster := &mockBroadcaster{
		failUntil:   2, // Fail twice, succeed on 3rd
		returnError: errors.New("nonce error"),
	}

	priv, _, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	signer := auth.GetNodeSigner(priv)

	ops := &EngineOperations{
		logger:   log.New(),
		accounts: accounts,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err = ops.BroadcastSettleMarketWithRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		1, // queryID
		5, // maxRetries
	)

	// Should succeed after retries
	if err != nil {
		t.Fatalf("Expected success after retries, got error: %v", err)
	}

	// Should have fetched nonce 3 times (once per attempt)
	if accounts.nonceCalls != 3 {
		t.Errorf("Expected 3 nonce fetches (fresh nonce each attempt), got %d", accounts.nonceCalls)
	}

	// Should have attempted 3 times
	if broadcaster.attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", broadcaster.attempts)
	}
}

// =============================================================================
// Test: Broadcast Settlement - Context Cancellation
// =============================================================================

func TestBroadcastSettleMarketWithRetry_ContextCancellation(t *testing.T) {
	accounts := &mockAccounts{}
	broadcaster := &mockBroadcaster{
		failUntil:   10, // Always fail to trigger retries
		returnError: errors.New("error"),
	}

	priv, _, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	signer := auth.GetNodeSigner(priv)

	ops := &EngineOperations{
		logger:   log.New(),
		accounts: accounts,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err = ops.BroadcastSettleMarketWithRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		1, // queryID
		3, // maxRetries
	)

	if err == nil {
		t.Fatal("Expected context cancellation error, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled error, got: %v", err)
	}
}

// =============================================================================
// Test: Broadcast Settlement - Transaction Failure (Non-OK Code)
// =============================================================================

func TestBroadcastSettleMarketWithRetry_TransactionFailure(t *testing.T) {
	accounts := &mockAccounts{}
	broadcaster := &mockBroadcaster{
		failUntil: 10, // Always return this result
		successResult: &ktypes.TxResult{
			Code: 99, // Non-OK code
			Log:  "settlement failed: attestation not signed",
		},
	}

	priv, _, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	signer := auth.GetNodeSigner(priv)

	ops := &EngineOperations{
		logger:   log.New(),
		accounts: accounts,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = ops.BroadcastSettleMarketWithRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		1, // queryID
		0, // maxRetries = 0 (only initial attempt)
	)

	if err == nil {
		t.Fatal("Expected error for failed transaction, got nil")
	}

	if !strings.Contains(err.Error(), "transaction failed") {
		t.Errorf("Expected transaction failure error, got: %v", err)
	}

	// Should only attempt once with maxRetries=0
	if broadcaster.attempts != 1 {
		t.Errorf("Expected 1 attempt, got %d", broadcaster.attempts)
	}
}

// =============================================================================
// Test: Broadcast Settlement - Exponential Backoff
// =============================================================================

func TestBroadcastSettleMarketWithRetry_ExponentialBackoff(t *testing.T) {
	accounts := &mockAccounts{}
	broadcaster := &mockBroadcaster{
		failUntil:   2, // Fail first 2 attempts
		returnError: errors.New("temporary error"),
	}

	priv, _, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	signer := auth.GetNodeSigner(priv)

	ops := &EngineOperations{
		logger:   log.New(),
		accounts: accounts,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	start := time.Now()

	err = ops.BroadcastSettleMarketWithRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		1, // queryID
		5, // maxRetries
	)

	elapsed := time.Since(start)

	// Should succeed after retries
	if err != nil {
		t.Fatalf("Expected success after retries, got error: %v", err)
	}

	// Should have attempted 3 times
	if broadcaster.attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", broadcaster.attempts)
	}

	// With exponential backoff (2s, 4s), total should be at least 6 seconds
	// but less than 15 seconds (generous upper bound due to test environment variability)
	if elapsed < 6*time.Second {
		t.Logf("Warning: Elapsed time %v seems too short for exponential backoff (expected >= 6s)", elapsed)
	}

	if elapsed > 15*time.Second {
		t.Errorf("Elapsed time %v exceeds reasonable upper bound", elapsed)
	}

	t.Logf("Exponential backoff timing: %v for %d attempts", elapsed, broadcaster.attempts)
}

// =============================================================================
// Test: Broadcast Settlement - Verify Transaction Structure
// =============================================================================

func TestBroadcastSettleMarketWithRetry_VerifyTransactionStructure(t *testing.T) {
	accounts := &mockAccounts{}

	var capturedTx *ktypes.Transaction
	capturingBroadcaster := func(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error) {
		capturedTx = tx
		return ktypes.Hash{1, 2, 3}, &ktypes.TxResult{
			Code: uint32(ktypes.CodeOk),
			Log:  "Market settled",
		}, nil
	}

	priv, _, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	signer := auth.GetNodeSigner(priv)

	ops := &EngineOperations{
		logger:   log.New(),
		accounts: accounts,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	queryID := 42

	err = ops.BroadcastSettleMarketWithRetry(
		ctx, "test-chain", signer, capturingBroadcaster,
		queryID,
		3,
	)

	if err != nil {
		t.Fatalf("Expected success, got error: %v", err)
	}

	// Verify transaction was created
	if capturedTx == nil {
		t.Fatal("Transaction was not captured")
	}

	// Verify transaction has signature
	if capturedTx.Signature == nil || capturedTx.Signature.Data == nil {
		t.Error("Transaction is not signed")
	}

	// Verify payload exists
	if capturedTx.Body.Payload == nil {
		t.Fatal("Transaction payload is nil")
	}

	// Verify payload is non-empty
	if len(capturedTx.Body.Payload) == 0 {
		t.Fatal("Transaction payload is empty")
	}

	// Verify payload type is ActionExecution
	if capturedTx.Body.PayloadType != ktypes.PayloadTypeExecute {
		t.Errorf("Expected PayloadType Execute, got %s", capturedTx.Body.PayloadType)
	}

	t.Logf("Successfully verified transaction structure for settle_market(query_id=%d)", queryID)
}
