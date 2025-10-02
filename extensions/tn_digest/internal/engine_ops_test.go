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

func TestParseDigestResultFromTxLog_HasMoreTrue(t *testing.T) {
	log := "INFO something\nNOTICE: auto_digest:{\"processed_days\":2,\"total_deleted_rows\":500,\"has_more_to_delete\":true}\nother"
	res, err := parseDigestResultFromTxLog(log)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ProcessedDays != 2 {
		t.Fatalf("processed_days: want 2, got %d", res.ProcessedDays)
	}
	if res.TotalDeletedRows != 500 {
		t.Fatalf("total_deleted_rows: want 500, got %d", res.TotalDeletedRows)
	}
	if !res.HasMoreToDelete {
		t.Fatalf("has_more_to_delete: want true, got false")
	}
}

func TestParseDigestResultFromTxLog_HasMoreFalse(t *testing.T) {
	log := "auto_digest:{\"processed_days\":1,\"total_deleted_rows\":1234,\"has_more_to_delete\":false}"
	res, err := parseDigestResultFromTxLog(log)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ProcessedDays != 1 {
		t.Fatalf("processed_days: want 1, got %d", res.ProcessedDays)
	}
	if res.TotalDeletedRows != 1234 {
		t.Fatalf("total_deleted_rows: want 1234, got %d", res.TotalDeletedRows)
	}
	if res.HasMoreToDelete {
		t.Fatalf("has_more_to_delete: want false, got true")
	}
}

func TestParseDigestResultFromTxLog_NoEntry(t *testing.T) {
	log := "INFO: nothing relevant here\nNOTICE: something else"
	_, err := parseDigestResultFromTxLog(log)
	if err == nil {
		t.Fatalf("expected error for missing auto_digest entry, got nil")
	}
}

// Mock implementations for testing retry logic

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
			Log:  "auto_digest:{\"processed_days\":100,\"total_deleted_rows\":500,\"has_more_to_delete\":false}",
		}
	}

	if m.attempts <= m.failUntil {
		// Return error but still return result (in case of broadcast errors with partial results)
		return ktypes.Hash{}, result, m.returnError
	}

	return ktypes.Hash{1, 2, 3}, result, nil
}

type mockAccounts struct {
	nonceCalls int
}

func (m *mockAccounts) GetAccount(ctx context.Context, db sql.Executor, accountID *ktypes.AccountID) (*ktypes.Account, error) {
	m.nonceCalls++
	return &ktypes.Account{
		ID:      accountID,
		Nonce:   int64(m.nonceCalls),
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

// Test retry logic without actual time delays by using context with timeout
func TestBroadcastAutoDigestWithArgsAndRetry_ImmediateSuccess(t *testing.T) {
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

	result, err := ops.BroadcastAutoDigestWithArgsAndRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		10000, 24, 2, 3,
	)

	if err != nil {
		t.Fatalf("Expected success, got error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	if broadcaster.attempts != 1 {
		t.Errorf("Expected 1 attempt, got %d", broadcaster.attempts)
	}

	if accounts.nonceCalls != 1 {
		t.Errorf("Expected 1 nonce fetch, got %d", accounts.nonceCalls)
	}

	if result.ProcessedDays != 100 {
		t.Errorf("Expected 100 processed days, got %d", result.ProcessedDays)
	}
}

func TestBroadcastAutoDigestWithArgsAndRetry_RetriesOnError(t *testing.T) {
	accounts := &mockAccounts{}
	broadcaster := &mockBroadcaster{
		failUntil:   2, // Fail first 2 attempts
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

	// Use very short timeout to fail fast instead of waiting for real backoff
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = ops.BroadcastAutoDigestWithArgsAndRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		10000, 24, 2, 3,
	)

	// Should get context deadline exceeded because of backoff delays
	if err == nil {
		t.Fatal("Expected error due to context timeout, got nil")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded, got: %v", err)
	}

	// Should have attempted at least once
	if broadcaster.attempts < 1 {
		t.Errorf("Expected at least 1 attempt, got %d", broadcaster.attempts)
	}
}

func TestBroadcastAutoDigestWithArgsAndRetry_MaxRetriesExceeded(t *testing.T) {
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

	// Use short timeout to fail fast
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result, err := ops.BroadcastAutoDigestWithArgsAndRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		10000, 24, 2, 0, // maxRetries = 0 (only initial attempt)
	)

	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if result != nil {
		t.Errorf("Expected nil result, got %v", result)
	}

	// With maxRetries=0, should only attempt once
	if broadcaster.attempts != 1 {
		t.Errorf("Expected 1 attempt with maxRetries=0, got %d", broadcaster.attempts)
	}
}

func TestBroadcastAutoDigestWithArgsAndRetry_FreshNonceEachAttempt(t *testing.T) {
	accounts := &mockAccounts{}
	broadcaster := &mockBroadcaster{
		failUntil:   1, // Fail once, succeed on 2nd
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

	// Use longer timeout to allow one retry
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = ops.BroadcastAutoDigestWithArgsAndRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		10000, 24, 2, 3,
	)

	// Should succeed after retry
	if err != nil {
		t.Fatalf("Expected success after retry, got error: %v", err)
	}

	// Should have fetched nonce twice (once per attempt)
	if accounts.nonceCalls != 2 {
		t.Errorf("Expected 2 nonce fetches (fresh nonce each attempt), got %d", accounts.nonceCalls)
	}

	// Should have attempted twice
	if broadcaster.attempts != 2 {
		t.Errorf("Expected 2 attempts, got %d", broadcaster.attempts)
	}
}

func TestBroadcastAutoDigestWithArgsAndRetry_ContextCancellation(t *testing.T) {
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

	_, err = ops.BroadcastAutoDigestWithArgsAndRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		10000, 24, 2, 3,
	)

	if err == nil {
		t.Fatal("Expected context cancellation error, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled error, got: %v", err)
	}
}

func TestBroadcastAutoDigestWithArgsAndRetry_TransactionFailure(t *testing.T) {
	accounts := &mockAccounts{}
	broadcaster := &mockBroadcaster{
		failUntil: 10, // Always return this result
		successResult: &ktypes.TxResult{
			Code: 99, // Non-OK code
			Log:  "transaction failed",
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

	// Set maxRetries=0 to only try once
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err = ops.BroadcastAutoDigestWithArgsAndRetry(
		ctx, "test-chain", signer, broadcaster.broadcast,
		10000, 24, 2, 0, // maxRetries=0
	)

	if err == nil {
		t.Fatal("Expected error for failed transaction, got nil")
	}

	// Retry logic retries on ANY error, including transaction failures
	// So we should get the transaction failure error
	if !strings.Contains(err.Error(), "transaction failed with code 99") && !strings.Contains(err.Error(), "max retries") {
		t.Errorf("Expected transaction failure or max retries error, got: %v", err)
	}
}
