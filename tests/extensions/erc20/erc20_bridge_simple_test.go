//go:build kwiltest

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
)

// TestERC20BridgeSimpleBalanceTx uses transaction-based isolation for perfect test isolation
// This allows reusing the same user accounts and escrow addresses across tests.
// Creates the extension alias during test execution instead of using seed scripts.
func TestERC20BridgeSimpleBalanceTx(t *testing.T) {
	seedAndRun(t, "erc20_bridge_simple_balance_tx", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable instance with alias in one step
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestChain)
		require.NoError(t, err)

		// Run the actual test logic
		testSimpleBalanceTx(t, platform)
		return nil
	})
}

// testSimpleBalanceTx performs the actual balance test logic with transaction-scoped platform
func testSimpleBalanceTx(t *testing.T, txPlatform *kwilTesting.Platform) {
	// Test-specific constants - can reuse the same user across tests now!
	const testWallet = "0x1111111111111111111111111111111111110001"

	// Query balance for a wallet with no prior deposits
	balance, err := testerc20.GetUserBalance(context.Background(), txPlatform, TestChain, testWallet)
	require.NoError(t, err)

	// Should return "0" for wallet with no prior deposits
	require.Equal(t, "0", balance, "expected balance of 0 for wallet with no prior deposits")
}

// Removed complex listener tests - keeping only simple extension functionality tests
// that follow the same pattern as query_test.go for consistency

// TestERC20BridgeAdminLockAffectsBalanceTx validates that the lock_admin system method
// correctly reduces a user's balance when called with OverrideAuthz.
// Uses transaction-based isolation for proper test cleanup.
//
// Test Scenario:
// 1. Set up ERC-20 bridge extension with a synced instance
// 2. Create extension alias during test execution
// 3. Credit user's balance with a realistic ERC-20 transfer (simulating deposit)
// 4. Call lock_admin to lock the user's tokens
// 5. Verify user's balance is reduced to 0
//
// This test ensures admin lock functionality works correctly and integrates
// with the extension's balance tracking.
func TestERC20BridgeAdminLockAffectsBalanceTx(t *testing.T) {
	seedAndRun(t, "erc20_bridge_admin_lock_affects_balance_tx", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable instance with alias in one step
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestChain)
		require.NoError(t, err)

		// Run the actual test logic
		testAdminLockTx(t, platform)
		return nil
	})
}

// testAdminLockTx performs the actual admin lock test logic with transaction-scoped platform
func testAdminLockTx(t *testing.T, txPlatform *kwilTesting.Platform) {
	// Test-specific constants - can reuse the same user across tests now!
	const (
		testUser   = "0xabc0000000000000000000000000000000000001"
		testAmount = "1000000000000000000" // 1.0 tokens
	)

	// Step 1: Pre-credit user's balance with realistic ERC-20 transfer
	// This simulates a user depositing tokens into the bridge escrow
	// Use the same escrow address as the test setup
	err := testerc20.CreditUserBalance(context.Background(), txPlatform, TestChain, TestEscrowA, testUser, testAmount)
	require.NoError(t, err)

	// Step 2: Execute admin lock operation
	// This should reduce user's balance by the locked amount
	err = testerc20.CallLockAdmin(context.Background(), txPlatform, TestChain, testUser, testAmount)
	require.NoError(t, err)

	// Step 3: Verify balance was correctly reduced
	// After locking all tokens, balance should be 0
	finalBalance, err := testerc20.GetUserBalance(context.Background(), txPlatform, TestChain, testUser)
	require.NoError(t, err)
	require.Equal(t, "0", finalBalance,
		"expected user balance to be zero after admin lock of entire balance")
}
