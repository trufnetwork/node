//go:build kwiltest

package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
)

// WithERC20TestSetup is a helper function that sets up the test environment with ERC-20 bridge
// Follows the same pattern as WithQueryTestSetup in query_test.go for consistency
func WithERC20TestSetup(escrowAddr string) func(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	setupFunc := testerc20.WithERC20TestSetup(escrowAddr)
	return func(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
		return func(ctx context.Context, platform *kwilTesting.Platform) error {
			// First run the ERC20 setup
			err := setupFunc(ctx, platform)
			if err != nil {
				return err
			}
			// Then run the test function
			return testFn(ctx, platform)
		}
	}
}

// TestERC20BridgeSimpleBalanceTx uses transaction-based isolation for perfect test isolation
// This allows reusing the same user accounts and escrow addresses across tests.
func TestERC20BridgeSimpleBalanceTx(t *testing.T) {
	// Compute absolute path to the seed script relative to this test file
	_, thisFile, _, _ := runtime.Caller(0)
	seedPath := filepath.Join(filepath.Dir(thisFile), "simple_mock.sql")

	seedScripts := migrations.GetSeedScriptPaths()
	seedScripts = append(seedScripts, seedPath)

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "erc20_bridge_simple_balance_tx",
		SeedScripts: seedScripts,
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				// Use transaction-based isolation with WithTx
				testFunc := testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
					// Setup ERC20 with the default escrow from simple_mock.sql
					testerc20.WithERC20TestSetupTx("0x1111111111111111111111111111111111111111")(t, txPlatform)

					// Now run the actual test logic with the transaction-scoped platform
					testSimpleBalanceTx(t, txPlatform)
				})

				// Execute the transaction-wrapped test
				testFunc(t)
				return nil
			},
		},
	}, &testutils.Options{Options: testutils.GetTestOptions()})
}

// testSimpleBalanceTx performs the actual balance test logic with transaction-scoped platform
func testSimpleBalanceTx(t *testing.T, txPlatform *kwilTesting.Platform) {
	// Test-specific constants - can reuse the same user across tests now!
	const testWallet = "0x1111111111111111111111111111111111110001"

	// Query balance for a wallet with no prior deposits
	balance, err := testerc20.GetUserBalance(context.Background(), txPlatform, testWallet)
	require.NoError(t, err)

	// Should return "0" for wallet with no prior deposits
	require.Equal(t, "0", balance, "expected balance of 0 for wallet with no prior deposits")
}

// testSimpleBalance performs the actual balance test logic (legacy version)
func testSimpleBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Test-specific constants
		const testWallet = "0x1111111111111111111111111111111111110001"

		// Query balance for a wallet with no prior deposits
		balance, err := testerc20.GetUserBalance(ctx, platform, testWallet)
		if err != nil {
			return fmt.Errorf("failed to get user balance: %w", err)
		}

		// Should return "0" for wallet with no prior deposits
		require.Equal(t, "0", balance, "expected balance of 0 for wallet with no prior deposits")
		return nil
	}
}

// Removed complex listener tests - keeping only simple extension functionality tests
// that follow the same pattern as query_test.go for consistency

// TestERC20BridgeAdminLockAffectsBalanceTx validates that the lock_admin system method
// correctly reduces a user's balance when called with OverrideAuthz.
// Uses transaction-based isolation for proper test cleanup.
//
// Test Scenario:
// 1. Set up ERC-20 bridge extension with a synced instance
// 2. Credit user's balance with a realistic ERC-20 transfer (simulating deposit)
// 3. Call lock_admin to lock the user's tokens
// 4. Verify user's balance is reduced to 0
//
// This test ensures admin lock functionality works correctly and integrates
// with the extension's balance tracking.
func TestERC20BridgeAdminLockAffectsBalanceTx(t *testing.T) {
	// Compute absolute path to the seed script relative to this test file
	_, thisFile, _, _ := runtime.Caller(0)
	seedPath := filepath.Join(filepath.Dir(thisFile), "simple_mock.sql")

	seedScripts := migrations.GetSeedScriptPaths()
	seedScripts = append(seedScripts, seedPath)

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "erc20_bridge_admin_lock_affects_balance_tx",
		SeedScripts: seedScripts,
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				// Use transaction-based isolation with WithTx
				testFunc := testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
					// Setup ERC20 with the default escrow from simple_mock.sql
					testerc20.WithERC20TestSetupTx("0x1111111111111111111111111111111111111111")(t, txPlatform)

					// Now run the actual test logic with the transaction-scoped platform
					testAdminLockTx(t, txPlatform)
				})

				// Execute the transaction-wrapped test
				testFunc(t)
				return nil
			},
		},
	}, &testutils.Options{Options: testutils.GetTestOptions()})
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
	err := testerc20.CreditUserBalance(context.Background(), txPlatform, "0x1111111111111111111111111111111111111111", testUser, testAmount)
	require.NoError(t, err)

	// Step 2: Execute admin lock operation
	// This should reduce user's balance by the locked amount
	err = testerc20.CallLockAdmin(context.Background(), txPlatform, testUser, testAmount)
	require.NoError(t, err)

	// Step 3: Verify balance was correctly reduced
	// After locking all tokens, balance should be 0
	finalBalance, err := testerc20.GetUserBalance(context.Background(), txPlatform, testUser)
	require.NoError(t, err)
	require.Equal(t, "0", finalBalance,
		"expected user balance to be zero after admin lock of entire balance")
}
