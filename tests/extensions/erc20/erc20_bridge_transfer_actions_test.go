//go:build kwiltest

package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"

	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
)

// TestSepoliaTransferActions tests the new sepolia_transfer() and sepolia_balance() public actions.
// This validates that users can transfer TRUF tokens directly using the new SQL actions
// without needing to use the extension methods directly.
func TestSepoliaTransferActions(t *testing.T) {
	seedAndRun(t, "sepolia_transfer_actions", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable instance with alias in one step
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestExtensionAlias)
		require.NoError(t, err)

		// Get the configured escrow address from bridge info
		configuredEscrow, err := getBridgeEscrowAddress(ctx, platform)
		require.NoError(t, err)

		// Credit initial balance to TestUserA using configured escrow
		// Need 3.0 TRUF: 1.0 for transfer + 1.0 fee + 1.0 remaining
		initialAmount := "3000000000000000000" // 3.0 TRUF
		err = testerc20.InjectERC20Transfer(ctx, platform,
			TestChain, configuredEscrow, TestERC20, TestUserA, TestUserA, initialAmount, 10, nil)
		require.NoError(t, err)

		// Verify initial balance via sepolia_wallet_balance action
		balanceA, err := callSepoliaWalletBalance(ctx, platform, TestUserA)
		require.NoError(t, err)
		require.Equal(t, initialAmount, balanceA, "UserA should have initial deposit")

		// Verify TestUserB has zero balance initially
		balanceB, err := callSepoliaWalletBalance(ctx, platform, TestUserB)
		require.NoError(t, err)
		require.Equal(t, "0", balanceB, "UserB should have zero balance initially")

		// Execute transfer via sepolia_transfer action (1.0 TRUF + 1.0 fee)
		err = callSepoliaTransfer(ctx, platform, TestUserA, TestUserB, TestAmount1)
		require.NoError(t, err)

		// Verify balances after transfer
		// UserA: 3.0 - 1.0 transfer - 1.0 fee = 1.0 remaining
		balanceA, err = callSepoliaWalletBalance(ctx, platform, TestUserA)
		require.NoError(t, err)
		require.Equal(t, TestAmount1, balanceA, "UserA should have remaining amount after transfer")

		balanceB, err = callSepoliaWalletBalance(ctx, platform, TestUserB)
		require.NoError(t, err)
		require.Equal(t, TestAmount1, balanceB, "UserB should have received transferred amount")

		return nil
	})
}

// TestTransferActionValidation tests the validation logic in the new transfer actions.
func TestTransferActionValidation(t *testing.T) {
	seedAndRun(t, "transfer_action_validation", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable instance with alias in one step
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestExtensionAlias)
		require.NoError(t, err)

		// Get the configured escrow address from bridge info
		configuredEscrow, err := getBridgeEscrowAddress(ctx, platform)
		require.NoError(t, err)

		// Test Case 1: Invalid address format
		t.Log("Testing invalid address format...")
		err = callSepoliaTransfer(ctx, platform, TestUserA, "invalid_address", TestAmount1)
		if err == nil {
			t.Log("ERROR: Expected error for invalid address format but got none")
		}
		require.Error(t, err)
		require.Contains(t, err.Error(), "Invalid Ethereum address format")

		// Test Case 2: Address without 0x prefix
		t.Log("Testing address without 0x prefix...")
		err = callSepoliaTransfer(ctx, platform, TestUserA, "1111111111111111111111111111111111111111", TestAmount1)
		if err == nil {
			t.Log("ERROR: Expected error for address without 0x prefix but got none")
		}
		require.Error(t, err)
		require.Contains(t, err.Error(), "Invalid Ethereum address format")

		// Test Case 3: Zero amount
		t.Log("Testing zero amount...")
		err = callSepoliaTransfer(ctx, platform, TestUserA, TestUserB, "0")
		if err == nil {
			t.Log("ERROR: Expected error for zero amount but got none")
		}
		require.Error(t, err)
		require.Contains(t, err.Error(), "Transfer amount must be positive")

		// Test Case 4: Negative amount (string parsing will fail at conversion)
		t.Log("Testing negative amount...")
		err = callSepoliaTransfer(ctx, platform, TestUserA, TestUserB, "-10")
		if err == nil {
			t.Log("ERROR: Expected error for negative amount but got none")
		}
		require.Error(t, err)

		// Test Case 5: Insufficient balance (user has no balance record - nil balance)
		t.Log("Testing insufficient balance with nil balance...")
		err = callSepoliaTransfer(ctx, platform, TestUserA, TestUserB, TestAmount1)
		if err == nil {
			t.Log("ERROR: Expected error for insufficient balance (nil) but got none")
		}
		require.Error(t, err)
		require.Contains(t, err.Error(), "Insufficient balance")

		// Test Case 6: Insufficient balance (user has some balance but not enough for transfer + fee)
		t.Log("Testing insufficient balance with partial balance...")

		// Give TestUserA 1.5 TRUF (not enough for 1.0 transfer + 1.0 fee)
		smallAmount := "1500000000000000000" // 1.5 tokens
		err = testerc20.InjectERC20Transfer(ctx, platform,
			TestChain, configuredEscrow, TestERC20, TestUserA, TestUserA, smallAmount, 10, nil)
		require.NoError(t, err)

		// Verify they have the small balance
		balance, err := callSepoliaWalletBalance(ctx, platform, TestUserA)
		require.NoError(t, err)
		require.Equal(t, smallAmount, balance, "TestUserA should have small balance")

		// Try to transfer 1.0 TRUF (but needs 2.0 total: 1.0 + 1.0 fee, only has 1.5)
		err = callSepoliaTransfer(ctx, platform, TestUserA, TestUserB, TestAmount1)
		if err == nil {
			t.Log("ERROR: Expected error for insufficient balance (partial) but got none")
		}
		require.Error(t, err)
		require.Contains(t, err.Error(), "Insufficient balance")
		require.Contains(t, err.Error(), "Requires an extra 1 TRUF fee")

		// Test balance query with invalid address
		t.Log("Testing balance query with invalid address...")
		_, err = callSepoliaWalletBalance(ctx, platform, "invalid_address")
		if err == nil {
			t.Log("ERROR: Expected error for invalid address in balance query but got none")
		}
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid ethereum address")

		return nil
	})
}

// TestMultipleTransferActions tests multiple sequential transfers using the new actions.
func TestMultipleTransferActions(t *testing.T) {
	seedAndRun(t, "multiple_transfer_actions", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable instance with alias in one step
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestExtensionAlias)
		require.NoError(t, err)

		// Get the configured escrow address from bridge info
		configuredEscrow, err := getBridgeEscrowAddress(ctx, platform)
		require.NoError(t, err)

		// Define test users
		userA := TestUserA
		userB := TestUserB
		userC := TestUserC

		// Credit large initial balance to userA
		// Need: 3.0 + 1.0 fee + 2.0 + 1.0 fee = 7.0 TRUF for A's transfers
		initialAmount := "10000000000000000000" // 10.0 tokens
		err = testerc20.InjectERC20Transfer(ctx, platform,
			TestChain, configuredEscrow, TestERC20, userA, userA, initialAmount, 10, nil)
		require.NoError(t, err)

		// Transfer A -> B (3 tokens + 1 fee = 4 deducted from A)
		err = callSepoliaTransfer(ctx, platform, userA, userB, "3000000000000000000")
		require.NoError(t, err)

		// Transfer A -> C (2 tokens + 1 fee = 3 deducted from A)
		err = callSepoliaTransfer(ctx, platform, userA, userC, TestAmount2)
		require.NoError(t, err)

		// Transfer B -> C (1 token + 1 fee = 2 deducted from B)
		err = callSepoliaTransfer(ctx, platform, userB, userC, TestAmount1)
		require.NoError(t, err)

		// Verify final balances
		// UserA: 10 - (3 + 1 fee) - (2 + 1 fee) = 10 - 4 - 3 = 3
		balanceA, err := callSepoliaWalletBalance(ctx, platform, userA)
		require.NoError(t, err)
		require.Equal(t, "3000000000000000000", balanceA, "UserA should have 3 tokens remaining after transfers + fees")

		// UserB: 3 received - (1 + 1 fee) = 3 - 2 = 1
		balanceB, err := callSepoliaWalletBalance(ctx, platform, userB)
		require.NoError(t, err)
		require.Equal(t, TestAmount1, balanceB, "UserB should have 1 token remaining after transfer + fee")

		// UserC: 2 received + 1 received = 3
		balanceC, err := callSepoliaWalletBalance(ctx, platform, userC)
		require.NoError(t, err)
		require.Equal(t, "3000000000000000000", balanceC, "UserC should have 3 tokens total")

		return nil
	})
}

// Helper function to call sepolia_transfer action
func callSepoliaTransfer(ctx context.Context, platform *kwilTesting.Platform, from, to, amount string) error {
	engineCtx := engCtx(ctx, platform, from, 1, false)

	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "sepolia_transfer", []any{to, amount}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}

// Helper function to call sepolia_wallet_balance action
func callSepoliaWalletBalance(ctx context.Context, platform *kwilTesting.Platform, userAddr string) (string, error) {
	engineCtx := engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 1, false)

	var balance string
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "sepolia_wallet_balance", []any{userAddr}, func(row *common.Row) error {
		if len(row.Values) != 1 {
			return fmt.Errorf("expected 1 column, got %d", len(row.Values))
		}
		balance = row.Values[0].(*types.Decimal).String()
		return nil
	})
	if err != nil {
		return "", err
	}
	if res != nil && res.Error != nil {
		return "", res.Error
	}
	return balance, nil
}

// TestTransferActionFeeExactBalance tests transfer with exactly enough balance (amount + 1 TRUF fee).
func TestTransferActionFeeExactBalance(t *testing.T) {
	seedAndRun(t, "transfer_action_fee_exact", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable instance with alias in one step
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestExtensionAlias)
		require.NoError(t, err)

		// Get the configured escrow address from bridge info
		configuredEscrow, err := getBridgeEscrowAddress(ctx, platform)
		require.NoError(t, err)

		// Give TestUserA exactly 2.0 TRUF (1.0 transfer + 1.0 fee)
		exactAmount := "2000000000000000000" // 2.0 TRUF
		err = testerc20.InjectERC20Transfer(ctx, platform,
			TestChain, configuredEscrow, TestERC20, TestUserA, TestUserA, exactAmount, 10, nil)
		require.NoError(t, err)

		transferAmount := TestAmount1 // 1.0 TRUF

		// Transfer should succeed
		err = callSepoliaTransfer(ctx, platform, TestUserA, TestUserB, transferAmount)
		require.NoError(t, err)

		// Verify sender now has 0 balance (2.0 - 1.0 transfer - 1.0 fee = 0)
		balanceA, err := callSepoliaWalletBalance(ctx, platform, TestUserA)
		require.NoError(t, err)
		require.Equal(t, "0", balanceA, "UserA should have 0 balance after transfer + fee")

		// Verify recipient received only the transfer amount (not the fee)
		balanceB, err := callSepoliaWalletBalance(ctx, platform, TestUserB)
		require.NoError(t, err)
		require.Equal(t, transferAmount, balanceB, "UserB should have received transfer amount")

		return nil
	})
}

// TestTransferActionFeeInsufficientBalance tests that transfers fail when user doesn't have enough for fee.
func TestTransferActionFeeInsufficientBalance(t *testing.T) {
	seedAndRun(t, "transfer_action_fee_insufficient", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable instance with alias in one step
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestExtensionAlias)
		require.NoError(t, err)

		// Get the configured escrow address from bridge info
		configuredEscrow, err := getBridgeEscrowAddress(ctx, platform)
		require.NoError(t, err)

		// Give TestUserC exactly 1.0 TRUF (not enough for 0.5 transfer + 1.0 fee)
		err = testerc20.InjectERC20Transfer(ctx, platform,
			TestChain, configuredEscrow, TestERC20, TestUserC, TestUserC, TestAmount1, 10, nil)
		require.NoError(t, err)

		// Try to transfer 0.5 TRUF (but needs 1.5 total: 0.5 + 1.0 fee)
		halfAmount := "500000000000000000" // 0.5 TRUF
		err = callSepoliaTransfer(ctx, platform, TestUserC, TestUserB, halfAmount)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Insufficient balance for transfer")
		require.Contains(t, err.Error(), "Requires an extra 1 TRUF fee")

		return nil
	})
}

// TestTransferActionFeeMultipleTransfers tests that fee is deducted on each transfer.
func TestTransferActionFeeMultipleTransfers(t *testing.T) {
	seedAndRun(t, "transfer_action_fee_multiple", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable instance with alias in one step
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestExtensionAlias)
		require.NoError(t, err)

		// Get the configured escrow address from bridge info
		configuredEscrow, err := getBridgeEscrowAddress(ctx, platform)
		require.NoError(t, err)

		// Give TestUserD 4.0 TRUF (enough for 1.0 transfer + 1.0 fee, twice)
		fourTRUF := "4000000000000000000" // 4.0 TRUF
		err = testerc20.InjectERC20Transfer(ctx, platform,
			TestChain, configuredEscrow, TestERC20, TestUserD, TestUserD, fourTRUF, 10, nil)
		require.NoError(t, err)

		// Verify TestUserD has 4.0 TRUF initially
		balanceD, err := callSepoliaWalletBalance(ctx, platform, TestUserD)
		require.NoError(t, err)
		require.Equal(t, fourTRUF, balanceD, "UserD should have 4.0 TRUF initially")

		// First transfer: 1.0 TRUF + 1.0 fee = 2.0 deducted
		err = callSepoliaTransfer(ctx, platform, TestUserD, TestUserB, TestAmount1)
		require.NoError(t, err)

		balanceD, err = callSepoliaWalletBalance(ctx, platform, TestUserD)
		require.NoError(t, err)
		require.Equal(t, "2000000000000000000", balanceD, "UserD should have 2.0 after first transfer+fee")

		// Second transfer: 1.0 TRUF + 1.0 fee = 2.0 deducted
		err = callSepoliaTransfer(ctx, platform, TestUserD, TestUserB, TestAmount1)
		require.NoError(t, err)

		balanceD, err = callSepoliaWalletBalance(ctx, platform, TestUserD)
		require.NoError(t, err)
		require.Equal(t, "0", balanceD, "UserD should have 0 after second transfer+fee")

		return nil
	})
}

// Helper function to get the configured escrow address from bridge info
func getBridgeEscrowAddress(ctx context.Context, platform *kwilTesting.Platform) (string, error) {
	engineCtx := engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 1, false)

	var escrow string
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "sepolia_get_erc20_bridge_info", []any{}, func(row *common.Row) error {
		if len(row.Values) < 2 {
			return fmt.Errorf("expected at least 2 columns, got %d", len(row.Values))
		}
		escrow = row.Values[1].(string) // escrow is the second column
		return nil
	})
	if err != nil {
		return "", err
	}
	if res != nil && res.Error != nil {
		return "", res.Error
	}
	return escrow, nil
}
