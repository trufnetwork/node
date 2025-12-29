//go:build kwiltest

package tests

import (
	"context"
	"strings"
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"

	"github.com/trufnetwork/kwil-db/core/types"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
)

// Hoodi Testnet Configuration
const (
	// Hoodi chain identifier (must match chains.go registration)
	TestChainHoodi = "hoodi"

	// Hoodi bridge extension alias for testing
	TestExtensionAliasHoodi = "hoodi_bridge_test"

	// TEST-ONLY addresses (not real Hoodi contracts!)
	// Using fake addresses to avoid conflicts with migration-created instances
	// Real Hoodi addresses are in migrations: 0x878d6aaeb6e746033f50b8dc268d54b4631554e7
	TestEscrowHoodi = "0x3333333333333333333333333333333333333333" // Fake test escrow
	TestERC20Hoodi  = "0x4444444444444444444444444444444444444444" // Fake test ERC20

	// Test users (same as Sepolia for consistency)
	TestUserHoodiA = "0xabc0000000000000000000000000000000000001"
	TestUserHoodiB = "0xabc0000000000000000000000000000000000002"
	TestUserHoodiC = "0xabc0000000000000000000000000000000000003"

	// Test amounts (18 decimals for TRUF token)
	TestAmountHoodi1 = "1000000000000000000" // 1.0 TRUF
	TestAmountHoodi2 = "2000000000000000000" // 2.0 TRUF
)

// TestERC20BridgeEndToEndHoodi tests the complete bridge flow on Hoodi testnet.
//
// Test flow:
// 1) Initialize Hoodi bridge instance with test alias
// 2) Inject deposit to give user a balance (simulates Hoodi → Kwil deposit)
// 3) Call bridge action to lock tokens and create withdrawal request
// 4) Finalize and confirm epoch (makes withdrawal ready)
// 5) Verify user has wallet rewards (withdrawal proof available)
//
// This validates:
// - Hoodi chain is recognized by kwil-db
// - Deposit listener would work (simulated via injection)
// - Withdrawal flow works end-to-end
// - Epoch management functions correctly
func TestERC20BridgeEndToEndHoodi(t *testing.T) {
	seedAndRun(t, "erc20_bridge_end_to_end_hoodi", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable Hoodi bridge instance with alias for end-to-end test
		// Args: (ctx, platform, chain, escrow, erc20, decimals, distribution_period_blocks, alias)
		require.NoError(t, erc20shim.ForTestingSeedAndActivateInstance(
			ctx, platform,
			TestChainHoodi,          // Hoodi chain
			TestEscrowHoodi,         // Hoodi bridge proxy address
			TestERC20Hoodi,          // TRUF token on Hoodi
			18,                      // Token decimals
			1,                       // Distribution period (1 block for testing)
			TestExtensionAliasHoodi, // Test alias
		))

		// Sanity check: ensure the instance reports synced and enabled via info()
		engineCtx := engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 1, false)
		var syncedResult, enabledResult bool
		resInfo, errInfo := platform.Engine.Call(engineCtx, platform.DB, TestExtensionAliasHoodi, "info", []any{}, func(row *common.Row) error {
			if len(row.Values) < 9 {
				return nil
			}
			syncedResult = row.Values[6].(bool)  // synced column
			enabledResult = row.Values[8].(bool) // enabled column
			return nil
		})
		require.NoError(t, errInfo)
		if resInfo != nil && resInfo.Error != nil {
			return resInfo.Error
		}
		require.True(t, syncedResult, "Hoodi instance should be synced before bridge")
		require.True(t, enabledResult, "Hoodi instance should be enabled before bridge")

		// Step 1: Inject deposit to give user a balance
		// This simulates a user depositing TRUF on Hoodi testnet
		// In production, the deposit listener would detect this on-chain
		err := testerc20.InjectERC20Transfer(
			ctx, platform,
			TestChainHoodi,   // Hoodi chain
			TestEscrowHoodi,  // Bridge contract
			TestERC20Hoodi,   // TRUF token
			TestUserHoodiA,   // Depositor
			TestUserHoodiA,   // Recipient (same user)
			TestAmountHoodi1, // 1.0 TRUF
			10,               // Point counter
			nil,              // Previous point (nil for first deposit)
		)
		require.NoError(t, err)

		// Verify user has the balance
		balance, err := testerc20.GetUserBalance(ctx, platform, TestExtensionAliasHoodi, TestUserHoodiA)
		require.NoError(t, err)
		require.Equal(t, TestAmountHoodi1, balance, "user should have deposit amount (1.0 TRUF)")

		// Step 2: Call bridge action as user to lock tokens and create withdrawal
		// This simulates user requesting withdrawal from Kwil → Hoodi
		engineCtx = engCtx(ctx, platform, TestUserHoodiA, 2, false)

		amtDec, err := types.ParseDecimalExplicit(TestAmountHoodi1, 78, 0)
		require.NoError(t, err)

		r, err := platform.Engine.Call(engineCtx, platform.DB, TestExtensionAliasHoodi, "bridge", []any{TestUserHoodiA, amtDec}, func(row *common.Row) error {
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// Verify a pending reward exists after bridge for this instance and user
		instanceID := erc20shim.ForTestingGetInstanceID(TestChainHoodi, TestEscrowHoodi)
		preQ := `
		{kwil_erc20_meta}SELECT count(*) FROM epoch_rewards r
		JOIN epochs e ON e.id = r.epoch_id
		WHERE e.instance_id = $id AND r.recipient = $user`
		var preRows int
		err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB, preQ, map[string]any{
			"id":   instanceID,
			"user": ethcommon.HexToAddress(TestUserHoodiA).Bytes(),
		}, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return nil
			}
			preRows = int(row.Values[0].(int64))
			return nil
		})
		require.NoError(t, err)
		require.Greater(t, preRows, 0, "expected pending epoch reward before finalize for user in Hoodi instance")

		// Step 3-4: Deterministically finalize current epoch and confirm
		// This makes the withdrawal "ready" for claiming on Hoodi
		var bh [32]byte // Block hash (deterministic for testing)
		require.NoError(t, erc20shim.ForTestingFinalizeAndConfirmCurrentEpoch(
			ctx, platform,
			TestChainHoodi,  // Hoodi chain
			TestEscrowHoodi, // Bridge contract
			11,              // Point counter
			bh,              // Block hash
		))

		// Diagnostics: fetch instance id via alias to verify setup
		engineCtx = engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 3, false)
		var confirmedInstanceID *types.UUID
		resID, errID := platform.Engine.Call(engineCtx, platform.DB, TestExtensionAliasHoodi, "id", []any{}, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return nil
			}
			confirmedInstanceID = row.Values[0].(*types.UUID)
			return nil
		})
		require.NoError(t, errID)
		if resID != nil && resID.Error != nil {
			return resID.Error
		}
		require.NotNil(t, confirmedInstanceID, "Hoodi instance id should not be nil")

		// Step 5: Query wallet rewards (confirmed only) to verify bridge flow worked
		// This confirms the withdrawal proof is available via GetWithdrawalProof
		rewardRows := 0
		r, err = platform.Engine.Call(engineCtx, platform.DB, TestExtensionAliasHoodi, "list_wallet_rewards", []any{TestUserHoodiA, false}, func(row *common.Row) error {
			rewardRows++
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}
		require.Greater(t, rewardRows, 0, "user should have at least one wallet reward after Hoodi bridge flow")

		return nil
	})
}

// TestERC20BridgeCustomRecipientHoodi tests withdrawal to a different recipient.
//
// Test flow:
// 1) User A deposits TRUF on Hoodi
// 2) User A requests withdrawal but specifies User B as recipient
// 3) Epoch finalizes
// 4) Verify User B (not User A) has the withdrawal reward
//
// This validates:
// - Custom recipient feature works on Hoodi
// - Withdrawal proof would be generated for User B's address
func TestERC20BridgeCustomRecipientHoodi(t *testing.T) {
	seedAndRun(t, "erc20_bridge_custom_recipient_hoodi", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable Hoodi bridge instance with alias for custom recipient test
		require.NoError(t, erc20shim.ForTestingSeedAndActivateInstance(
			ctx, platform,
			TestChainHoodi,
			TestEscrowHoodi,
			TestERC20Hoodi,
			18,
			1,
			TestExtensionAliasHoodi,
		))

		// Give user A balance to bridge (1.0 TRUF)
		require.NoError(t, testerc20.InjectERC20Transfer(
			ctx, platform,
			TestChainHoodi,
			TestEscrowHoodi,
			TestERC20Hoodi,
			TestUserHoodiA, // Depositor
			TestUserHoodiA, // Recipient (same)
			TestAmountHoodi1,
			10,
			nil,
		))

		// User A calls bridge action but specifies User B as recipient
		engineCtx := engCtx(ctx, platform, TestUserHoodiA, 2, false)
		amtDec, err := types.ParseDecimalExplicit(TestAmountHoodi1, 78, 0)
		require.NoError(t, err)

		r, err := platform.Engine.Call(engineCtx, platform.DB, TestExtensionAliasHoodi, "bridge", []any{TestUserHoodiB, amtDec}, func(row *common.Row) error {
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// Finalize and confirm epoch
		var bh [32]byte
		require.NoError(t, erc20shim.ForTestingFinalizeAndConfirmCurrentEpoch(
			ctx, platform,
			TestChainHoodi,
			TestEscrowHoodi,
			11,
			bh,
		))

		// Verify User B (custom recipient) has the reward, not User A
		instanceID := erc20shim.ForTestingGetInstanceID(TestChainHoodi, TestEscrowHoodi)
		customRewardQuery := `
		{kwil_erc20_meta}SELECT count(*) FROM epoch_rewards r
		JOIN epochs e ON e.id = r.epoch_id
		WHERE e.instance_id = $id AND r.recipient = $user`
		var rows int
		err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB, customRewardQuery, map[string]any{
			"id":   instanceID,
			"user": ethcommon.HexToAddress(TestUserHoodiB).Bytes(),
		}, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return nil
			}
			rows = int(row.Values[0].(int64))
			return nil
		})
		require.NoError(t, err)
		require.Greater(t, rows, 0, "expected pending epoch reward for custom recipient (User B) on Hoodi")

		// Query wallet rewards for User B and verify recipient address
		engineCtx = engCtx(ctx, platform, TestUserHoodiA, 3, false)
		rewardRows := 0
		r, err = platform.Engine.Call(engineCtx, platform.DB, TestExtensionAliasHoodi, "list_wallet_rewards", []any{TestUserHoodiB, false}, func(row *common.Row) error {
			rewardRows++
			// Verify the recipient column contains User B's address
			recipientValue, ok := row.Values[4].(string)
			require.True(t, ok, "recipient should be string")
			require.True(t, strings.EqualFold(TestUserHoodiB, recipientValue), "recipient should be User B")
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}
		require.Greater(t, rewardRows, 0, "user B should have at least one wallet reward after custom bridge on Hoodi")

		return nil
	})
}

// TestERC20BridgeHoodiMultipleDeposits tests multiple deposits and withdrawals.
//
// Test flow:
// 1) User A deposits 1.0 TRUF
// 2) User B deposits 2.0 TRUF
// 3) User A withdraws 0.5 TRUF
// 4) User B withdraws 1.0 TRUF
// 5) Finalize epoch
// 6) Verify both users have correct wallet rewards
//
// This validates:
// - Multiple users can use the same bridge instance
// - Balances are tracked correctly per user
// - Partial withdrawals work
func TestERC20BridgeHoodiMultipleDeposits(t *testing.T) {
	seedAndRun(t, "erc20_bridge_hoodi_multiple_deposits", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable Hoodi bridge instance
		require.NoError(t, erc20shim.ForTestingSeedAndActivateInstance(
			ctx, platform,
			TestChainHoodi,
			TestEscrowHoodi,
			TestERC20Hoodi,
			18,
			1,
			TestExtensionAliasHoodi,
		))

		// Deposit for User A (1.0 TRUF)
		// First deposit has prev=nil
		pointA := int64(10)
		require.NoError(t, testerc20.InjectERC20Transfer(
			ctx, platform,
			TestChainHoodi,
			TestEscrowHoodi,
			TestERC20Hoodi,
			TestUserHoodiA,
			TestUserHoodiA,
			TestAmountHoodi1, // 1.0 TRUF
			pointA,
			nil, // First deposit: prev=nil
		))

		// Deposit for User B (2.0 TRUF)
		// MUST chain to previous deposit for ordered-sync processing
		pointB := int64(11)
		prevPoint := pointA // Chain to User A's deposit
		require.NoError(t, testerc20.InjectERC20Transfer(
			ctx, platform,
			TestChainHoodi,
			TestEscrowHoodi,
			TestERC20Hoodi,
			TestUserHoodiB,
			TestUserHoodiB,
			TestAmountHoodi2, // 2.0 TRUF
			pointB,
			&prevPoint, // Chain to previous deposit
		))

		// Verify balances
		balanceA, err := testerc20.GetUserBalance(ctx, platform, TestExtensionAliasHoodi, TestUserHoodiA)
		require.NoError(t, err)
		require.Equal(t, TestAmountHoodi1, balanceA, "User A should have 1.0 TRUF")

		balanceB, err := testerc20.GetUserBalance(ctx, platform, TestExtensionAliasHoodi, TestUserHoodiB)
		require.NoError(t, err)
		require.Equal(t, TestAmountHoodi2, balanceB, "User B should have 2.0 TRUF")

		// User A withdraws 0.5 TRUF
		halfAmount := "500000000000000000" // 0.5 TRUF
		engineCtxA := engCtx(ctx, platform, TestUserHoodiA, 2, false)
		amtDecHalf, err := types.ParseDecimalExplicit(halfAmount, 78, 0)
		require.NoError(t, err)

		r, err := platform.Engine.Call(engineCtxA, platform.DB, TestExtensionAliasHoodi, "bridge", []any{TestUserHoodiA, amtDecHalf}, func(row *common.Row) error {
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// User B withdraws 1.0 TRUF
		engineCtxB := engCtx(ctx, platform, TestUserHoodiB, 3, false)
		amtDecOne, err := types.ParseDecimalExplicit(TestAmountHoodi1, 78, 0)
		require.NoError(t, err)

		r, err = platform.Engine.Call(engineCtxB, platform.DB, TestExtensionAliasHoodi, "bridge", []any{TestUserHoodiB, amtDecOne}, func(row *common.Row) error {
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// Finalize and confirm epoch
		var bh [32]byte
		require.NoError(t, erc20shim.ForTestingFinalizeAndConfirmCurrentEpoch(
			ctx, platform,
			TestChainHoodi,
			TestEscrowHoodi,
			12,
			bh,
		))

		// Verify both users have wallet rewards
		engineCtx := engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 4, false)

		// Check User A rewards (should have 1 reward for 0.5 TRUF)
		rewardRowsA := 0
		r, err = platform.Engine.Call(engineCtx, platform.DB, TestExtensionAliasHoodi, "list_wallet_rewards", []any{TestUserHoodiA, false}, func(row *common.Row) error {
			rewardRowsA++
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}
		require.Greater(t, rewardRowsA, 0, "User A should have wallet rewards")

		// Check User B rewards (should have 1 reward for 1.0 TRUF)
		rewardRowsB := 0
		r, err = platform.Engine.Call(engineCtx, platform.DB, TestExtensionAliasHoodi, "list_wallet_rewards", []any{TestUserHoodiB, false}, func(row *common.Row) error {
			rewardRowsB++
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}
		require.Greater(t, rewardRowsB, 0, "User B should have wallet rewards")

		return nil
	})
}
