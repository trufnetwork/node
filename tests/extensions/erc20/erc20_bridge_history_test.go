//go:build kwiltest

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
)

// TestERC20BridgeHistory verifies the transaction history API end-to-end.
func TestERC20BridgeHistory(t *testing.T) {
	seedAndRun(t, "erc20_bridge_history", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Initialize instance
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestExtensionAlias)
		require.NoError(t, err)

		// 1. Inject a Deposit (simulates external deposit)
		// This will create a 'deposit' record in transaction_history
		err = testerc20.InjectERC20Transfer(ctx, platform, TestChain, TestEscrowA, TestERC20, TestUserA, TestUserA, TestAmount1, 10, nil)
		require.NoError(t, err)

		// 2. Perform a Transfer (internal)
		// This will create a 'transfer' record
		// We use the extension's 'transfer' action via engine call
		engineCtx := engCtx(ctx, platform, TestUserA, 20, false) // Block height 20
		transferAmt, _ := types.ParseDecimalExplicit("500000000000000000", 78, 0) // 0.5 tokens

		// Transfer from UserA to UserB
		res, err := platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "transfer", []any{TestUserB, transferAmt}, nil)
		require.NoError(t, err)
		require.Nil(t, res.Error)

		// 3. Query History for UserA (should see Deposit + Transfer Sent)
		var historyRows [][]any
		res, err = platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "get_history", []any{TestUserA, int64(10), int64(0)}, func(row *common.Row) error {
			historyRows = append(historyRows, row.Values)
			return nil
		})
		require.NoError(t, err)
		require.Nil(t, res.Error)

		require.Len(t, historyRows, 2, "UserA should have 2 history records")

		// Expect newest first (Transfer at block 20, Deposit at block 10)
		transferRow := historyRows[0]
		depositRow := historyRows[1]

		// Verify Transfer Record
		require.Equal(t, "transfer", transferRow[0].(string))
		require.Equal(t, transferAmt.String(), transferRow[1].(*types.Decimal).String())
		// from_address
		// to_address
		// internal_tx_hash
		// external_tx_hash (should be nil)
		require.Equal(t, "completed", transferRow[6].(string))
		// block_height
		
		// Verify Deposit Record
		require.Equal(t, "deposit", depositRow[0].(string))
		require.Equal(t, TestAmount1, depositRow[1].(*types.Decimal).String())
		// external_tx_hash (should be set from InjectERC20Transfer mock)
		require.Equal(t, "completed", depositRow[6].(string))

		// 4. Query History for UserB (should see Transfer Received)
		historyRows = nil
		res, err = platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "get_history", []any{TestUserB, int64(10), int64(0)}, func(row *common.Row) error {
			historyRows = append(historyRows, row.Values)
			return nil
		})
		require.NoError(t, err)
		require.Nil(t, res.Error)

		require.Len(t, historyRows, 1, "UserB should have 1 history record")
		require.Equal(t, "transfer", historyRows[0][0].(string))
		require.Equal(t, transferAmt.String(), historyRows[0][1].(*types.Decimal).String())

		// 5. Perform a Withdrawal (bridge out)
		// This will create a 'withdrawal' record with status 'pending_epoch'
		withdrawalAmt, _ := types.ParseDecimalExplicit("100000000000000000", 78, 0) // 0.1 tokens
		withdrawalRecipient := "0x1111111111111111111111111111111111111111" // External address

		engineCtx3 := engCtx(ctx, platform, TestUserA, 30, false) // Block height 30
		res, err = platform.Engine.Call(engineCtx3, platform.DB, TestExtensionAlias, "bridge", []any{withdrawalRecipient, withdrawalAmt}, nil)
		require.NoError(t, err)
		require.Nil(t, res.Error)

		// 6. Query History for UserA again (should see Withdrawal + Transfer + Deposit)
		historyRows = nil
		res, err = platform.Engine.Call(engineCtx3, platform.DB, TestExtensionAlias, "get_history", []any{TestUserA, int64(10), int64(0)}, func(row *common.Row) error {
			historyRows = append(historyRows, row.Values)
			return nil
		})
		require.NoError(t, err)
		require.Nil(t, res.Error)

		require.Len(t, historyRows, 3, "UserA should have 3 history records")

		// Expect newest first (Withdrawal at block 30)
		withdrawalRow := historyRows[0]
		require.Equal(t, "withdrawal", withdrawalRow[0].(string))
		require.Equal(t, withdrawalAmt.String(), withdrawalRow[1].(*types.Decimal).String())
		// to_address should match recipient
		require.Equal(t, "pending_epoch", withdrawalRow[6].(string))
		require.Equal(t, int64(30), withdrawalRow[7].(int64)) // block_height

		return nil
	})
}
