//go:build kwiltest

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"

	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
)

// TestERC20BridgeTransferBalances is a minimal transfer flow test.
// Goal: verify node can credit a user via injected transfer, then transfer to another user and observe balances.
// Scope: avoid re-testing Kwil internals; assert only the observable integration points.
//
// Test flow:
// 1) Seed with simple_mock.sql and run schema test wrapper.
// 2) Initialize ERC20 test instance via shims.
// 3) Inject deposit: from userA -> escrow; assert alias.balance(userA) == injected.
// 4) Call alias.transfer(to=userB, amount=injected/2); assert balances for userA, userB updated accordingly.
func TestERC20BridgeTransferBalances(t *testing.T) {
	seedAndRun(t, "erc20_bridge_transfer_balances", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable instance with alias for transfer test
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestExtensionAlias)
		require.NoError(t, err)

		// Step 1: Inject deposit for userA
		err = testerc20.InjectERC20Transfer(ctx, platform, TestChain, TestEscrowA, TestERC20, TestUserA, TestUserA, TestAmount2, 10, nil)
		require.NoError(t, err)

		// Verify userA received the full deposit
		balanceA, err := testerc20.GetUserBalance(ctx, platform, TestExtensionAlias, TestUserA)
		require.NoError(t, err)
		require.Equal(t, TestAmount2, balanceA, "userA should have full deposit amount")

		// Verify userB has zero balance initially
		balanceB, err := testerc20.GetUserBalance(ctx, platform, TestExtensionAlias, TestUserB)
		require.NoError(t, err)
		require.Equal(t, "0", balanceB, "userB should have zero balance initially")

		// Step 2: Transfer half amount from userA to userB
		engineCtx := engCtx(ctx, platform, TestUserA, 2, false)

		// transfer expects amount as numeric(78,0)
		halfDec, err := types.ParseDecimalExplicit(TestAmount1, 78, 0)
		require.NoError(t, err)

		r, err := platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "transfer", []any{TestUserB, halfDec}, func(row *common.Row) error {
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// Step 3: Verify balances after transfer
		// userA should have remaining amount
		balanceA, err = testerc20.GetUserBalance(ctx, platform, TestExtensionAlias, TestUserA)
		require.NoError(t, err)
		require.Equal(t, TestAmount1, balanceA, "userA should have remaining amount after transfer")

		// userB should have received the transferred amount
		balanceB, err = testerc20.GetUserBalance(ctx, platform, TestExtensionAlias, TestUserB)
		require.NoError(t, err)
		require.Equal(t, TestAmount1, balanceB, "userB should have received transferred amount")

		return nil
	})
}
