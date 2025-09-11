//go:build kwiltest

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"

	"github.com/trufnetwork/kwil-db/core/types"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
)

// TestERC20BridgeMultiInstanceIsolation is a minimal multi-instance test.
// Goal: prove balances are isolated across two escrows/aliases.
// Scope: do not test cross-instance internals, only observable separation.
//
// Test flow:
// 1) Create two separate ERC20 instances with different escrow addresses.
// 2) Use direct SQL to create two aliases in the test database.
// 3) Inject deposit only for escrowA, userX.
// 4) Assert aliasA.balance(userX) > 0 and aliasB.balance(userX) == 0.
func TestERC20BridgeMultiInstanceIsolation(t *testing.T) {
	seedAndRun(t, "erc20_bridge_multi_instance_isolation", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use distinct escrow addresses to avoid conflicts with seed files and other tests
		aliasA := "aliasA"
		aliasB := "aliasB"
		escrowA := "0xffaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		escrowB := "0xffbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

		// Initialize both instances active+synced
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, escrowA, TestERC20, 18, 60, aliasA)
		require.NoError(t, err)
		err = erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, escrowB, TestERC20, 18, 60, aliasB)
		require.NoError(t, err)

		// Step 1: Inject deposit only for escrowA, userX
		// This simulates a deposit to escrowA only
		err = testerc20.InjectERC20Transfer(ctx, platform, TestChain, escrowA, TestERC20, TestUserA, escrowA, TestAmount1, 10, nil)
		require.NoError(t, err)

		// Step 2: Verify isolation - aliasA should have balance, aliasB should not
		engineCtx := engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 2, false)

		// Check balance in aliasA (should have deposit)
		var balanceA *types.Decimal
		r, err := platform.Engine.Call(engineCtx, platform.DB, aliasA, "balance", []any{TestUserA}, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return nil
			}
			balanceA, _ = row.Values[0].(*types.Decimal)
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		require.Equal(t, TestAmount1, balanceA.String(), "aliasA should have the deposit amount")

		// Check balance in aliasB (should be zero - no deposit made)
		var balanceB *types.Decimal
		r, err = platform.Engine.Call(engineCtx, platform.DB, aliasB, "balance", []any{TestUserA}, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return nil
			}
			balanceB, _ = row.Values[0].(*types.Decimal)
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		require.Equal(t, "0", balanceB.String(), "aliasB should have zero balance (no deposit made)")

		// Cleanup: Deactivate both instances for test isolation
		erc20shim.ForTestingDisableInstance(ctx, platform, TestChain, escrowA, aliasA)
		erc20shim.ForTestingDisableInstance(ctx, platform, TestChain, escrowB, aliasB)

		return nil
	})
}
