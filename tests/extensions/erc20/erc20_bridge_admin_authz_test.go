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

// TestERC20BridgeAdminAuthz is a minimal SYSTEM auth boundary test.
// Goal: ensure SYSTEM-only method lock_admin fails without OverrideAuthz and succeeds with it.
// Scope: avoid deep permission trees; just validate the two contrasting outcomes.
//
// Test flow:
// 1) Seed + init instance.
// 2) Inject deposit so user has balance.
// 3) Attempt lock_admin without OverrideAuthz: expect execution error.
// 4) Retry with OverrideAuthz: expect success and balance changes to reflect lock.
func TestERC20BridgeAdminAuthz(t *testing.T) {
	seedAndRun(t, "erc20_bridge_admin_authz", "simple_mock.sql", func(ctx context.Context, platform *kwilTesting.Platform) error {
		app := &common.App{DB: platform.DB, Engine: platform.Engine}

		// Singleton reset and initialize is handled by seedAndRun
		// Use the sepolia_bridge alias created by simple_mock.sql
		err := erc20shim.ForTestingInitializeExtension(ctx, app)
		require.NoError(t, err)

		// Step 1: Inject deposit so user has balance
		err = testerc20.InjectERC20Transfer(ctx, app, TestChain, TestEscrowA, TestERC20, TestUserA, TestEscrowA, TestAmount2, 10, nil)
		require.NoError(t, err)

		// Verify user has the balance
		balance, err := testerc20.GetUserBalance(ctx, platform, TestUserA)
		require.NoError(t, err)
		require.Equal(t, TestAmount2, balance, "user should have deposit amount")

		// Step 2: Attempt lock_admin without OverrideAuthz - should fail
		engineCtxFail := engCtx(ctx, platform, TestUserA, 2, false)

		// lock_admin expects amount as numeric(78,0)
		amtDec, err := types.ParseDecimalExplicit(TestAmount1, 78, 0)
		require.NoError(t, err)

		_, err = platform.Engine.Call(engineCtxFail, platform.DB, "sepolia_bridge", "lock_admin", []any{TestUserA, amtDec}, func(row *common.Row) error {
			return nil
		})
		// Should fail with authz error (SYSTEM method called without OverrideAuthz)
		require.Error(t, err, "action lock_admin is system-only")

		// Balance should remain unchanged
		balance, err = testerc20.GetUserBalance(ctx, platform, TestUserA)
		require.NoError(t, err)
		require.Equal(t, TestAmount2, balance, "balance should remain unchanged after failed lock_admin")

		// Step 3: Retry lock_admin WITH OverrideAuthz - should succeed
		engineCtxSuccess := engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 3, true)

		r, err := platform.Engine.Call(engineCtxSuccess, platform.DB, "sepolia_bridge", "lock_admin", []any{TestUserA, amtDec}, func(row *common.Row) error {
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// Step 4: Verify balance was reduced after successful lock_admin
		balance, err = testerc20.GetUserBalance(ctx, platform, TestUserA)
		require.NoError(t, err)
		require.Equal(t, TestAmount1, balance, "balance should be reduced after successful lock_admin")

		return nil
	})
}
