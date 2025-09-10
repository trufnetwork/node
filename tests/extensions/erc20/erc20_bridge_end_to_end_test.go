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
	deverc20 "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20/dev"
)

// TestERC20BridgeEndToEnd is a minimal E2E test using only actions + shims.
// Goal: deposit -> bridge -> finalize/confirm -> list_wallet_rewards shows reward row(s).
// Scope: prove happy-path building blocks; avoid deep verification of internals.
//
// Test flow:
// 1) Seed + init instance; set small distribution period with ForTestingSetDistributionPeriod.
// 2) Inject deposit to give the user a balance.
// 3) Call alias.bridge(amount) as user to lock+issue into epoch.
// 4) Finalize current epoch and create next (ForTestingFinalizeCurrentEpoch).
// 5) Confirm finalized epochs (ForTestingConfirmAllFinalizedEpochs).
// 6) Call alias.list_wallet_rewards(user,false) and assert at least one row returned.
func TestERC20BridgeEndToEnd(t *testing.T) {
	seedAndRun(t, "erc20_bridge_end_to_end", "simple_mock.sql", func(ctx context.Context, platform *kwilTesting.Platform) error {
		app := &common.App{DB: platform.DB, Engine: platform.Engine}

		// Singleton reset and initialize is handled by seedAndRun
		// Use the sepolia_bridge alias created by simple_mock.sql

		// Set small distribution period for quick epoch progression using dev helper
		require.NoError(t, deverc20.SetDistributionPeriod(ctx, app, TestChain, TestEscrowA, 1))

		err := erc20shim.ForTestingInitializeExtension(ctx, app)
		require.NoError(t, err)

		// Step 1: Inject deposit to give user a balance
		err = testerc20.InjectERC20Transfer(ctx, app, TestChain, TestEscrowA, TestERC20, TestUserA, TestEscrowA, TestAmount1, 10, nil)
		require.NoError(t, err)

		// Verify user has the balance
		balance, err := testerc20.GetUserBalance(ctx, platform, TestUserA)
		require.NoError(t, err)
		require.Equal(t, TestAmount1, balance, "user should have deposit amount")

		// Step 2: Call bridge action as user to lock+issue into epoch
		engineCtx := engCtx(ctx, platform, TestUserA, 2, false)

		amtDec, err := types.ParseDecimalExplicit(TestAmount1, 78, 0)
		require.NoError(t, err)
		_, err = platform.Engine.Call(engineCtx, platform.DB, "sepolia_bridge", "bridge", []any{amtDec}, func(row *common.Row) error {
			return nil
		})
		require.NoError(t, err)

		// Step 3: Finalize current epoch and create next
		var bh [32]byte
		require.NoError(t, erc20shim.ForTestingFinalizeCurrentEpoch(ctx, app, TestChain, TestEscrowA, 11, bh))

		// Step 4: Confirm finalized epochs
		require.NoError(t, erc20shim.ForTestingConfirmAllFinalizedEpochs(ctx, app, TestChain, TestEscrowA))

		// Step 5: Query wallet rewards to verify bridge flow worked
		engineCtx = engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 3, false)

		rewardRows := 0
		r, err := platform.Engine.Call(engineCtx, platform.DB, "sepolia_bridge", "list_wallet_rewards", []any{TestUserA, false}, func(row *common.Row) error {
			rewardRows++
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// Assert that at least one reward row was returned
		require.Greater(t, rewardRows, 0, "user should have at least one wallet reward after bridge flow")

		return nil
	})
}
