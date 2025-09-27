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
	seedAndRun(t, "erc20_bridge_end_to_end", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable instance with alias for end-to-end test
		require.NoError(t, erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 1, TestExtensionAlias))

		// Sanity: ensure the instance reports synced and enabled via info()
		engineCtx := engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 1, false)
		var syncedResult, enabledResult bool
		resInfo, errInfo := platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "info", []any{}, func(row *common.Row) error {
			if len(row.Values) < 9 {
				return nil
			}
			syncedResult = row.Values[6].(bool)
			enabledResult = row.Values[8].(bool)
			return nil
		})
		require.NoError(t, errInfo)
		if resInfo != nil && resInfo.Error != nil {
			return resInfo.Error
		}
		require.True(t, syncedResult, "instance should be synced before bridge")
		require.True(t, enabledResult, "instance should be enabled before bridge")

		// Step 1: Inject deposit to give user a balance
		err := testerc20.InjectERC20Transfer(ctx, platform, TestChain, TestEscrowA, TestERC20, TestUserA, TestEscrowA, TestAmount1, 10, nil)
		require.NoError(t, err)

		// Verify user has the balance
		balance, err := testerc20.GetUserBalance(ctx, platform, TestExtensionAlias, TestUserA)
		require.NoError(t, err)
		require.Equal(t, TestAmount1, balance, "user should have deposit amount")

		// Step 2: Call bridge action as user to lock+issue into epoch
		engineCtx = engCtx(ctx, platform, TestUserA, 2, false)

		amtDec, err := types.ParseDecimalExplicit(TestAmount1, 78, 0)
		require.NoError(t, err)
		r, err := platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "bridge", []any{TestUserA, amtDec}, func(row *common.Row) error {
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// Verify a pending reward exists after bridge for this instance and user
		preQ := `
		{kwil_erc20_meta}SELECT count(*) FROM epoch_rewards r
		JOIN epochs e ON e.id = r.epoch_id
		WHERE e.instance_id = $id AND r.recipient = $user`
		var preRows int
		err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB, preQ, map[string]any{
			"id":   erc20shim.ForTestingGetInstanceID(TestChain, TestEscrowA),
			"user": ethcommon.HexToAddress(TestUserA).Bytes(),
		}, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return nil
			}
			preRows = int(row.Values[0].(int64))
			return nil
		})
		require.NoError(t, err)
		require.Greater(t, preRows, 0, "expected pending epoch reward before finalize for user in this instance")

		// Step 3-4: Deterministically finalize current epoch and confirm
		var bh [32]byte
		require.NoError(t, erc20shim.ForTestingFinalizeAndConfirmCurrentEpoch(ctx, platform, TestChain, TestEscrowA, 11, bh))

		// Diagnostics: fetch instance id via alias
		engineCtx = engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 3, false)
		var instanceID *types.UUID
		resID, errID := platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "id", []any{}, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return nil
			}
			instanceID = row.Values[0].(*types.UUID)
			return nil
		})
		require.NoError(t, errID)
		if resID != nil && resID.Error != nil {
			return resID.Error
		}
		require.NotNil(t, instanceID, "instance id should not be nil")

		// Step 5: Query wallet rewards (confirmed only) to verify bridge flow worked deterministically
		rewardRows := 0
		r, err = platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "list_wallet_rewards", []any{TestUserA, false}, func(row *common.Row) error {
			rewardRows++
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}
		require.Greater(t, rewardRows, 0, "user should have at least one wallet reward after bridge flow")

		return nil
	})
}

func TestERC20BridgeCustomRecipient(t *testing.T) {
	seedAndRun(t, "erc20_bridge_custom_recipient", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Enable instance with alias for custom recipient test
		require.NoError(t, erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 1, TestExtensionAlias))

		// Give user A balance to bridge
		require.NoError(t, testerc20.InjectERC20Transfer(ctx, platform, TestChain, TestEscrowA, TestERC20, TestUserA, TestEscrowA, TestAmount1, 10, nil))

		engineCtx := engCtx(ctx, platform, TestUserA, 2, false)
		amtDec, err := types.ParseDecimalExplicit(TestAmount1, 78, 0)
		require.NoError(t, err)

		r, err := platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "bridge", []any{TestUserB, amtDec}, func(row *common.Row) error {
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		var bh [32]byte
		require.NoError(t, erc20shim.ForTestingFinalizeAndConfirmCurrentEpoch(ctx, platform, TestChain, TestEscrowA, 11, bh))

		instanceID := erc20shim.ForTestingGetInstanceID(TestChain, TestEscrowA)
		customRewardQuery := `
		{kwil_erc20_meta}SELECT count(*) FROM epoch_rewards r
		JOIN epochs e ON e.id = r.epoch_id
		WHERE e.instance_id = $id AND r.recipient = $user`
		var rows int
		err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB, customRewardQuery, map[string]any{
			"id":   instanceID,
			"user": ethcommon.HexToAddress(TestUserB).Bytes(),
		}, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return nil
			}
			rows = int(row.Values[0].(int64))
			return nil
		})
		require.NoError(t, err)
		require.Greater(t, rows, 0, "expected pending epoch reward for custom recipient")

		engineCtx = engCtx(ctx, platform, TestUserA, 3, false)
		rewardRows := 0
		r, err = platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "list_wallet_rewards", []any{TestUserB, false}, func(row *common.Row) error {
			rewardRows++
			recipientValue, ok := row.Values[4].(string)
			require.True(t, ok)
			require.True(t, strings.EqualFold(TestUserB, recipientValue))
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}
		require.Greater(t, rewardRows, 0, "user B should have at least one wallet reward after custom bridge")

		return nil
	})
}
