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
)

// TestERC20BridgeActionsSmoke is a minimal actions smoke test.
// Goal: prove node can call named erc20 actions and receive sensible values.
// Scope: do not re-test Kwil internals; only verify building blocks needed by node logic.
//
// Test flow:
// 1) Initialize ERC20 instance via test shims (ForTestingForceSyncInstance + ForTestingInitializeExtension)
// 2) Call erc20.info() and assert chain/escrow/period/synced fields are set
// 3) Call erc20.decimals() and assert expected 18
// 4) Call erc20.scale_up("1") and expect 1000000000000000000 (1 * 10^18)
// 5) Call erc20.scale_down(1e18) and expect "1"
func TestERC20BridgeActionsSmoke(t *testing.T) {
	seedAndRun(t, "erc20_bridge_actions_smoke", func(ctx context.Context, platform *kwilTesting.Platform) error {

		// Singleton reset and initialize is handled by seedAndRun
		// Ensure active+synced in-memory
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestExtensionAlias)
		require.NoError(t, err)

		// Test erc20.info() action
		engineCtx := engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 1, false)

		var chainResult, escrowResult, periodResult string
		var syncedResult bool
		r, err := platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "info", []any{}, func(row *common.Row) error {
			chainResult = row.Values[0].(string)
			escrowResult = row.Values[1].(string)
			periodResult = row.Values[2].(string)
			syncedResult = row.Values[6].(bool)
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		// Assert info fields are properly set
		require.Equal(t, TestChain, chainResult, "info should return expected chain")
		require.Equal(t, TestEscrowA, escrowResult, "info should return expected escrow")
		require.NotEmpty(t, periodResult, "info should return non-empty period")
		require.True(t, syncedResult, "instance should be synced after force sync")

		// Test erc20.decimals() action
		var decimalsResult int64
		r, err = platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "decimals", []any{}, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return nil
			}
			decimalsResult = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		require.Equal(t, int64(18), decimalsResult, "decimals should return 18 for ERC20")

		// Test erc20.scale_up("1") action
		var scaledUpResult *types.Decimal
		r, err = platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "scale_up", []any{"1"}, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return nil
			}
			scaledUpResult = row.Values[0].(*types.Decimal)
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		require.Equal(t, TestAmount1, scaledUpResult.String(), "scale_up(1) should return 1e18")

		// Test erc20.scale_down(1e18) action
		var scaledDownResult string
		// scale_down meta handler expects: UUID + *types.Decimal
		// makeMetaHandler prepends &id, so we pass amount as *types.Decimal
		amountDecimal, err := types.ParseDecimalExplicit(TestAmount1, 78, 0)
		require.NoError(t, err)

		r, err = platform.Engine.Call(engineCtx, platform.DB, TestExtensionAlias, "scale_down", []any{amountDecimal}, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return nil
			}
			scaledDownResult = row.Values[0].(string)
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		if !(scaledDownResult == "1" || scaledDownResult == "1.000000000000000000") {
			require.Failf(t, "unexpected scale_down result", "got %s, expected 1 or 1.000000000000000000", scaledDownResult)
		}

		return nil
	})
}
