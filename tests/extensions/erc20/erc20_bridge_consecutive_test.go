//go:build kwiltest

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
)

// TestERC20BridgeConsecutiveTests ensures that running multiple ERC20 bridge tests
// consecutively does not fail with "already active" errors due to improper cleanup.
func TestERC20BridgeConsecutiveTests(t *testing.T) {
	// Test 1: Run first ERC20 test
	t.Run("first_test", func(t *testing.T) {
		seedAndRun(t, "erc20_bridge_first", func(ctx context.Context, platform *kwilTesting.Platform) error {
			// Enable instance with alias for first test
			err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestChain)
			require.NoError(t, err)

			// Simple test: just call info() to verify the extension is working
			engineCtx := engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 1, false)
			var syncedResult bool
			res, err := platform.Engine.Call(engineCtx, platform.DB, TestChain, "info", []any{}, func(row *common.Row) error {
				if len(row.Values) >= 7 {
					syncedResult = row.Values[6].(bool)
				}
				return nil
			})
			require.NoError(t, err)
			if res != nil && res.Error != nil {
				return res.Error
			}
			require.True(t, syncedResult, "first test: instance should be synced")

			return nil
		})
	})

	// Test 2: Run second ERC20 test immediately after the first
	// This test will fail with "already active" if our cleanup doesn't work
	t.Run("second_test", func(t *testing.T) {
		seedAndRun(t, "erc20_bridge_second", func(ctx context.Context, platform *kwilTesting.Platform) error {
			// This should not fail with "already active" error due to proper cleanup
			err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, TestChain, TestEscrowA, TestERC20, 18, 60, TestChain)
			if err != nil {
				// If this fails, let's check the DB state to debug
				var isActive bool
				checkErr := platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB, `
					{kwil_erc20_meta}SELECT active FROM reward_instances WHERE id = $id
				`, map[string]any{"id": erc20shim.ForTestingGetInstanceID(TestChain, TestEscrowA)}, func(row *common.Row) error {
					if len(row.Values) == 1 {
						isActive = row.Values[0].(bool)
					}
					return nil
				})
				if checkErr == nil {
					t.Logf("DEBUG: Instance active in DB: %v", isActive)
				}
				require.NoError(t, err, "second test: should not fail with 'already active' error")
			}

			// Simple test: just call info() to verify the extension is working
			engineCtx := engCtx(ctx, platform, "0x0000000000000000000000000000000000000000", 1, false)
			var syncedResult bool
			res, err := platform.Engine.Call(engineCtx, platform.DB, TestChain, "info", []any{}, func(row *common.Row) error {
				if len(row.Values) >= 7 {
					syncedResult = row.Values[6].(bool)
				}
				return nil
			})
			require.NoError(t, err)
			if res != nil && res.Error != nil {
				return res.Error
			}
			require.True(t, syncedResult, "second test: instance should be synced")

			return nil
		})
	})
}
