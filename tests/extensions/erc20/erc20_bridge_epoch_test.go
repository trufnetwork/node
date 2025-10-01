//go:build kwiltest

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"

	ethcommon "github.com/ethereum/go-ethereum/common"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
)

// TestERC20BridgeEpochFlow validates lock-and-issue, finalize and confirm using shims.
func TestERC20BridgeEpochFlow(t *testing.T) {
	seedAndRun(t, "erc20_bridge_epoch_flow", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Singleton reset and initialize is handled by seedAndRun

		// Use a different escrow for this test to avoid conflicts with seeded instance
		chain := "sepolia"
		escrow := "0xdddddddddddddddddddddddddddddddddddddddd"
		erc20 := "0x2222222222222222222222222222222222222222"
		user := "0xabc0000000000000000000000000000000000001"
		value := "500000000000000000" // 0.5

		// Enable instance with alias (includes alias creation, activation via period + rehydrate), idempotently and collision-free
		require.NoError(t, erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, chain, escrow, erc20, 18, 1, TestExtensionAlias))

		// Credit balance via injected transfer (simulates inbound deposit)
		require.NoError(t, testerc20.InjectERC20Transfer(ctx, platform, chain, escrow, erc20, user, user, value, 10, nil))

		// Lock and issue directly into epoch (simulate bridge request)
		require.NoError(t, erc20shim.ForTestingLockAndIssueDirect(ctx, platform, chain, escrow, user, value))

		// Assert pre-finalize: there should be a pending reward in epoch_rewards
		preQ := `
		{kwil_erc20_meta}SELECT count(*) FROM epoch_rewards`
		var preRows int
		err := platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB, preQ, nil, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return nil
			}
			preRows = int(row.Values[0].(int64))
			return nil
		})
		require.NoError(t, err)
		require.Greater(t, preRows, 0, "expected pending epoch reward before finalize")

		// Finalize and confirm via helper
		var bh [32]byte
		require.NoError(t, erc20shim.ForTestingFinalizeAndConfirmCurrentEpoch(ctx, platform, chain, escrow, 11, bh))

		// Query confirmed rewards directly
		q := `
		{kwil_erc20_meta}SELECT r.recipient, r.amount FROM epoch_rewards r
		JOIN epochs e ON e.id = r.epoch_id
		WHERE e.confirmed IS TRUE AND r.recipient = $recipient
		`
		var rows int
		ubytes := ethcommon.HexToAddress(user).Bytes()
		err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB, q, map[string]any{"recipient": ubytes}, func(row *common.Row) error {
			rows++
			return nil
		})
		require.NoError(t, err)
		require.Greater(t, rows, 0, "expected confirmed wallet reward rows")

		// Cleanup: Disable the test instance (tears down runtimes + resets singleton)
		require.NoError(t, erc20shim.ForTestingDisableInstance(ctx, platform, chain, escrow, TestExtensionAlias))

		return nil
	})
}
