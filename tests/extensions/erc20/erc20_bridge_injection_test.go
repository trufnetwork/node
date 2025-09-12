//go:build kwiltest

package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"

	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
)

// TestERC20BridgeInjectedTransferAffectsBalance uses the production code path via ordered-sync/evm-sync shims.
func TestERC20BridgeInjectedTransferAffectsBalance(t *testing.T) {
	seedAndRun(t, "erc20_bridge_injected_transfer_affects_balance", func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use a different escrow for this test to avoid conflicts with seeded instance
		chain := "sepolia"
		escrow := "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
		erc20 := "0x2222222222222222222222222222222222222222"
		user := "0xabc0000000000000000000000000000000000001"
		value := "1000000000000000000"

		// Enable instance with alias for injection test
		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, chain, escrow, erc20, 18, 60, TestChain)
		require.NoError(t, err)

		// Cleanup: Deactivate the test instance
		t.Cleanup(func() {
			erc20shim.ForTestingDisableInstance(ctx, platform, chain, escrow, TestChain)
		})

		// Inject a transfer: from user to escrow (lock/credit path)
		err = testerc20.InjectERC20Transfer(ctx, platform, chain, escrow, erc20, user, escrow, value, 1, nil)
		require.NoError(t, err)

		// Query balance via the test alias
		txCtx := &common.TxContext{
			Ctx:          ctx,
			BlockContext: &common.BlockContext{Height: 2},
			Signer:       platform.Deployer,
			Caller:       "0x0000000000000000000000000000000000000000",
			TxID:         platform.Txid(),
		}
		engCtx := &common.EngineContext{TxContext: txCtx}

		var got string
		r, err := platform.Engine.Call(engCtx, platform.DB, TestChain, "balance", []any{user}, func(row *common.Row) error {
			if len(row.Values) != 1 {
				return fmt.Errorf("expected 1 column, got %d", len(row.Values))
			}
			got = fmt.Sprintf("%v", row.Values[0])
			return nil
		})
		require.NoError(t, err)
		if r != nil && r.Error != nil {
			return r.Error
		}

		require.Equal(t, value, got, "expected balance to reflect injected transfer amount")

		return nil
	})
}
