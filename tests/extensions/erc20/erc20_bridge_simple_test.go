package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
)

// Minimal integration to validate that the ERC-20 bridge extension can be initialized
// via USE and that balance() can be called (expected 0 without any deposits).
// Seed script: tests/extensions/erc20/simple_mock.sql
// Reference: Kwil ERC-20 Bridge Usage (balance/bridge): https://docs.kwil.com/docs/erc20-bridge/usage#bridge
func TestERC20BridgeSimpleBalance(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "erc20_bridge_simple_balance",
		SeedScripts: []string{"./tests/extensions/erc20/simple_mock.sql"},
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				// Arbitrary wallet address to query (no prior deposits => expected 0)
				wallet := "0x1111111111111111111111111111111111110001"

				txCtx := &common.TxContext{
					Ctx:          ctx,
					BlockContext: &common.BlockContext{Height: 1},
					Signer:       platform.Deployer, // signer identity; not material for a read-only balance
					Caller:       "0x0000000000000000000000000000000000000000",
					TxID:         platform.Txid(),
				}
				engCtx := &common.EngineContext{TxContext: txCtx}

				// Call our seeded action which proxies to sepolia_bridge.balance($wallet)
				// Created in simple_mock.sql
				var got string
				r, err := platform.Engine.Call(engCtx, platform.DB, "", "get_balance", []any{wallet}, func(row *common.Row) error {
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

				// Expect 0 for no prior deposits
				require.Equal(t, "0", got, "expected zero balance for fresh wallet without deposit events")
				return nil
			},
		},
	}, &testutils.Options{Options: testutils.GetTestOptions()})
}
