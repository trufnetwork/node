package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"

	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
)

// TestERC20BridgeInjectedTransferAffectsBalance uses the production code path via ordered-sync/evm-sync shims.
func TestERC20BridgeInjectedTransferAffectsBalance(t *testing.T) {
	// seed simple bridge action
	_, thisFile, _, _ := runtime.Caller(0)
	seedPath := filepath.Join(filepath.Dir(thisFile), "simple_mock.sql")

	seedScripts := migrations.GetSeedScriptPaths()
	seedScripts = append(seedScripts, seedPath)

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "erc20_bridge_injected_transfer_affects_balance",
		SeedScripts: seedScripts,
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				app := &common.App{DB: platform.DB, Engine: platform.Engine}
				// ensure ordered-sync topic and meta schema via helpers
				chain := "sepolia"
				escrow := "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
				erc20 := "0x2222222222222222222222222222222222222222"
				user := "0xabc0000000000000000000000000000000000001"
				value := "1000000000000000000"

				_, _ = erc20shim.ForTestingForceSyncInstance(ctx, app, chain, escrow, erc20, 18)

				// Inject a transfer: from user to escrow (lock/credit path)
				err := testerc20.InjectERC20Transfer(ctx, app, chain, escrow, erc20, user, escrow, value, 1, nil)
				require.NoError(t, err)

				// Query balance via action seeded in simple_mock.sql
				txCtx := &common.TxContext{
					Ctx:          ctx,
					BlockContext: &common.BlockContext{Height: 2},
					Signer:       platform.Deployer,
					Caller:       "0x0000000000000000000000000000000000000000",
					TxID:         platform.Txid(),
				}
				engCtx := &common.EngineContext{TxContext: txCtx}

				var got string
				r, err := platform.Engine.Call(engCtx, platform.DB, "", "get_balance", []any{user}, func(row *common.Row) error {
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
			},
		},
	}, &testutils.Options{Options: testutils.GetTestOptions()})
}
