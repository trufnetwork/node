//go:build kwiltest

package tests

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"

	ethcommon "github.com/ethereum/go-ethereum/common"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	orderedsync "github.com/trufnetwork/kwil-db/node/exts/ordered-sync"
)

// TestERC20BridgeEpochFlow validates lock-and-issue, finalize and confirm using shims.
func TestERC20BridgeEpochFlow(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	seedPath := filepath.Join(filepath.Dir(thisFile), "simple_mock.sql")

	seedScripts := migrations.GetSeedScriptPaths()
	seedScripts = append(seedScripts, seedPath)

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "erc20_bridge_epoch_flow",
		SeedScripts: seedScripts,
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				app := &common.App{DB: platform.DB, Engine: platform.Engine}
				// ensure ordered-sync namespace
				require.NoError(t, orderedsync.ForTestingEnsureNamespace(ctx, app))

				// Note: Global state reset should be handled by test framework, not individual tests
				chain := "sepolia"
				escrow := "0xdddddddddddddddddddddddddddddddddddddddd"
				erc20 := "0x2222222222222222222222222222222222222222"
				user := "0xabc0000000000000000000000000000000000001"
				value := "500000000000000000" // 0.5

				// Ensure instance synced (register topic and create DB instance/epoch if missing)
				_, err := erc20shim.ForTestingForceSyncInstance(ctx, app, chain, escrow, erc20, 18)
				require.NoError(t, err)

				// Make distribution period small for test determinism
				require.NoError(t, erc20shim.ForTestingSetDistributionPeriod(ctx, app, chain, escrow, 1))

				// Credit balance via injected transfer (simulates inbound deposit)
				require.NoError(t, testerc20.InjectERC20Transfer(ctx, app, chain, escrow, erc20, user, escrow, value, 10, nil))

				// Lock and issue directly into epoch (simulate bridge request)
				require.NoError(t, erc20shim.ForTestingLockAndIssueDirect(ctx, app, chain, escrow, user, value))

				// Assert pre-finalize: there should be a pending reward in epoch_rewards
				preQ := `
				{kwil_erc20_meta}SELECT count(*) FROM epoch_rewards`
				var preRows int
				err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB, preQ, nil, func(row *common.Row) error {
					if len(row.Values) != 1 {
						return nil
					}
					preRows = int(row.Values[0].(int64))
					return nil
				})
				require.NoError(t, err)
				require.Greater(t, preRows, 0, "expected pending epoch reward before finalize")

				// Finalize current epoch and create next
				var bh [32]byte
				require.NoError(t, erc20shim.ForTestingFinalizeCurrentEpoch(ctx, app, chain, escrow, 11, bh))

				// Confirm finalized epochs
				require.NoError(t, erc20shim.ForTestingConfirmAllFinalizedEpochs(ctx, app, chain, escrow))

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

				return nil
			},
		},
	}, &testutils.Options{Options: testutils.GetTestOptions()})
}
