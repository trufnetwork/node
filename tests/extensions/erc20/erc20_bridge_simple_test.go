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
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
)

// TestERC20BridgeSimpleBalance validates ERC-20 bridge extension initialization and balance() call.
// This test requires the erc20_bridge extension to be available in the test environment.
//
// BLOCKER: The erc20_bridge extension is not currently available in the test environment.
// When this extension becomes available, this test will:
// 1. Execute the USE erc20_bridge statement from simple_mock.sql
// 2. Call the get_balance action which proxies to sepolia_bridge.balance()
// 3. Verify it returns "0" for a wallet with no deposit events
//
// Until then, this test will fail with a clear error about the missing extension.
func TestERC20BridgeSimpleBalance(t *testing.T) {
	// Compute absolute path to the seed script relative to this test file
	_, thisFile, _, _ := runtime.Caller(0)
	seedPath := filepath.Join(filepath.Dir(thisFile), "simple_mock.sql")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "erc20_bridge_simple_balance",
		SeedScripts: []string{seedPath}, // This will fail if erc20_bridge extension doesn't exist
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				// Arbitrary wallet address to query (no prior deposits => expected 0)
				wallet := "0x1111111111111111111111111111111111110001"

				txCtx := &common.TxContext{
					Ctx:          ctx,
					BlockContext: &common.BlockContext{Height: 1},
					Signer:       platform.Deployer,
					Caller:       "0x0000000000000000000000000000000000000000",
					TxID:         platform.Txid(),
				}
				engCtx := &common.EngineContext{TxContext: txCtx}

				// Call our seeded action which proxies to sepolia_bridge.balance($wallet)
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

// TestERC20BridgeMockListener tests our ERC-20 bridge testing helpers with a mock listener.
// This doesn't require the real erc20_bridge extension but validates our test infrastructure.
func TestERC20BridgeMockListener(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "erc20_bridge_mock_listener",
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				// Create mock listener that broadcasts ERC-20 transfer events
				mock := testutils.MockERC20Listener([]string{"erc20_transfer"})

				// Configure ERC-20 bridge with mock listener
				cfg := testutils.NewERC20BridgeConfig().
					WithRPC("sepolia", "wss://mock-sepolia.com").
					WithSigner("test_bridge", "/dev/null").
					WithMockListener("evm_sync", mock).
					WithAutoStart()

				// Setup test environment
				helper, err := testerc20.SetupERC20BridgeTest(ctx, platform, cfg)
				require.NoError(t, err)
				defer helper.Cleanup()

				// Start listener and verify it works
				require.NoError(t, helper.StartERC20Listener(ctx))

				// Verify listener is running
				require.True(t, helper.IsListenerRunning(), "listener should be running")

				// Stop listener
				require.NoError(t, helper.StopERC20Listener())

				// Verify listener stopped
				require.False(t, helper.IsListenerRunning(), "listener should have stopped")

				return nil
			},
		},
	}, &testutils.Options{Options: testutils.GetTestOptions()})
}
