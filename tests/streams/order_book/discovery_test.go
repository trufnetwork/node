//go:build kwiltest

package order_book

import (
	"context"
	"fmt"
	"testing"
	"time"

	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestOrderBookDiscovery verifies that new markets populate denormalized columns
// and can be discovered via the indexed get_markets_by_stream view.
func TestOrderBookDiscovery(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_Discovery",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testDiscoveryWorkflow(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

func testDiscoveryWorkflow(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker
		lastBalancePointComponents = nil
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance for fees
		err = giveBalanceChainedComponents(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// 1. Create a Market
		dataProvider := "0x2222222222222222222222222222222222222222"
		streamID := "stdiscovery000000000000000000000" // Exactly 32 chars
		actionID := "price_above_threshold"
		argsBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}

		queryComponents, err := encodeQueryComponentsABI(dataProvider, streamID, actionID, argsBytes)
		require.NoError(t, err)

		settleTime := time.Now().Add(1 * time.Hour).Unix()
		var queryID int
		err = callCreateMarketWithComponents(ctx, platform, &userAddr, queryComponents, settleTime, int64(5), int64(100), func(row *common.Row) error {
			queryID = int(row.Values[0].(int64))
			return nil
		})
		require.NoError(t, err)

		// 2. Verify Structured Columns in DB
		// We'll query ob_queries directly using standard SQL to prove denormalization worked
		engineCtx := engCtx(ctx, platform, userAddr.Address(), 1)
		
		var dbProvider []byte
		var dbStreamID []byte
		var dbActionID string
		var dbQueryArgs []byte

		err = platform.Engine.Execute(engineCtx, platform.DB, 
			"SELECT data_provider, stream_id, action_id, query_args FROM ob_queries WHERE id = " + fmt.Sprintf("%d", queryID), nil, func(row *common.Row) error {
			require.NotNil(t, row.Values[0])
			v0, ok := row.Values[0].([]byte)
			require.True(t, ok, "expected []byte for data_provider")
			dbProvider = v0

			require.NotNil(t, row.Values[1])
			v1, ok := row.Values[1].([]byte)
			require.True(t, ok, "expected []byte for stream_id")
			dbStreamID = v1

			require.NotNil(t, row.Values[2])
			v2, ok := row.Values[2].(string)
			require.True(t, ok, "expected string for action_id")
			dbActionID = v2

			require.NotNil(t, row.Values[3])
			v3, ok := row.Values[3].([]byte)
			require.True(t, ok, "expected []byte for query_args")
			dbQueryArgs = v3
			return nil
		})
		require.NoError(t, err)

		require.Equal(t, gethCommon.HexToAddress(dataProvider).Bytes(), dbProvider, "data_provider should be denormalized")
		
		var expectedStreamID [32]byte
		copy(expectedStreamID[:], []byte(streamID))
		require.Equal(t, expectedStreamID[:], dbStreamID, "stream_id should be denormalized")
		
		require.Equal(t, actionID, dbActionID, "action_id should be denormalized")
		require.Equal(t, argsBytes, dbQueryArgs, "query_args should be denormalized")

		// 3. Test Discovery View
		var discoveryCount int
		// Parameters: $stream_id, $limit, $offset
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "get_markets_by_stream", []any{expectedStreamID[:], int64(10), int64(0)}, func(row *common.Row) error {
			discoveryCount++
			require.Equal(t, int64(queryID), row.Values[0].(int64), "discovered ID should match")
			require.Equal(t, actionID, row.Values[3].(string), "discovered action_id should match")
			return nil
		})
		require.NoError(t, err)
		require.Nil(t, res.Error)
		require.Equal(t, 1, discoveryCount, "should find exactly one market for this stream")

		// 4. Create another market for the same stream to test indexing/multiple results
		queryComponents2, err := encodeQueryComponentsABI(dataProvider, streamID, "price_below_threshold", []byte{0x00})
		require.NoError(t, err)
		
		err = callCreateMarketWithComponents(ctx, platform, &userAddr, queryComponents2, settleTime + 100, int64(5), int64(100), nil)
		require.NoError(t, err)

		discoveryCount = 0
		res, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_markets_by_stream", []any{expectedStreamID[:], int64(10), int64(0)}, func(row *common.Row) error {
			discoveryCount++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, discoveryCount, "should now find two markets for this stream")

		return nil
	}
}

// engCtx helper from other tests
func engCtx(ctx context.Context, platform *kwilTesting.Platform, caller string, height int64) *common.EngineContext {
	return &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    height,
				Timestamp: time.Now().Unix(),
			},
			Caller:        caller,
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		},
	}
}
