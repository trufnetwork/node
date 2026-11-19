//go:build kwiltest

package order_book

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// create_market refuses new markets on an action listed in retired_market_actions (migration
// 064). The table ships empty, and markets that already exist on a retired action still settle.

const retiredOldAbove = "price_above_threshold"

func TestRetiredMarketActions(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_RetiredMarketActions",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testRetiredActionRefusesNewMarkets(t),
			testRetiredActionExistingMarketStillSettles(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// Before the owner retires an action, markets on it are created as before. After, a new market on
// it is refused, while its outcome version and an action that is not retired still take markets.
func testRetiredActionRefusesNewMarkets(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		now := time.Now().Unix()
		at := now - 7200
		fx := setupOutcomeFixture(t, ctx, platform, "0x2222222222222222222222222222222222222222",
			"stretiredrefuses0000000000000000", now, []outcomeRecord{{at - 60, "102"}})

		fx.createMarket(t, platform, retiredOldAbove, fx.encodeOldThreshold(t, at, "101"))

		require.NoError(t, deployerExecute(ctx, platform,
			"INSERT INTO retired_market_actions (action_name) VALUES ('"+retiredOldAbove+"')"))

		err := fx.createMarketErr(t, platform, retiredOldAbove, fx.encodeOldThreshold(t, at, "100"))
		require.Error(t, err, "a new market on a retired action must be refused")
		require.Contains(t, err.Error(), "price_above_threshold is retired for new markets")

		above := outcomeArgs{action: outcomeAbove, at: at, window: outcomeDay, tolerance: 600}.withThreshold("101")
		fx.createMarket(t, platform, outcomeAbove, fx.encode(t, above))
		fx.createMarket(t, platform, "price_below_threshold", fx.encodeOldThreshold(t, at, "105"))
		return nil
	}
}

// A market created on an action before the owner retires it still captures and settles.
func testRetiredActionExistingMarketStillSettles(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		now := time.Now().Unix()
		at := now - 7200
		fx := setupOutcomeFixture(t, ctx, platform, "0x2222222222222222222222222222222222222222",
			"stretiredsettles0000000000000000", now, []outcomeRecord{{at - 60, "102"}})

		argsBytes := fx.encodeOldThreshold(t, at, "101")
		queryID := fx.createMarket(t, platform, retiredOldAbove, argsBytes)

		require.NoError(t, deployerExecute(ctx, platform,
			"INSERT INTO retired_market_actions (action_name) VALUES ('"+retiredOldAbove+"')"))

		fx.attest(t, platform, retiredOldAbove, argsBytes)
		settled, outcome, _ := fx.settle(t, platform, queryID, nil)
		require.True(t, settled, "the market must settle after its action is retired")
		require.NotNil(t, outcome)
		require.True(t, *outcome, "102 is above 101")
		return nil
	}
}

// encodeOldThreshold encodes price_above_threshold or price_below_threshold args (migration 040):
// data provider, stream, timestamp, threshold and a NULL frozen_at.
func (fx *outcomeFixture) encodeOldThreshold(t *testing.T, at int64, threshold string) []byte {
	t.Helper()
	encoded, err := tn_utils.EncodeActionArgs([]any{fx.dataProvider, fx.streamID, at, mustDecimal(t, threshold), nil})
	require.NoError(t, err)
	return encoded
}
