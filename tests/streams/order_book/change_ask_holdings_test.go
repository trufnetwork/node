//go:build kwiltest

package order_book

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"

	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
)

// change_ask grows an ask by pulling the extra shares from the seller's
// holdings (the price = 0 row). A seller with no holdings row for the outcome
// has nothing to pull, so the change is rejected and every YES share keeps a
// NO share on the other side.
func TestChangeAskHoldings(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_05C_ChangeAskHoldings",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testChangeAskGrowRightAfterSplit(t),
			testChangeAskGrowAfterPartialFill(t),
			testChangeAskGrowFromHoldings(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// A split lists the whole NO leg, so the seller holds no NO shares and cannot
// grow the NO ask.
func testChangeAskGrowRightAfterSplit(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		maker := util.Unsafe_NewEthereumAddressFromString("0x7a00000000000000000000000000000000000011")
		require.NoError(t, giveBalanceChained(ctx, platform, maker.Address(), "500000000000000000000"))
		marketID := askHoldingsMarket(t, ctx, platform, &maker, "stchangeaskholdings0000000000001")

		// 100 YES held, 100 NO listed @44, no NO holdings row.
		require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, &maker, marketID, 56, 100))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, false, 0, 0)
		askHoldingsRequireParity(t, ctx, platform, marketID, 100, 100)

		err := callChangeAsk(ctx, platform, &maker, marketID, false, 44, 40, 1000)
		t.Logf("change_ask(NO, 44 -> 40, 1000) with no NO holdings row: err=%v", err)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Insufficient shares in holdings. Need 900 more shares, but only have 0 in holdings.")

		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, false, 44, 100)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, false, 40, 0)
		askHoldingsRequireParity(t, ctx, platform, marketID, 100, 100)
		return nil
	}
}

// The seller lists every YES share, a buyer takes 6, and the seller tries to
// move the ask back to its full size of 25. That is rejected; the seller
// re-quotes the 19 it has, burns its book against itself, and the market
// settles.
func testChangeAskGrowAfterPartialFill(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		maker := util.Unsafe_NewEthereumAddressFromString("0x7a00000000000000000000000000000000000021")
		taker := util.Unsafe_NewEthereumAddressFromString("0x7a00000000000000000000000000000000000022")
		marketID, settleTime, helper := askHoldingsSettleableMarket(t, ctx, platform, &maker, &taker, "stchangeaskholdings0000000000002")

		// 25 YES held and 25 NO held, then all 25 YES listed @14.
		require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, &maker, marketID, 1, 25))
		require.NoError(t, callCancelOrder(ctx, platform, &maker, marketID, false, 99))
		require.NoError(t, callPlaceSellOrder(ctx, platform, &maker, marketID, true, 14, 25))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 0, 0)

		// The buyer takes 6 of the 25.
		require.NoError(t, callPlaceBuyOrder(ctx, platform, &taker, marketID, true, 14, 6))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 14, 19)
		askHoldingsRequireParity(t, ctx, platform, marketID, 25, 25)

		err := callChangeAsk(ctx, platform, &maker, marketID, true, 14, 13, 25)
		t.Logf("change_ask(YES, 14 -> 13, 25) with 19 listed and no YES holdings row: err=%v", err)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Insufficient shares in holdings. Need 6 more shares, but only have 0 in holdings.")
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 14, 19)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 13, 0)
		askHoldingsRequireParity(t, ctx, platform, marketID, 25, 25)

		// 19 YES @10 against 25 NO @90 burns 19 pairs and leaves 6 NO listed.
		require.NoError(t, callChangeAsk(ctx, platform, &maker, marketID, true, 14, 10, 19))
		require.NoError(t, callPlaceSellOrder(ctx, platform, &maker, marketID, false, 90, 25))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 10, 0)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, false, 90, 6)
		askHoldingsRequireParity(t, ctx, platform, marketID, 6, 6)

		settleErr := askHoldingsSettle(t, ctx, platform, helper, marketID, settleTime)
		t.Logf("settle_market: err=%v", settleErr)
		require.NoError(t, settleErr)

		var settled bool
		var winningOutcome *bool
		err = platform.Engine.Execute(helper.NewEngineContext(), platform.DB,
			`SELECT settled, winning_outcome FROM ob_queries WHERE id = $id`,
			map[string]any{"id": marketID},
			func(row *common.Row) error {
				settled = row.Values[0].(bool)
				if row.Values[1] != nil {
					outcome := row.Values[1].(bool)
					winningOutcome = &outcome
				}
				return nil
			})
		require.NoError(t, err)
		require.True(t, settled)
		require.NotNil(t, winningOutcome)
		require.True(t, *winningOutcome)

		positions, err := getPositions(ctx, platform, marketID)
		require.NoError(t, err)
		require.Empty(t, positions)
		return nil
	}
}

// Every branch of the holdings check: too few held, more than enough held,
// exactly enough held (the row is removed), then nothing held.
func testChangeAskGrowFromHoldings(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		maker := util.Unsafe_NewEthereumAddressFromString("0x7a00000000000000000000000000000000000031")
		taker := util.Unsafe_NewEthereumAddressFromString("0x7a00000000000000000000000000000000000032")
		require.NoError(t, giveBalanceChained(ctx, platform, maker.Address(), "500000000000000000000"))
		require.NoError(t, giveBalanceChained(ctx, platform, taker.Address(), "500000000000000000000"))
		marketID := askHoldingsMarket(t, ctx, platform, &maker, "stchangeaskholdings0000000000003")

		// 3 YES held, 22 listed @14, then the buyer takes 6: 16 listed.
		require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, &maker, marketID, 1, 25))
		require.NoError(t, callCancelOrder(ctx, platform, &maker, marketID, false, 99))
		require.NoError(t, callPlaceSellOrder(ctx, platform, &maker, marketID, true, 14, 22))
		require.NoError(t, callPlaceBuyOrder(ctx, platform, &taker, marketID, true, 14, 6))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 0, 3)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 14, 16)

		err := callChangeAsk(ctx, platform, &maker, marketID, true, 14, 13, 24)
		t.Logf("change_ask(YES, 14 -> 13, 24) with 16 listed and 3 held: err=%v", err)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Insufficient shares in holdings. Need 8 more shares, but only have 3 in holdings.")
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 14, 16)
		askHoldingsRequireParity(t, ctx, platform, marketID, 25, 25)

		require.NoError(t, callChangeAsk(ctx, platform, &maker, marketID, true, 14, 13, 18))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 0, 1)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 13, 18)
		askHoldingsRequireParity(t, ctx, platform, marketID, 25, 25)

		require.NoError(t, callChangeAsk(ctx, platform, &maker, marketID, true, 13, 12, 19))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 0, 0)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 12, 19)
		askHoldingsRequireParity(t, ctx, platform, marketID, 25, 25)

		err = callChangeAsk(ctx, platform, &maker, marketID, true, 12, 11, 20)
		t.Logf("change_ask(YES, 12 -> 11, 20) with 19 listed and no YES holdings row: err=%v", err)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Insufficient shares in holdings. Need 1 more shares, but only have 0 in holdings.")
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 12, 19)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &maker, true, 11, 0)
		askHoldingsRequireParity(t, ctx, platform, marketID, 25, 25)
		return nil
	}
}

// askHoldingsMarket creates a market that settles in a day. It has no
// attestation, so it is only for trading.
func askHoldingsMarket(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, maker *util.EthereumAddress, streamID string) int {
	queryComponents, err := encodeQueryComponentsForTests(maker.Address(), streamID, "get_record", []byte{0x01})
	require.NoError(t, err)

	var marketID int64
	err = callCreateMarket(ctx, platform, maker, queryComponents, time.Now().Add(24*time.Hour).Unix(), 5, 1,
		func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
	require.NoError(t, err)
	return int(marketID)
}

// askHoldingsSettleableMarket creates a market an hour out with a signed YES
// attestation taken at its settle time, so settle_market can run on it. The
// maker is the deployer and data provider; maker and taker are funded to trade.
func askHoldingsSettleableMarket(t *testing.T, ctx context.Context, platform *kwilTesting.Platform,
	maker, taker *util.EthereumAddress, streamID string) (int, int64, *attestationTests.AttestationTestHelper) {
	platform.Deployer = maker.Bytes()
	helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)

	require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
	require.NoError(t, setup.CreateDataProvider(ctx, platform, maker.Address()))
	require.NoError(t, giveBalanceChained(ctx, platform, maker.Address(), "500000000000000000000"))
	require.NoError(t, giveBalanceChained(ctx, platform, taker.Address(), "500000000000000000000"))

	dataProvider := maker.Address()
	engineCtx := helper.NewEngineContext()
	_, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
		[]any{streamID, "primitive"}, nil)
	require.NoError(t, err)

	yes, err := kwilTypes.ParseDecimalExplicit("1.000000000000000000", 36, 18)
	require.NoError(t, err)
	_, err = platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
		[]any{[]string{dataProvider}, []string{streamID}, []int64{1000}, []*kwilTypes.Decimal{yes}}, nil)
	require.NoError(t, err)

	argsBytes, err := tn_utils.EncodeActionArgs([]any{dataProvider, streamID, int64(1500), nil, false})
	require.NoError(t, err)

	settleTime := time.Now().Add(1 * time.Hour).Unix()
	var requestTxID string
	engineCtx = helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = settleTime // settle_market only takes a capture from settle_time on
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
		[]any{dataProvider, streamID, "get_last_record", argsBytes, false, nil},
		func(row *common.Row) error {
			requestTxID = row.Values[0].(string)
			return nil
		})
	require.NoError(t, err)
	require.Nil(t, res.Error)
	helper.SignAttestation(requestTxID)

	queryComponents, err := encodeQueryComponentsForTests(dataProvider, streamID, "get_last_record", argsBytes)
	require.NoError(t, err)

	var marketID int
	engineCtx = helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = time.Now().Unix()
	res, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
		[]any{testExtensionName, queryComponents, settleTime, int64(5), int64(1)},
		func(row *common.Row) error {
			marketID = int(row.Values[0].(int64))
			return nil
		})
	require.NoError(t, err)
	require.Nil(t, res.Error)
	require.Greater(t, marketID, 0)
	return marketID, settleTime, helper
}

// askHoldingsSettle calls settle_market just after the market's settle time.
func askHoldingsSettle(t *testing.T, ctx context.Context, platform *kwilTesting.Platform,
	helper *attestationTests.AttestationTestHelper, marketID int, settleTime int64) error {
	engineCtx := helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = settleTime + 1
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market", []any{marketID}, nil)
	require.NoError(t, err)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

// askHoldingsRequireOrder checks the wallet's position at one price; 0 means
// no row (price 0 is holdings).
func askHoldingsRequireOrder(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, marketID int,
	wallet *util.EthereumAddress, outcome bool, price int, want int64) {
	amount, _, err := getOrderDetails(ctx, platform, marketID, wallet.Address(), outcome, price)
	require.NoError(t, err)
	require.Equal(t, want, amount, "outcome=%v price=%d", outcome, price)
}

// askHoldingsRequireParity checks the share totals validate_market_collateral
// reports and that it calls them balanced exactly when they are equal.
func askHoldingsRequireParity(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, marketID int, wantTrue, wantFalse int64) {
	validBinaries, _, totalTrue, totalFalse, _, _, _ := validateMarket(t, ctx, platform, marketID)
	t.Logf("validate_market_collateral: valid_token_binaries=%v total_true=%d total_false=%d", validBinaries, totalTrue, totalFalse)
	require.Equal(t, wantTrue, totalTrue, "TRUE shares")
	require.Equal(t, wantFalse, totalFalse, "FALSE shares")
	require.Equal(t, totalTrue == totalFalse, validBinaries)
}
