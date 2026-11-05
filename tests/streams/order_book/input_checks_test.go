//go:build kwiltest

package order_book

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Input checks on the order book: a NULL argument gets its own error, one
// order makes at most 100 fills per match type, settlement checks the vault
// for markets that only hold buy orders, and the batch unlock helper checks
// what it is given.
func TestOrderInputChecks(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_InputChecks",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testOrderActionsRefuseNullArguments(t),
			testCreateMarketRefusesNullArguments(t),
			testDirectMatchFillBudget(t),
			testMintMatchFillBudget(t),
			testBurnMatchFillBudget(t),
			testMintWaitsForCrossingBook(t),
			testSettleChecksVaultForOpenBuys(t),
			testSettleRefundsOpenBuysWhenVaultCovers(t),
			testBatchUnlockChecksInputs(t),
			testLockCollateralHelperDropped(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// Every required argument of the order actions is refused when it is NULL,
// with an error that names it, and nothing in the book or the wallet moves.
func testOrderActionsRefuseNullArguments(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		maker := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000001")
		require.NoError(t, giveBalanceChained(ctx, platform, maker.Address(), "500000000000000000000"))
		marketID := askHoldingsMarket(t, ctx, platform, &maker, "stinputchecksnull000000000000001")

		// Something for the cancel and change calls to point at:
		// 10 YES held, 10 NO listed @44 and a YES bid for 10 @40.
		require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, &maker, marketID, 56, 10))
		require.NoError(t, callPlaceBuyOrder(ctx, platform, &maker, marketID, true, 40, 10))

		positionsBefore, err := getPositions(ctx, platform, marketID)
		require.NoError(t, err)
		balanceBefore, err := getUSDCBalance(ctx, platform, maker.Address())
		require.NoError(t, err)

		cases := []struct {
			action string
			args   []any
			want   string
		}{
			{"place_buy_order", []any{nil, true, 50, int64(10)}, "query_id is required"},
			{"place_buy_order", []any{marketID, true, nil, int64(10)}, "price is required"},
			{"place_buy_order", []any{marketID, true, 50, nil}, "amount is required"},
			{"place_sell_order", []any{marketID, true, nil, int64(5)}, "price is required"},
			{"place_sell_order", []any{marketID, true, 60, nil}, "amount is required"},
			{"place_split_limit_order", []any{marketID, nil, int64(5)}, "true_price is required"},
			{"place_split_limit_order", []any{marketID, 56, nil}, "amount is required"},
			{"cancel_order", []any{nil, true, -40}, "query_id is required"},
			{"cancel_order", []any{marketID, true, nil}, "price is required"},
			{"change_bid", []any{marketID, true, nil, -45, int64(10)}, "old_price is required"},
			{"change_bid", []any{marketID, true, -40, nil, int64(10)}, "new_price is required"},
			{"change_bid", []any{marketID, true, -40, -45, nil}, "new_amount is required"},
			{"change_ask", []any{marketID, false, nil, 48, int64(10)}, "old_price is required"},
			{"change_ask", []any{marketID, false, 44, nil, int64(10)}, "new_price is required"},
			{"change_ask", []any{marketID, false, 44, 48, nil}, "new_amount is required"},
			{"settle_market", []any{nil}, "query_id is required"},
		}
		for _, tc := range cases {
			err := callActionQueries(ctx, platform, &maker, tc.action, tc.args, nil)
			t.Logf("%s %v: err=%v", tc.action, tc.args, err)
			require.Error(t, err, "%s %v", tc.action, tc.args)
			require.Contains(t, err.Error(), tc.want, "%s %v", tc.action, tc.args)
		}

		positionsAfter, err := getPositions(ctx, platform, marketID)
		require.NoError(t, err)
		require.Equal(t, positionsBefore, positionsAfter)
		balanceAfter, err := getUSDCBalance(ctx, platform, maker.Address())
		require.NoError(t, err)
		require.Equal(t, balanceBefore.String(), balanceAfter.String())
		return nil
	}
}

// create_market refuses a NULL parameter before it charges the creation fee.
func testCreateMarketRefusesNullArguments(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		creator := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000002")
		require.NoError(t, giveBalanceChained(ctx, platform, creator.Address(), "500000000000000000000"))
		queryComponents, err := encodeQueryComponentsForTests(creator.Address(), "stinputchecksnull000000000000002", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		trufBefore, err := getBalance(ctx, platform, creator.Address())
		require.NoError(t, err)

		cases := []struct {
			args []any
			want string
		}{
			{[]any{testExtensionName, nil, settleTime, int64(5), int64(1)}, "query_components is required"},
			{[]any{testExtensionName, queryComponents, nil, int64(5), int64(1)}, "settle_time is required"},
			{[]any{testExtensionName, queryComponents, settleTime, nil, int64(1)}, "max_spread is required"},
			{[]any{testExtensionName, queryComponents, settleTime, int64(5), nil}, "min_order_size is required"},
		}
		for _, tc := range cases {
			err := callActionQueries(ctx, platform, &creator, "create_market", tc.args, nil)
			t.Logf("create_market %v: err=%v", tc.args[1:], err)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		}

		trufAfter, err := getBalance(ctx, platform, creator.Address())
		require.NoError(t, err)
		require.Equal(t, trufBefore.String(), trufAfter.String(), "no creation fee is charged")
		require.Zero(t, countRows(t, ctx, platform, "SELECT 1 FROM ob_queries", nil))
		return nil
	}
}

// One sell that crosses 101 resting bids fills 100 of them; the bid left
// over stays on the book and the next order in the market fills it.
func testDirectMatchFillBudget(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		seller := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000010")
		buyerA := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000011")
		buyerB := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000012")
		require.NoError(t, giveBalanceChained(ctx, platform, seller.Address(), "500000000000000000000"))
		require.NoError(t, giveUSDCBalanceChained(ctx, platform, buyerA.Address(), "100000000000000000000"))
		require.NoError(t, giveUSDCBalanceChained(ctx, platform, buyerB.Address(), "100000000000000000000"))
		marketID := askHoldingsMarket(t, ctx, platform, &seller, "stfillbudgetdirect00000000000001")
		place := orderPlacer(t, ctx, platform)

		// 101 one-share YES bids: buyer A at every price from 99 down to 1,
		// then buyer B at 2 and at 1, which puts B's bid @1 last in line.
		for price := 99; price >= 1; price-- {
			place(&buyerA, "place_buy_order", marketID, true, price, int64(1))
		}
		place(&buyerB, "place_buy_order", marketID, true, 2, int64(1))
		place(&buyerB, "place_buy_order", marketID, true, 1, int64(1))
		require.Equal(t, 101, countRestingOrders(t, ctx, platform, marketID, true, true))

		// The seller lists 101 YES @1, which crosses every bid.
		place(&seller, "place_split_limit_order", marketID, 50, int64(101))
		place(&seller, "place_sell_order", marketID, true, 1, int64(101))

		require.Equal(t, 1, countRestingOrders(t, ctx, platform, marketID, true, true))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &buyerB, true, -1, 1)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &seller, true, 1, 1)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &buyerA, true, 0, 99)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &buyerB, true, 0, 1)
		requireMarketBalanced(t, ctx, platform, marketID)

		// The next YES order matches the pair that was left crossing.
		place(&buyerA, "place_buy_order", marketID, true, 1, int64(1))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &buyerB, true, -1, 0)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &buyerB, true, 0, 2)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &seller, true, 1, 0)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &buyerA, true, -1, 1)
		requireMarketBalanced(t, ctx, platform, marketID)
		return nil
	}
}

// A YES bid that can mint against 101 NO bids mints 100 pairs; the next NO
// bid at that price mints the last one.
func testMintMatchFillBudget(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		yesBuyer := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000020")
		require.NoError(t, giveBalanceChained(ctx, platform, yesBuyer.Address(), "500000000000000000000"))
		marketID := askHoldingsMarket(t, ctx, platform, &yesBuyer, "stfillbudgetmint0000000000000001")
		noBuyers := fundedWallets(t, ctx, platform, 0x7d, 102, "1000000000000000000")
		place := orderPlacer(t, ctx, platform)

		for i := 0; i < 101; i++ {
			place(&noBuyers[i], "place_buy_order", marketID, false, 40, int64(1))
		}
		place(&yesBuyer, "place_buy_order", marketID, true, 60, int64(101))

		askHoldingsRequireOrder(t, ctx, platform, marketID, &yesBuyer, true, -60, 1)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &yesBuyer, true, 0, 100)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &noBuyers[99], false, 0, 1)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &noBuyers[100], false, -40, 1)
		require.Equal(t, 1, countRestingOrders(t, ctx, platform, marketID, false, true))
		requireMarketBalanced(t, ctx, platform, marketID)

		place(&noBuyers[101], "place_buy_order", marketID, false, 40, int64(1))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &yesBuyer, true, -60, 0)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &yesBuyer, true, 0, 101)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &noBuyers[100], false, -40, 0)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &noBuyers[100], false, 0, 1)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &noBuyers[101], false, -40, 1)
		requireMarketBalanced(t, ctx, platform, marketID)
		return nil
	}
}

// A YES ask that can burn against 101 NO asks burns 100 pairs; the next NO
// ask at that price burns the last one.
func testBurnMatchFillBudget(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		seller := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000030")
		require.NoError(t, giveBalanceChained(ctx, platform, seller.Address(), "500000000000000000000"))
		marketID := askHoldingsMarket(t, ctx, platform, &seller, "stfillbudgetburn0000000000000001")
		noSellers := fundedWallets(t, ctx, platform, 0x7e, 102, "2000000000000000000")
		place := orderPlacer(t, ctx, platform)

		// Each wallet splits one pair @60: 1 YES held, 1 NO listed @40.
		for i := 0; i < 101; i++ {
			place(&noSellers[i], "place_split_limit_order", marketID, 60, int64(1))
		}
		// The seller's own NO leg is listed @90 and matches nothing.
		place(&seller, "place_split_limit_order", marketID, 10, int64(101))
		place(&seller, "place_sell_order", marketID, true, 60, int64(101))

		askHoldingsRequireOrder(t, ctx, platform, marketID, &seller, true, 60, 1)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &noSellers[99], false, 40, 0)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &noSellers[100], false, 40, 1)
		requireMarketBalanced(t, ctx, platform, marketID)

		place(&noSellers[101], "place_split_limit_order", marketID, 60, int64(1))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &seller, true, 60, 0)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &noSellers[100], false, 40, 0)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &noSellers[101], false, 40, 1)
		requireMarketBalanced(t, ctx, platform, marketID)
		return nil
	}
}

// A YES bid that crosses 101 asks fills 100 of them directly. The ask left
// over still crosses the bid, so the call does not mint the rest of the bid
// against a NO bid at the complementary price. The next YES order fills the
// crossing pair at the ask's price.
func testMintWaitsForCrossingBook(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		maker := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000060")
		yesBuyer := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000061")
		noBuyer := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000062")
		lateBuyer := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000063")
		require.NoError(t, giveBalanceChained(ctx, platform, maker.Address(), "500000000000000000000"))
		require.NoError(t, giveUSDCBalanceChained(ctx, platform, yesBuyer.Address(), "100000000000000000000"))
		require.NoError(t, giveUSDCBalanceChained(ctx, platform, noBuyer.Address(), "10000000000000000000"))
		require.NoError(t, giveUSDCBalanceChained(ctx, platform, lateBuyer.Address(), "10000000000000000000"))
		marketID := askHoldingsMarket(t, ctx, platform, &maker, "stfillbudgetcross000000000000001")
		sellers := fundedWallets(t, ctx, platform, 0x7f, 101, "1000000000000000000")
		place := orderPlacer(t, ctx, platform)

		// 101 one-share YES asks @40. Each split lists its NO share @90, which
		// no order below reaches.
		for i := range sellers {
			place(&sellers[i], "place_split_limit_order", marketID, 10, int64(1))
			place(&sellers[i], "place_sell_order", marketID, true, 40, int64(1))
		}
		require.Equal(t, 101, countRestingOrders(t, ctx, platform, marketID, true, false))

		// A NO bid @50 that a YES bid @50 could mint against.
		place(&noBuyer, "place_buy_order", marketID, false, 50, int64(1))

		// The YES bid @50 for 101 shares fills 100 asks and keeps one share open.
		place(&yesBuyer, "place_buy_order", marketID, true, 50, int64(101))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &yesBuyer, true, 0, 100)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &yesBuyer, true, -50, 1)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &noBuyer, false, -50, 1)
		require.Equal(t, 1, countRestingOrders(t, ctx, platform, marketID, true, false))
		requireMarketBalanced(t, ctx, platform, marketID)

		// The next YES order matches the crossing pair at the ask's price.
		place(&lateBuyer, "place_buy_order", marketID, true, 1, int64(1))
		askHoldingsRequireOrder(t, ctx, platform, marketID, &yesBuyer, true, -50, 0)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &yesBuyer, true, 0, 101)
		askHoldingsRequireOrder(t, ctx, platform, marketID, &noBuyer, false, -50, 1)
		require.Equal(t, 0, countRestingOrders(t, ctx, platform, marketID, true, false))
		requireMarketBalanced(t, ctx, platform, marketID)
		return nil
	}
}

// A market that holds only buy orders is refunded out of the shared vault, so
// settlement checks the vault for it too. With the vault $5 short of what the
// open markets are owed, settling it is refused and nothing moves.
func testSettleChecksVaultForOpenBuys(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		maker := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000040")
		taker := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000041")
		marketID, settleTime, helper, otherMarket := openBuysMarkets(t, ctx, platform, &maker, &taker, "stsettleopenbuys0000000000000001")

		// Pay $5 out of the vault: it now holds $9 against $14 owed.
		recipient := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000049")
		require.NoError(t, privateCall(ctx, platform, &maker, "ob_unlock_collateral",
			[]any{testExtensionName, recipient.Address(), numeric78(t, "5000000000000000000")}))
		_, validCollateral, _, _, vault, expected, _ := validateMarket(t, ctx, platform, marketID)
		t.Logf("vault=%s expected=%s", vault, expected)
		require.False(t, validCollateral)

		takerBefore, err := getUSDCBalance(ctx, platform, taker.Address())
		require.NoError(t, err)

		err = askHoldingsSettle(t, ctx, platform, helper, marketID, settleTime)
		t.Logf("settle_market on a market with only open buys and a short vault: err=%v", err)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Vault collateral mismatch")

		askHoldingsRequireOrder(t, ctx, platform, marketID, &taker, true, -40, 10)
		takerAfter, err := getUSDCBalance(ctx, platform, taker.Address())
		require.NoError(t, err)
		require.Equal(t, takerBefore.String(), takerAfter.String())
		require.Equal(t, int64(10), otherMarketShares(t, ctx, platform, otherMarket))
		return nil
	}
}

// With the vault whole, the same market settles and the bid is refunded.
func testSettleRefundsOpenBuysWhenVaultCovers(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		maker := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000042")
		taker := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000043")
		marketID, settleTime, helper, _ := openBuysMarkets(t, ctx, platform, &maker, &taker, "stsettleopenbuys0000000000000003")

		takerBefore, err := getUSDCBalance(ctx, platform, taker.Address())
		require.NoError(t, err)

		require.NoError(t, askHoldingsSettle(t, ctx, platform, helper, marketID, settleTime))

		askHoldingsRequireOrder(t, ctx, platform, marketID, &taker, true, -40, 0)
		takerAfter, err := getUSDCBalance(ctx, platform, taker.Address())
		require.NoError(t, err)
		refund := new(big.Int).Sub(takerAfter, takerBefore)
		require.Equal(t, "4000000000000000000", refund.String(), "10 shares @40 refunded")
		return nil
	}
}

// ob_batch_unlock_collateral refuses mismatched arrays, missing or malformed
// entries and an unknown bridge before it pays anything; a valid batch still
// pays and records its impact.
func testBatchUnlockChecksInputs(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		maker := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000050")
		require.NoError(t, giveBalanceChained(ctx, platform, maker.Address(), "500000000000000000000"))
		marketID := askHoldingsMarket(t, ctx, platform, &maker, "stbatchunlockinputs0000000000001")
		// A 10-pair split puts $10 in the vault and gives the maker a participant id.
		require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, &maker, marketID, 50, 10))
		pid := int64(participantIDForWallet(t, ctx, platform, &maker))

		recipient := util.Unsafe_NewEthereumAddressFromString("0x7c00000000000000000000000000000000000059")
		wallet := recipient.Address()
		one := numeric78(t, "1000000000000000000")
		batch := func(bridge string, pids, wallets, amounts, outcomes any) []any {
			return []any{marketID, bridge, pids, wallets, amounts, outcomes}
		}

		// Each bad batch has a valid entry first and the bad one second.
		pids := []int64{pid, pid}
		wallets := []string{wallet, wallet}
		amounts := []*kwilTypes.Decimal{one, one}
		outcomes := []bool{true, true}
		cases := []struct {
			name string
			args []any
			want string
		}{
			{"arrays of different length", batch(testExtensionName, pids, []string{wallet}, amounts, outcomes),
				"Batch unlock arrays must have the same length"},
			{"no participant", batch(testExtensionName, []*int64{&pid, nil}, wallets, amounts, outcomes),
				"Batch unlock participant is required"},
			{"no outcome", batch(testExtensionName, pids, wallets, amounts, []*bool{&outcomes[0], nil}),
				"Batch unlock outcome is required"},
			{"no wallet", batch(testExtensionName, pids, []*string{&wallet, nil}, amounts, outcomes),
				"Batch unlock wallet is required"},
			{"short wallet", batch(testExtensionName, pids, []string{wallet, "0x1234"}, amounts, outcomes),
				"Invalid batch unlock wallet format"},
			{"no amount", batch(testExtensionName, pids, wallets, []*kwilTypes.Decimal{one, nil}, outcomes),
				"Batch unlock amount is required"},
			{"zero amount", batch(testExtensionName, pids, wallets, []*kwilTypes.Decimal{one, numeric78(t, "0")}, outcomes),
				"Batch unlock amount must be positive"},
			{"negative amount", batch(testExtensionName, pids, wallets, []*kwilTypes.Decimal{one, numeric78(t, "-1")}, outcomes),
				"Batch unlock amount must be positive"},
			{"unknown bridge", batch("no_such_bridge", pids, wallets, amounts, outcomes),
				"Invalid bridge"},
		}

		impactsBefore, err := getNetImpacts(ctx, platform, marketID)
		require.NoError(t, err)
		for _, tc := range cases {
			err := privateCall(ctx, platform, &maker, "ob_batch_unlock_collateral", tc.args)
			t.Logf("%s: err=%v", tc.name, err)
			require.Error(t, err, tc.name)
			require.Contains(t, err.Error(), tc.want, tc.name)
		}
		received, err := getUSDCBalance(ctx, platform, wallet)
		require.NoError(t, err)
		require.Equal(t, "0", received.String(), "nothing is paid by a refused batch")
		impactsAfterRefusals, err := getNetImpacts(ctx, platform, marketID)
		require.NoError(t, err)
		require.Equal(t, len(impactsBefore), len(impactsAfterRefusals), "nothing is recorded by a refused batch")

		require.NoError(t, privateCall(ctx, platform, &maker, "ob_batch_unlock_collateral",
			batch(testExtensionName, []int64{pid}, []string{wallet}, []*kwilTypes.Decimal{one}, []bool{true})))
		received, err = getUSDCBalance(ctx, platform, wallet)
		require.NoError(t, err)
		require.Equal(t, "1000000000000000000", received.String())
		impacts, err := getNetImpacts(ctx, platform, marketID)
		require.NoError(t, err)
		require.Len(t, impacts, len(impactsBefore)+1)
		last := impacts[len(impacts)-1]
		require.Equal(t, int(pid), last.ParticipantID)
		require.True(t, last.Outcome)
		require.Equal(t, "1000000000000000000", last.CollateralChange.String())
		return nil
	}
}

// ob_lock_collateral had no callers. A fresh chain never creates it, and
// running the vault migration on a chain that still has it removes it.
func testLockCollateralHelperDropped(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		require.False(t, actionExists(t, ctx, platform, "ob_lock_collateral"))
		require.True(t, actionExists(t, ctx, platform, "ob_unlock_collateral"))

		require.NoError(t, deployerExecute(ctx, platform, `CREATE OR REPLACE ACTION ob_lock_collateral($bridge TEXT, $amount NUMERIC(78, 0))
PRIVATE {
    if $amount IS NULL OR $amount <= 0::NUMERIC(78, 0) {
        ERROR('Lock amount must be positive');
    }
};`))
		require.True(t, actionExists(t, ctx, platform, "ob_lock_collateral"))

		var vaultMigration string
		for _, path := range migrations.GetSeedScriptPaths() {
			if filepath.Base(path) == "031-order-book-vault.sql" {
				content, err := os.ReadFile(path)
				require.NoError(t, err)
				vaultMigration = string(content)
			}
		}
		require.NotEmpty(t, vaultMigration)
		require.NoError(t, deployerExecute(ctx, platform, vaultMigration))

		require.False(t, actionExists(t, ctx, platform, "ob_lock_collateral"))
		require.True(t, actionExists(t, ctx, platform, "ob_unlock_collateral"))
		return nil
	}
}

// ===== helpers =====

// orderPlacer returns a function that runs one order action per block, one
// second apart, so the book's time priority is fixed.
func orderPlacer(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) func(*util.EthereumAddress, string, ...any) {
	ts := time.Now().Unix()
	return func(signer *util.EthereumAddress, action string, args ...any) {
		ts++
		err := activityCall(ctx, platform, signer, 1, ts, action, args, nil)
		require.NoError(t, err, "%s %v", action, args)
	}
}

// fundedWallets returns n wallets whose first byte is prefix, each holding
// amount of the market's bridge token.
func fundedWallets(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, prefix byte, n int, amount string) []util.EthereumAddress {
	wallets := make([]util.EthereumAddress, n)
	for i := range wallets {
		wallets[i] = util.Unsafe_NewEthereumAddressFromString(fmt.Sprintf("0x%02x%038x", prefix, i+1))
		require.NoError(t, giveUSDCBalanceChained(ctx, platform, wallets[i].Address(), amount))
	}
	return wallets
}

// countRestingOrders counts open orders on one side of one outcome.
func countRestingOrders(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, marketID int, outcome bool, bids bool) int {
	query := "SELECT 1 FROM ob_positions WHERE query_id = $query_id AND outcome = $outcome AND price > 0"
	if bids {
		query = "SELECT 1 FROM ob_positions WHERE query_id = $query_id AND outcome = $outcome AND price < 0"
	}
	return countRows(t, ctx, platform, query, map[string]any{"$query_id": marketID, "$outcome": outcome})
}

func countRows(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, query string, params map[string]any) int {
	engineCtx := &common.EngineContext{TxContext: &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		TxID:         platform.Txid(),
	}}
	rows := 0
	err := platform.Engine.Execute(engineCtx, platform.DB, query, params, func(*common.Row) error {
		rows++
		return nil
	})
	require.NoError(t, err)
	return rows
}

// requireMarketBalanced checks that every YES share has a NO share and that
// the vault covers what the open markets are owed.
func requireMarketBalanced(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, marketID int) {
	validBinaries, validCollateral, totalTrue, totalFalse, vault, expected, _ := validateMarket(t, ctx, platform, marketID)
	require.True(t, validBinaries, "TRUE shares=%d FALSE shares=%d", totalTrue, totalFalse)
	require.True(t, validCollateral, "vault=%s expected=%s", vault, expected)
}

// openBuysMarkets sets up two markets on the same bridge: a settleable one
// that holds only a YES bid for 10 @40 ($4 in the vault) and another one that
// holds 10 share pairs ($10 in the vault).
func openBuysMarkets(t *testing.T, ctx context.Context, platform *kwilTesting.Platform,
	maker, taker *util.EthereumAddress, streamID string) (int, int64, *attestationTests.AttestationTestHelper, int) {
	marketID, settleTime, helper := askHoldingsSettleableMarket(t, ctx, platform, maker, taker, streamID)
	otherStream := streamID[:len(streamID)-1] + "2"
	otherMarket := askHoldingsMarket(t, ctx, platform, maker, otherStream)

	require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, maker, otherMarket, 50, 10))
	require.NoError(t, callPlaceBuyOrder(ctx, platform, taker, marketID, true, 40, 10))

	validBinaries, validCollateral, totalTrue, totalFalse, vault, expected, openBuys := validateMarket(t, ctx, platform, marketID)
	t.Logf("market %d: shares %d/%d, open buys value %d, vault=%s expected=%s", marketID, totalTrue, totalFalse, openBuys, vault, expected)
	require.True(t, validBinaries)
	require.True(t, validCollateral)
	require.Zero(t, totalTrue)
	require.Zero(t, totalFalse)
	require.Equal(t, int64(400), openBuys)
	return marketID, settleTime, helper, otherMarket
}

func otherMarketShares(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, marketID int) int64 {
	_, _, totalTrue, _, _, _, _ := validateMarket(t, ctx, platform, marketID)
	return totalTrue
}

// privateCall runs a PRIVATE action the way another action reaches it.
func privateCall(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, action string, args []any) error {
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    1,
			Timestamp: time.Now().Unix(),
		},
		Signer:        signer.Bytes(),
		Caller:        signer.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	res, err := platform.Engine.Call(&common.EngineContext{TxContext: tx, OverrideAuthz: true}, platform.DB, "", action, args, nil)
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}

// deployerExecute runs a statement as the migration deployer does.
func deployerExecute(ctx context.Context, platform *kwilTesting.Platform, stmt string) error {
	return platform.Engine.Execute(&common.EngineContext{
		TxContext: &common.TxContext{
			Ctx:          ctx,
			Signer:       platform.Deployer,
			Caller:       string(platform.Deployer),
			TxID:         platform.Txid(),
			BlockContext: &common.BlockContext{Height: 1},
		},
		OverrideAuthz: true,
	}, platform.DB, stmt, nil, nil)
}

func actionExists(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, name string) bool {
	return countRows(t, ctx, platform, "SELECT 1 FROM info.actions WHERE namespace = 'main' AND name = $name",
		map[string]any{"$name": name}) > 0
}

func numeric78(t *testing.T, value string) *kwilTypes.Decimal {
	d, err := kwilTypes.ParseDecimalExplicit(value, 78, 0)
	require.NoError(t, err)
	return d
}
