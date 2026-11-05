//go:build kwiltest

package order_book

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// collateralPauseOwner owns the namespace in TestCollateralLockPause, so it is
// the only wallet that may call set_collateral_lock_paused.
var collateralPauseOwner = util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")

const collateralPausedErr = "Orders that lock new collateral are paused on " + testUSDCExtensionName

// TestCollateralLockPause covers the owner's collateral pause (060): only the
// owner can flip it; while a bridge is paused every order-book action that locks
// collateral on it is refused, and cancels, bid decreases, sells and settlement
// keep working; once resumed, the refused actions work again.
func TestCollateralLockPause(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_CollateralLockPause",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          collateralPauseOwner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testCollateralPauseOwnerOnly(t),
			testCollateralPauseRefusesLocks(t),
			testCollateralPauseKeepsExitsOpen(t),
			testCollateralPauseKeepsSettlementOpen(t),
			testCollateralPauseIsPerBridge(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testCollateralPauseOwnerOnly: only the owner can pause or resume, bad input is
// refused, both directions are idempotent and a repeated pause keeps its height.
func testCollateralPauseOwnerOnly(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		trader := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

		err := callSetCollateralLockPaused(ctx, platform, &trader, 10, testUSDCExtensionName, true)
		require.ErrorContains(t, err, "owner", "a wallet that is not the owner cannot pause")
		require.Empty(t, getCollateralLockPauses(t, ctx, platform))

		err = callSetCollateralLockPaused(ctx, platform, &collateralPauseOwner, 10, "not_a_bridge", true)
		require.ErrorContains(t, err, "Invalid bridge")
		err = callSetCollateralLockPaused(ctx, platform, &collateralPauseOwner, 10, nil, true)
		require.ErrorContains(t, err, "bridge parameter is required")
		err = callSetCollateralLockPaused(ctx, platform, &collateralPauseOwner, 10, testUSDCExtensionName, nil)
		require.ErrorContains(t, err, "paused is required")
		require.Empty(t, getCollateralLockPauses(t, ctx, platform))

		require.NoError(t, callSetCollateralLockPaused(ctx, platform, &collateralPauseOwner, 10, testUSDCExtensionName, true))
		require.NoError(t, callSetCollateralLockPaused(ctx, platform, &collateralPauseOwner, 20, testUSDCExtensionName, true))
		require.Equal(t, map[string]int64{testUSDCExtensionName: 10}, getCollateralLockPauses(t, ctx, platform),
			"a second pause keeps the first paused_at")

		err = callSetCollateralLockPaused(ctx, platform, &trader, 30, testUSDCExtensionName, false)
		require.ErrorContains(t, err, "owner", "a wallet that is not the owner cannot resume")
		require.Equal(t, map[string]int64{testUSDCExtensionName: 10}, getCollateralLockPauses(t, ctx, platform))

		require.NoError(t, callSetCollateralLockPaused(ctx, platform, &collateralPauseOwner, 30, testUSDCExtensionName, false))
		require.NoError(t, callSetCollateralLockPaused(ctx, platform, &collateralPauseOwner, 40, testUSDCExtensionName, false))
		require.Empty(t, getCollateralLockPauses(t, ctx, platform))

		return nil
	}
}

// testCollateralPauseRefusesLocks: while hoodi_tt2 is paused, a buy, a split and
// a bid raise are refused and leave the book and the wallet as they were; after
// the owner resumes, each of them locks as before.
func testCollateralPauseRefusesLocks(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		trader := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")
		require.NoError(t, giveBalanceChained(ctx, platform, trader.Address(), "100000000000000000000"))
		marketID := createCollateralPauseMarket(t, ctx, platform, &trader, "stcollpause000000000000000000001")

		// A resting bid from before the pause, to raise later: 10 YES at 50 locks 5.
		require.NoError(t, callPlaceBuyOrder(ctx, platform, &trader, marketID, true, 50, 10))

		require.NoError(t, callSetCollateralLockPaused(ctx, platform, &collateralPauseOwner, 1, testUSDCExtensionName, true))

		bookBefore, err := getPositions(ctx, platform, marketID)
		require.NoError(t, err)
		balanceBefore, err := getUSDCBalance(ctx, platform, trader.Address())
		require.NoError(t, err)

		err = callPlaceBuyOrder(ctx, platform, &trader, marketID, true, 40, 10)
		require.ErrorContains(t, err, collateralPausedErr, "buy")
		err = callPlaceSplitLimitOrder(ctx, platform, &trader, marketID, 60, 10)
		require.ErrorContains(t, err, collateralPausedErr, "split")
		err = callChangeBid(ctx, platform, &trader, marketID, true, -50, -60, 10)
		require.ErrorContains(t, err, collateralPausedErr, "bid raise")

		bookAfter, err := getPositions(ctx, platform, marketID)
		require.NoError(t, err)
		require.Equal(t, bookBefore, bookAfter, "refused orders leave the book as it was")
		balanceAfter, err := getUSDCBalance(ctx, platform, trader.Address())
		require.NoError(t, err)
		require.Equal(t, balanceBefore.String(), balanceAfter.String(), "refused orders lock nothing")

		require.NoError(t, callSetCollateralLockPaused(ctx, platform, &collateralPauseOwner, 2, testUSDCExtensionName, false))

		require.NoError(t, callPlaceBuyOrder(ctx, platform, &trader, marketID, true, 40, 10))   // locks 4
		require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, &trader, marketID, 60, 10))  // locks 10
		require.NoError(t, callChangeBid(ctx, platform, &trader, marketID, true, -50, -60, 10)) // locks 1 more

		balanceResumed, err := getUSDCBalance(ctx, platform, trader.Address())
		require.NoError(t, err)
		require.Equal(t, new(big.Int).Sub(balanceBefore, toWei("15")).String(), balanceResumed.String())

		return nil
	}
}

// testCollateralPauseKeepsExitsOpen: while hoodi_tt2 is paused, a bid cancel, a
// bid decrease, a bid move at the same collateral, a new sell from holdings and
// a sell cancel all go through, and the refunds reach the wallet.
func testCollateralPauseKeepsExitsOpen(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		trader := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")
		require.NoError(t, giveBalanceChained(ctx, platform, trader.Address(), "100000000000000000000"))
		marketID := createCollateralPauseMarket(t, ctx, platform, &trader, "stcollpause000000000000000000002")

		// Book from before the pause: two bids, and a split that holds 10 YES and lists 10 NO at 40.
		require.NoError(t, callPlaceBuyOrder(ctx, platform, &trader, marketID, true, 56, 10))  // locks 5.6
		require.NoError(t, callPlaceBuyOrder(ctx, platform, &trader, marketID, false, 30, 10)) // locks 3
		require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, &trader, marketID, 60, 10)) // locks 10

		require.NoError(t, callSetCollateralLockPaused(ctx, platform, &collateralPauseOwner, 1, testUSDCExtensionName, true))
		balancePaused, err := getUSDCBalance(ctx, platform, trader.Address())
		require.NoError(t, err)

		require.NoError(t, callCancelOrder(ctx, platform, &trader, marketID, true, -56), "bid cancel refunds 5.6")
		require.NoError(t, callChangeBid(ctx, platform, &trader, marketID, false, -30, -20, 10), "bid decrease refunds 1")
		require.NoError(t, callChangeBid(ctx, platform, &trader, marketID, false, -20, -10, 20), "10 x 20 = 20 x 10, no new collateral")
		require.NoError(t, callPlaceSellOrder(ctx, platform, &trader, marketID, true, 70, 5), "sell from holdings")
		require.NoError(t, callCancelOrder(ctx, platform, &trader, marketID, false, 40), "sell cancel")

		balanceAfter, err := getUSDCBalance(ctx, platform, trader.Address())
		require.NoError(t, err)
		require.Equal(t, new(big.Int).Add(balancePaused, toWei("6.6")).String(), balanceAfter.String())

		positions, err := getPositions(ctx, platform, marketID)
		require.NoError(t, err)
		book := map[collateralPauseBookKey]int64{}
		for _, p := range positions {
			book[collateralPauseBookKey{outcome: p.Outcome, price: p.Price}] = p.Amount
		}
		require.Equal(t, map[collateralPauseBookKey]int64{
			{outcome: false, price: -10}: 20, // the moved NO bid
			{outcome: true, price: 70}:   5,  // the new YES sell
			{outcome: true, price: 0}:    5,  // YES still held
			{outcome: false, price: 0}:   10, // NO back in holdings
		}, book)

		return nil
	}
}

// testCollateralPauseKeepsSettlementOpen: with hoodi_tt2 paused, the market
// still validates and settlement pays the winners and refunds the open bid.
func testCollateralPauseKeepsSettlementOpen(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// The trader also proposes the block and validates, as in
		// testPnLImpactSettlement, so distribute_fees has someone to pay.
		traderPub := NewTestProposerPub(t)
		trader := util.Unsafe_NewEthereumAddressFromString(fmt.Sprintf("0x%x", crypto.EthereumAddressFromPubKey(traderPub)))
		platform.Validators.ForTestingAddValidator(traderPub.Bytes(), crypto.KeyTypeSecp256k1, 1)

		require.NoError(t, InjectDualBalance(ctx, platform, trader.Address(), "100000000000000000000"))
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		marketID := createCollateralPauseMarket(t, ctx, platform, &trader, "stcollpause000000000000000000003")

		require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, &trader, marketID, 50, 10)) // locks 10: YES held, NO listed at 50
		require.NoError(t, callPlaceBuyOrder(ctx, platform, &trader, marketID, false, 30, 10)) // locks 3: open bid

		require.NoError(t, callSetCollateralLockPaused(ctx, platform, &collateralPauseOwner, 1, testUSDCExtensionName, true))

		validBinaries, validCollateral, _, _, _, _, _ := validateMarket(t, ctx, platform, marketID)
		require.True(t, validBinaries)
		require.True(t, validCollateral)

		balancePaused, err := getUSDCBalance(ctx, platform, trader.Address())
		require.NoError(t, err)

		require.NoError(t, callProcessSettlementWithProposer(t, ctx, platform, marketID, true, traderPub))

		positions, err := getPositions(ctx, platform, marketID)
		require.NoError(t, err)
		require.Empty(t, positions, "settlement clears the book")

		// 10 winning YES at 0.98 plus the 3 refunded for the open bid, plus any
		// part of the 0.2 fee paid back to the trader as data provider and validator.
		balanceSettled, err := getUSDCBalance(ctx, platform, trader.Address())
		require.NoError(t, err)
		received := new(big.Int).Sub(balanceSettled, balancePaused)
		require.True(t, received.Cmp(toWei("12.8")) >= 0, "received %s", received)
		require.True(t, received.Cmp(toWei("13")) <= 0, "received %s", received)

		return nil
	}
}

// testCollateralPauseIsPerBridge: pausing one bridge leaves markets on another
// bridge open.
func testCollateralPauseIsPerBridge(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		trader := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")
		require.NoError(t, giveBalanceChained(ctx, platform, trader.Address(), "100000000000000000000"))
		marketID := createCollateralPauseMarket(t, ctx, platform, &trader, "stcollpause000000000000000000004")

		require.NoError(t, callSetCollateralLockPaused(ctx, platform, &collateralPauseOwner, 1, "sepolia_bridge", true))
		require.Equal(t, map[string]int64{"sepolia_bridge": 1}, getCollateralLockPauses(t, ctx, platform))

		require.NoError(t, callPlaceBuyOrder(ctx, platform, &trader, marketID, true, 40, 10))
		require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, &trader, marketID, 60, 10))
		require.NoError(t, callChangeBid(ctx, platform, &trader, marketID, true, -40, -45, 10))

		return nil
	}
}

// =============================================================================
// Helpers
// =============================================================================

type collateralPauseBookKey struct {
	outcome bool
	price   int16
}

func collateralPauseTx(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, height int64) *common.TxContext {
	return &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    height,
			Timestamp: time.Now().Unix(),
		},
		Signer:        signer.Bytes(),
		Caller:        signer.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
}

func callSetCollateralLockPaused(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress,
	height int64, bridge, paused any) error {
	res, err := platform.Engine.Call(
		&common.EngineContext{TxContext: collateralPauseTx(ctx, platform, signer, height)},
		platform.DB, "", "set_collateral_lock_paused", []any{bridge, paused}, nil,
	)
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}

func getCollateralLockPauses(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) map[string]int64 {
	pauses := map[string]int64{}
	res, err := platform.Engine.Call(
		&common.EngineContext{TxContext: collateralPauseTx(ctx, platform, &collateralPauseOwner, 1)},
		platform.DB, "", "get_collateral_lock_pauses", []any{},
		func(row *common.Row) error {
			pauses[row.Values[0].(string)] = row.Values[1].(int64)
			return nil
		},
	)
	require.NoError(t, err)
	if res != nil {
		require.NoError(t, res.Error)
	}
	return pauses
}

func createCollateralPauseMarket(t *testing.T, ctx context.Context, platform *kwilTesting.Platform,
	creator *util.EthereumAddress, streamID string) int {
	queryComponents, err := encodeQueryComponentsForTests(creator.Address(), streamID, "get_record", nil)
	require.NoError(t, err)

	var marketID int64
	err = callCreateMarket(ctx, platform, creator, queryComponents, time.Now().Add(24*time.Hour).Unix(), 5, 20,
		func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
	require.NoError(t, err)
	return int(marketID)
}
