//go:build kwiltest

package order_book

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/feefund"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"

	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
)

// Markets on the outcome actions of migration 063 (ids 13 to 16), from create_market through
// request_attestation and sign_attestation to settle_market. Each settles YES, NO or INVALID. An
// INVALID market has no winner: every share is redeemed at $0.50 and every open buy order
// refunded, with no fee, so the data provider, validators and LPs get nothing and the vault
// releases exactly what the market owes (website#5250).

const (
	outcomeIndexChange = "index_change_in_range_v2"
	outcomeAbove       = "price_above_threshold_v2"
	outcomeBelow       = "price_below_threshold_v2"
	outcomeInRange     = "value_in_range_v2"

	outcomeDay = int64(86400)

	// One USDC on the test bridge, in base units.
	outcomeUnit = int64(1_000_000_000_000_000_000)
)

func TestOutcomeSettlement(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_OutcomeSettlement",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testOutcomeSettlesYesNoInvalid(t, outcomeIndexChange),
			testOutcomeSettlesYesNoInvalid(t, outcomeAbove),
			testOutcomeSettlesYesNoInvalid(t, outcomeBelow),
			testOutcomeSettlesYesNoInvalid(t, outcomeInRange),
			testOutcomeBucketsSettleExactlyOneYes(t),
			testOutcomeCaptureWaitsForAPrint(t),
			testOutcomeSettleTimeBeforeTimestamp(t),
			testOutcomeInvalidRedeemsHalfAndRefundsBids(t),
			testOutcomeYesPaysAsBefore(t),
			testOutcomeCreateMarketRefusals(t),
			testOutcomeRegistration(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// =============================================================================
// Each action settles YES, NO and INVALID
// =============================================================================

// The stream moved from 100 to 102 over a day, and holds 102 just before T. The INVALID market
// asks about a time five days back with a one-hour window, where the stream holds nothing, so its
// deadline has passed by the time it settles.
func testOutcomeSettlesYesNoInvalid(t *testing.T, action string) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		now := time.Now().Unix()
		at := now - 7200
		fx := setupOutcomeFixture(t, ctx, platform, "0x2222222222222222222222222222222222222222",
			"stoutcomeperaction00000000000000", now, []outcomeRecord{
				{at + 60 - outcomeDay, "100"},
				{at - 60, "102"},
				{at + 60, "102"},
			})

		base := outcomeArgs{action: action, at: at, window: outcomeDay, tolerance: 600, interval: outcomeDay}
		var yes, no outcomeArgs
		switch action {
		case outcomeIndexChange: // +2%
			yes, no = base.withBounds("1", "3"), base.withBounds("3", "5")
		case outcomeAbove:
			yes, no = base.withThreshold("101"), base.withThreshold("105")
		case outcomeBelow:
			yes, no = base.withThreshold("105"), base.withThreshold("101")
		case outcomeInRange:
			yes, no = base.withBounds("100", "103"), base.withMin("103")
		}
		invalid := yes
		invalid.at, invalid.window = now-5*outcomeDay, 3600

		for _, c := range []struct {
			label string
			args  outcomeArgs
			want  *bool
		}{
			{"YES", yes, boolPtr(true)},
			{"NO", no, boolPtr(false)},
			{"INVALID", invalid, nil},
		} {
			argsBytes := fx.encode(t, c.args)
			queryID := fx.createMarket(t, platform, action, argsBytes)
			fx.attest(t, platform, action, argsBytes)

			settled, outcome, settledAt := fx.settle(t, platform, queryID, nil)
			require.True(t, settled, "%s %s: market should be settled", action, c.label)
			require.Equal(t, fx.settleTime+1, settledAt, "%s %s: settled_at", action, c.label)
			if c.want == nil {
				require.Nil(t, outcome, "%s INVALID: winning_outcome stays NULL", action)
			} else {
				require.NotNil(t, outcome, "%s %s: winning_outcome should be set", action, c.label)
				require.Equal(t, *c.want, *outcome, "%s %s", action, c.label)
			}
		}

		return nil
	}
}

// A value on a bucket boundary belongs to the bucket it opens and to no other, so a ladder
// settles exactly one YES. The 040 value_in_range is inclusive at both ends and would settle two.
func testOutcomeBucketsSettleExactlyOneYes(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		now := time.Now().Unix()
		at := now - 7200
		fx := setupOutcomeFixture(t, ctx, platform, "0x3333333333333333333333333333333333333333",
			"stoutcomebuckets0000000000000000", now, []outcomeRecord{{at - 60, "100"}})

		base := outcomeArgs{action: outcomeInRange, at: at, window: outcomeDay, tolerance: 600}
		buckets := []struct {
			args outcomeArgs
			want bool
		}{
			{base.withMax("99"), false},
			{base.withBounds("99", "100"), false},
			{base.withBounds("100", "101"), true},
			{base.withMin("101"), false},
		}

		yesCount := 0
		for i, bucket := range buckets {
			argsBytes := fx.encode(t, bucket.args)
			queryID := fx.createMarket(t, platform, outcomeInRange, argsBytes)
			fx.attest(t, platform, outcomeInRange, argsBytes)

			settled, outcome, _ := fx.settle(t, platform, queryID, nil)
			require.True(t, settled)
			require.NotNil(t, outcome)
			require.Equal(t, bucket.want, *outcome, "bucket %d", i)
			if *outcome {
				yesCount++
			}
		}
		require.Equal(t, 1, yesCount, "a value on a boundary settles exactly one bucket YES")

		return nil
	}
}

// =============================================================================
// A capture before the print reverts; the market settles once it lands
// =============================================================================

func testOutcomeCaptureWaitsForAPrint(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		now := time.Now().Unix()
		at := now - 600
		fx := setupOutcomeFixture(t, ctx, platform, "0x4444444444444444444444444444444444444444",
			"stoutcomelateprint00000000000000", now, []outcomeRecord{{at - 10*outcomeDay, "50"}})

		argsBytes := fx.encode(t, outcomeArgs{action: outcomeAbove, at: at, window: 2 * outcomeDay, tolerance: 3600}.withThreshold("100"))
		queryID := fx.createMarket(t, platform, outcomeAbove, argsBytes)

		err := fx.attestErr(t, platform, outcomeAbove, argsBytes)
		require.Error(t, err, "no print yet: the capture reverts")
		require.Contains(t, err.Error(), "awaiting print")
		require.Zero(t, countRows(t, ctx, platform,
			"SELECT 1 FROM attestations a JOIN ob_queries q ON a.attestation_hash = q.hash WHERE q.id = $id",
			map[string]any{"$id": queryID}), "a reverted capture stores nothing")

		err = fx.settleErr(t, platform, queryID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Attestation not found")

		// The print lands, stamped a minute before T.
		engineCtx := fx.helper.NewEngineContext()
		mustCallAction(t, engineCtx, platform, "insert_records", []any{
			[]string{fx.dataProvider}, []string{fx.streamID}, []int64{at - 60},
			[]*kwilTypes.Decimal{mustDecimal(t, "120")},
		}, nil)

		fx.attest(t, platform, outcomeAbove, argsBytes)
		settled, outcome, _ := fx.settle(t, platform, queryID, nil)
		require.True(t, settled)
		require.NotNil(t, outcome)
		require.True(t, *outcome, "120 is above 100")

		return nil
	}
}

// create_market lets settle_time fall before T. A capture at settle_time then waits for block
// time T instead of settling on the record that was there early, and the market settles on the
// value as of T, as the 040 actions do.
func testOutcomeSettleTimeBeforeTimestamp(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		now := time.Now().Unix()
		fx := setupOutcomeFixture(t, ctx, platform, "0xdddddddddddddddddddddddddddddddddddddddd",
			"stoutcomebeforet0000000000000000", now, []outcomeRecord{{now - 60, "120"}})

		// T is two hours after settle_time; the one-day tolerance already holds the 120.
		at := fx.settleTime + 2*3600
		argsBytes := fx.encode(t, outcomeArgs{action: outcomeAbove, at: at, window: 2 * outcomeDay, tolerance: outcomeDay}.withThreshold("100"))
		queryID := fx.createMarket(t, platform, outcomeAbove, argsBytes)

		err := fx.attestErrAt(t, platform, outcomeAbove, argsBytes, fx.settleTime+1)
		require.Error(t, err, "a capture before T reverts")
		require.Contains(t, err.Error(), "awaiting print")
		require.Contains(t, err.Error(), "is before the timestamp")
		require.Zero(t, countRows(t, ctx, platform,
			"SELECT 1 FROM attestations a JOIN ob_queries q ON a.attestation_hash = q.hash WHERE q.id = $id",
			map[string]any{"$id": queryID}), "a reverted capture stores nothing")

		// The value as of T is written before T.
		mustCallAction(t, fx.helper.NewEngineContext(), platform, "insert_records", []any{
			[]string{fx.dataProvider}, []string{fx.streamID}, []int64{at - 60},
			[]*kwilTypes.Decimal{mustDecimal(t, "80")},
		}, nil)

		fx.attestAt(t, platform, outcomeAbove, argsBytes, at+1)
		settled, outcome, settledAt := fx.settleAt(t, platform, queryID, nil, at+1)
		require.True(t, settled)
		require.NotNil(t, outcome)
		require.False(t, *outcome, "80, the value as of T, is not above 100")
		require.Equal(t, at+1, settledAt)

		return nil
	}
}

// =============================================================================
// INVALID with a traded book
// =============================================================================

func testOutcomeInvalidRedeemsHalfAndRefundsBids(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		wallet := func(hex string) *util.EthereumAddress {
			addr := util.Unsafe_NewEthereumAddressFromString(hex)
			return &addr
		}
		dataProvider := wallet("0x5555555555555555555555555555555555555555")
		alice := wallet("0x6666666666666666666666666666666666666666")
		bob := wallet("0x7777777777777777777777777777777777777777")
		carol := wallet("0x8888888888888888888888888888888888888888")
		dave := wallet("0x9999999999999999999999999999999999999999")
		erin := wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		frank := wallet("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
		gina := wallet("0xcccccccccccccccccccccccccccccccccccccccc")
		traders := []*util.EthereumAddress{alice, bob, carol, dave, erin, frank, gina}

		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		platform.Deployer = dataProvider.Bytes()
		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		require.NoError(t, setup.CreateDataProvider(ctx, platform, dataProvider.Address()))
		for _, w := range append([]*util.EthereumAddress{dataProvider}, traders...) {
			require.NoError(t, giveBalanceChained(ctx, platform, w.Address(), "1000000000000000000000"))
		}
		require.NoError(t, feefund.EnsureWalletFunded(ctx, platform, dataProvider.Address(), "200000000000000000000"))

		// The stream holds nothing near T, five days back, and the one-hour window has long
		// passed, so the market resolves INVALID.
		now := time.Now().Unix()
		fx := &outcomeFixture{
			helper:       helper,
			dataProvider: dataProvider.Address(),
			streamID:     "stoutcomeinvalidbook000000000000",
			createAt:     now,
			settleTime:   now + 3600,
		}
		mustCallAction(t, helper.NewEngineContext(), platform, "create_stream", []any{fx.streamID, "primitive"}, nil)

		argsBytes := fx.encode(t, outcomeArgs{
			action: outcomeIndexChange, at: now - 5*outcomeDay, window: 3600, tolerance: 600, interval: outcomeDay,
		}.withBounds("1", "3"))
		queryID := fx.createMarket(t, platform, outcomeIndexChange, argsBytes)

		// Another market keeps collateral in the same vault and stays open.
		otherComponents, err := encodeQueryComponentsForTests(dataProvider.Address(), "stoutcomeotherbook00000000000000", "get_record", nil)
		require.NoError(t, err)
		otherID := fx.createMarketFromComponents(t, platform, otherComponents, now+outcomeDay)

		start := usdcBalances(t, ctx, platform, append(traders, dataProvider))

		// alice mints 100 pairs at 60/40 and bob lifts the NO side, so they hold opposite sides.
		require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, alice, queryID, 60, 100))
		require.NoError(t, callPlaceBuyOrder(ctx, platform, bob, queryID, false, 40, 100))
		// alice offers 30 of her YES at 70; carol and dave leave bids resting.
		require.NoError(t, callPlaceSellOrder(ctx, platform, alice, queryID, true, 70, 30))
		require.NoError(t, callPlaceBuyOrder(ctx, platform, carol, queryID, true, 30, 50))
		require.NoError(t, callPlaceBuyOrder(ctx, platform, dave, queryID, false, 20, 40))
		// erin and frank quote both sides, so the market earns LP samples.
		setupLPScenario(t, ctx, platform, erin, frank, queryID)
		// gina's collateral sits in the vault for the other market.
		require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, gina, otherID, 50, 20))

		positions, err := getPositions(ctx, platform, queryID)
		require.NoError(t, err)
		ids := map[string]int{}
		for _, w := range traders {
			ids[w.Address()] = participantIDForWallet(t, ctx, platform, w)
		}
		requirePosition(t, positions, ids[alice.Address()], true, 0, 70)
		requirePosition(t, positions, ids[alice.Address()], true, 70, 30)
		requirePosition(t, positions, ids[bob.Address()], false, 0, 100)
		requirePosition(t, positions, ids[carol.Address()], true, -30, 50)
		requirePosition(t, positions, ids[dave.Address()], false, -20, 40)

		validBinaries, validCollateral, _, _, vaultBefore, expectedBefore, _ := validateMarket(t, ctx, platform, queryID)
		require.True(t, validBinaries, "the book is balanced before settlement")
		require.True(t, validCollateral, "the vault covers both markets before settlement")
		require.Equal(t, expectedBefore, vaultBefore, "the vault holds exactly what both markets owe")

		require.NoError(t, triggerDirectSampling(ctx, platform, queryID, 1000))
		require.Positive(t, countRows(t, ctx, platform, "SELECT 1 FROM ob_rewards WHERE query_id = $id",
			map[string]any{"$id": queryID}), "the LP quotes earn samples before settlement")

		fx.attest(t, platform, outcomeIndexChange, argsBytes)

		validatorPub, validatorAddr := injectTestValidator(t, platform)
		walletsAtSettle := append(append([]*util.EthereumAddress{}, traders...), dataProvider, wallet(validatorAddr))
		before := usdcBalances(t, ctx, platform, walletsAtSettle)

		settled, outcome, settledAt := fx.settle(t, platform, queryID, validatorPub)
		require.True(t, settled, "the market is settled")
		require.Nil(t, outcome, "INVALID leaves winning_outcome NULL")
		require.Equal(t, fx.settleTime+1, settledAt)

		// Every share pays 50 cents and every bid gets back what it locked.
		expectedPay := map[int]*big.Int{}
		obligation := new(big.Int)
		for _, p := range positions {
			pay := positionInvalidPay(p)
			if expectedPay[p.ParticipantID] == nil {
				expectedPay[p.ParticipantID] = new(big.Int)
			}
			expectedPay[p.ParticipantID].Add(expectedPay[p.ParticipantID], pay)
			obligation.Add(obligation, pay)
		}
		after := usdcBalances(t, ctx, platform, walletsAtSettle)
		for _, w := range traders {
			want := expectedPay[ids[w.Address()]]
			if want == nil {
				want = new(big.Int)
			}
			got := new(big.Int).Sub(after[w.Address()], before[w.Address()])
			require.Equal(t, want.String(), got.String(), "INVALID payout of %s", w.Address())
		}
		require.Equal(t, before[dataProvider.Address()].String(), after[dataProvider.Address()].String(),
			"the data provider is paid no fee")
		require.Equal(t, before[validatorAddr].String(), after[validatorAddr].String(),
			"the validator is paid no fee")

		// From the start: alice minted 100 pairs for $100, sold the NO side for $40 and gets $50
		// for her 100 YES; bob paid $40 and gets $50; the bids and the LP quotes get back exactly
		// what they put in.
		requireDeltaFromStart(t, ctx, platform, alice, start, "-10")
		requireDeltaFromStart(t, ctx, platform, bob, start, "10")
		requireDeltaFromStart(t, ctx, platform, carol, start, "0")
		requireDeltaFromStart(t, ctx, platform, dave, start, "0")
		requireDeltaFromStart(t, ctx, platform, erin, start, "0")
		requireDeltaFromStart(t, ctx, platform, frank, start, "0")
		requireDeltaFromStart(t, ctx, platform, gina, start, "-20")

		// The vault releases exactly this market's obligation and still covers the other market.
		_, otherValid, _, _, vaultAfter, otherExpected, _ := validateMarket(t, ctx, platform, otherID)
		require.True(t, otherValid)
		require.Equal(t, otherExpected, vaultAfter, "the vault now holds exactly the other market's collateral")
		vaultDrop := new(big.Int).Sub(mustBigInt(t, vaultBefore), mustBigInt(t, vaultAfter))
		require.Equal(t, obligation.String(), vaultDrop.String(), "the vault drops by this market's obligation only")
		require.Equal(t, new(big.Int).Mul(big.NewInt(20), big.NewInt(outcomeUnit)).String(), vaultAfter)

		remaining, err := getPositions(ctx, platform, queryID)
		require.NoError(t, err)
		require.Empty(t, remaining, "every position is closed")
		require.Zero(t, countRows(t, ctx, platform, "SELECT 1 FROM ob_rewards WHERE query_id = $id",
			map[string]any{"$id": queryID}), "the LP samples are deleted")
		require.Zero(t, countRows(t, ctx, platform, "SELECT 1 FROM ob_fee_distributions WHERE query_id = $id",
			map[string]any{"$id": queryID}), "no fee is distributed")
		otherPositions, err := getPositions(ctx, platform, otherID)
		require.NoError(t, err)
		require.Len(t, otherPositions, 2, "the other market is untouched")

		requireInvalidLedger(t, ctx, platform, queryID, positions, settledAt)

		return nil
	}
}

// positionInvalidPay is what an INVALID market pays a position, in base units: 50 cents a share
// for a holding or an ask, the locked price for a bid.
func positionInvalidPay(p Position) *big.Int {
	cents := int64(50)
	if p.Price < 0 {
		cents = -int64(p.Price)
	}
	pay := new(big.Int).Mul(big.NewInt(p.Amount), big.NewInt(cents))
	pay.Mul(pay, big.NewInt(outcomeUnit))
	return pay.Div(pay, big.NewInt(100))
}

// requireInvalidLedger checks the rows the indexer reads: one 'settled' order event per position
// and one payout impact per position.
func requireInvalidLedger(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, queryID int, positions []Position, settledAt int64) {
	t.Helper()

	sorted := append([]Position(nil), positions...)
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if a.ParticipantID != b.ParticipantID {
			return a.ParticipantID < b.ParticipantID
		}
		if a.Outcome != b.Outcome {
			return !a.Outcome
		}
		if a.Price != b.Price {
			return a.Price < b.Price
		}
		return a.Amount < b.Amount
	})

	var wantEvents, wantImpacts []string
	for _, p := range sorted {
		price := int64(50)
		if p.Price < 0 {
			price = -int64(p.Price)
		}
		wantEvents = append(wantEvents, fmt.Sprintf("%d/%t/%d/%d", p.ParticipantID, p.Outcome, price, p.Amount))
		wantImpacts = append(wantImpacts, fmt.Sprintf("%d/%t/0/%s/false", p.ParticipantID, p.Outcome, positionInvalidPay(p)))
	}

	var gotEvents []string
	engineCtx := readContext(ctx, platform)
	err := platform.Engine.Execute(engineCtx, platform.DB,
		`SELECT participant_id, outcome, price, amount FROM ob_order_events
		 WHERE query_id = $id AND event_type = 'settled' AND block_timestamp = $ts ORDER BY id`,
		map[string]any{"$id": queryID, "$ts": settledAt},
		func(row *common.Row) error {
			gotEvents = append(gotEvents, fmt.Sprintf("%d/%t/%d/%d",
				row.Values[0].(int64), row.Values[1].(bool), row.Values[2].(int64), row.Values[3].(int64)))
			return nil
		})
	require.NoError(t, err)
	require.Equal(t, wantEvents, gotEvents, "one settled event per position, 50 for a share and the bid price for a bid")

	var gotImpacts []string
	err = platform.Engine.Execute(engineCtx, platform.DB,
		`SELECT participant_id, outcome, shares_change, collateral_change, is_negative FROM ob_net_impacts
		 WHERE query_id = $id AND timestamp = $ts ORDER BY id`,
		map[string]any{"$id": queryID, "$ts": settledAt},
		func(row *common.Row) error {
			gotImpacts = append(gotImpacts, fmt.Sprintf("%d/%t/%d/%s/%t",
				row.Values[0].(int64), row.Values[1].(bool), row.Values[2].(int64),
				row.Values[3].(*kwilTypes.Decimal).String(), row.Values[4].(bool)))
			return nil
		})
	require.NoError(t, err)
	// ob_batch_unlock_collateral writes these in its own order, so only the set is compared.
	require.ElementsMatch(t, wantImpacts, gotImpacts, "one payout impact per position, with the position's own outcome")
}

// =============================================================================
// YES and NO pay as the old actions do
// =============================================================================

func testOutcomeYesPaysAsBefore(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dataProvider := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")
		winner := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")
		loser := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")

		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		platform.Deployer = dataProvider.Bytes()
		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		require.NoError(t, setup.CreateDataProvider(ctx, platform, dataProvider.Address()))
		for _, w := range []string{dataProvider.Address(), winner.Address(), loser.Address()} {
			require.NoError(t, giveBalanceChained(ctx, platform, w, "1000000000000000000000"))
		}
		require.NoError(t, feefund.EnsureWalletFunded(ctx, platform, dataProvider.Address(), "200000000000000000000"))

		now := time.Now().Unix()
		at := now - 7200
		fx := &outcomeFixture{
			helper:       helper,
			dataProvider: dataProvider.Address(),
			streamID:     "stoutcomeyespays0000000000000000",
			createAt:     now,
			settleTime:   now + 3600,
		}
		engineCtx := helper.NewEngineContext()
		mustCallAction(t, engineCtx, platform, "create_stream", []any{fx.streamID, "primitive"}, nil)
		mustCallAction(t, engineCtx, platform, "insert_records", []any{
			[]string{fx.dataProvider}, []string{fx.streamID}, []int64{at - 60},
			[]*kwilTypes.Decimal{mustDecimal(t, "102")},
		}, nil)

		argsBytes := fx.encode(t, outcomeArgs{action: outcomeInRange, at: at, window: outcomeDay, tolerance: 600}.withBounds("100", "103"))
		queryID := fx.createMarket(t, platform, outcomeInRange, argsBytes)

		winnerStart, err := getUSDCBalance(ctx, platform, winner.Address())
		require.NoError(t, err)
		loserStart, err := getUSDCBalance(ctx, platform, loser.Address())
		require.NoError(t, err)

		require.NoError(t, callPlaceSplitLimitOrder(ctx, platform, &winner, queryID, 60, 100))
		require.NoError(t, callPlaceBuyOrder(ctx, platform, &loser, queryID, false, 40, 100))

		fx.attest(t, platform, outcomeInRange, argsBytes)
		settled, outcome, _ := fx.settle(t, platform, queryID, NewTestProposerPub(t))
		require.True(t, settled)
		require.NotNil(t, outcome)
		require.True(t, *outcome, "102 is in [100, 103)")

		requireUSDCDelta(t, ctx, platform, winner.Address(), winnerStart, "38",
			"100 YES redeem at 98 USDC after the 2% settlement fee")
		requireUSDCDelta(t, ctx, platform, loser.Address(), loserStart, "-40",
			"the losing side is paid nothing")
		require.Equal(t, 1, countRows(t, ctx, platform, "SELECT 1 FROM ob_fee_distributions WHERE query_id = $id",
			map[string]any{"$id": queryID}), "a YES market distributes its fee as before")

		return nil
	}
}

// =============================================================================
// create_market refuses what could never settle
// =============================================================================

func testOutcomeCreateMarketRefusals(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		now := time.Now().Unix()
		at := now - 7200
		fx := setupOutcomeFixture(t, ctx, platform, "0x2222222222222222222222222222222222222222",
			"stoutcomerefusals000000000000000", now, []outcomeRecord{{at - 60, "102"}})

		composedID := "stoutcomecomposed000000000000000"
		mustCallAction(t, fx.helper.NewEngineContext(), platform, "create_stream", []any{composedID, "composed"}, nil)

		index := outcomeArgs{action: outcomeIndexChange, at: at, window: outcomeDay, tolerance: 3600, interval: outcomeDay}.withBounds("1", "3")
		above := outcomeArgs{action: outcomeAbove, at: at, window: outcomeDay, tolerance: 3600}.withThreshold("100")
		inRange := outcomeArgs{action: outcomeInRange, at: at, window: outcomeDay, tolerance: 3600}.withBounds("1", "2")

		modify := func(a outcomeArgs, f func(*outcomeArgs)) outcomeArgs {
			f(&a)
			return a
		}
		cases := []struct {
			args outcomeArgs
			want string
		}{
			{modify(above, func(a *outcomeArgs) { a.at = -1 }), "'timestamp' must not be negative"},
			{modify(above, func(a *outcomeArgs) { a.at = now + 315360001 }), "is more than 315360000 seconds after the block time"},
			{modify(above, func(a *outcomeArgs) { a.window = 0 }), "'window' must be in (0, 315360000]"},
			{modify(above, func(a *outcomeArgs) { a.tolerance = 3888001; a.window = 4000000 }), "'tolerance' must be in [0, 3888000]"},
			{modify(above, func(a *outcomeArgs) { a.tolerance = 7200; a.window = 3600 }), "must not be less than 'tolerance'"},
			{modify(above, func(a *outcomeArgs) { a.threshold = nil }), "requires a 'threshold' parameter"},
			{modify(index, func(a *outcomeArgs) { a.interval = 7200 }), "must be more than twice 'tolerance'"},
			{modify(index, func(a *outcomeArgs) { a.interval = 0 }), "'time_interval' must be in (0, 315360000]"},
			{modify(index, func(a *outcomeArgs) { a.min, a.max = nil, nil }), "got both NULL"},
			{index.withBounds("3", "1"), "must be less than 'max_change'"},
			{modify(inRange, func(a *outcomeArgs) { a.min, a.max = nil, nil }), "got both NULL"},
			{inRange.withBounds("2", "2"), "must be less than 'max_value'"},
		}
		for _, c := range cases {
			err := fx.createMarketErr(t, platform, c.args.action, fx.encode(t, c.args))
			require.Error(t, err, "%s: %s", c.args.action, c.want)
			require.Contains(t, err.Error(), c.want)
		}

		// Arg counts and types the engine would refuse when it runs the action.
		raw := func(args ...any) []byte {
			encoded, err := tn_utils.EncodeActionArgs(args)
			require.NoError(t, err)
			return encoded
		}
		narrow, err := kwilTypes.ParseDecimalExplicit("100", 36, 10)
		require.NoError(t, err)
		for _, c := range []struct {
			action string
			args   []byte
			want   string
		}{
			{outcomeAbove, raw(fx.dataProvider, fx.streamID, at, outcomeDay, int64(3600)), "requires exactly 6 args, got 5"},
			{outcomeAbove, raw(fx.dataProvider, fx.streamID, at, nil, int64(3600), mustDecimal(t, "100")), "requires a 'window' parameter, got NULL"},
			{outcomeAbove, raw(fx.dataProvider, fx.streamID, at, outcomeDay, int64(3600), narrow), "must be NUMERIC(36,18), got NUMERIC(36,10)"},
			{outcomeAbove, raw(fx.dataProvider, fx.streamID, "now", outcomeDay, int64(3600), mustDecimal(t, "100")), "'timestamp' must be INT8"},
		} {
			err := fx.createMarketErr(t, platform, c.action, c.args)
			require.Error(t, err, c.want)
			require.Contains(t, err.Error(), c.want)
		}

		// A composed stream, and a stream that does not exist.
		composedFx := *fx
		composedFx.streamID = composedID
		err = composedFx.createMarketErr(t, platform, outcomeAbove, composedFx.encode(t, above))
		require.Error(t, err)
		require.Contains(t, err.Error(), "reads primitive streams only")

		missingFx := *fx
		missingFx.streamID = "stoutcomemissing0000000000000000"
		err = missingFx.createMarketErr(t, platform, outcomeAbove, missingFx.encode(t, above))
		require.Error(t, err)
		require.Contains(t, err.Error(), "Stream not found")

		require.Zero(t, countRows(t, ctx, platform, "SELECT 1 FROM ob_queries", nil), "no refused market is stored")

		// The same arguments, corrected, are accepted for every action.
		for _, args := range []outcomeArgs{index, above, modify(above, func(a *outcomeArgs) { a.action = outcomeBelow }), inRange} {
			fx.createMarket(t, platform, args.action, fx.encode(t, args))
		}
		require.Equal(t, 4, countRows(t, ctx, platform, "SELECT 1 FROM ob_queries", nil))

		return nil
	}
}

// =============================================================================
// The ids agree with the Go registry, and the migration guard fails loudly
// =============================================================================

func testOutcomeRegistration(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		want := map[string]int64{
			outcomeIndexChange: tn_utils.IndexChangeInRangeV2ActionID,
			outcomeAbove:       tn_utils.PriceAboveThresholdV2ActionID,
			outcomeBelow:       tn_utils.PriceBelowThresholdV2ActionID,
			outcomeInRange:     tn_utils.ValueInRangeV2ActionID,
		}
		got := map[string]int64{}
		err := platform.Engine.Execute(readContext(ctx, platform), platform.DB,
			"SELECT action_name, action_id FROM attestation_actions WHERE action_id >= 13 AND action_id <= 16", nil,
			func(row *common.Row) error {
				got[row.Values[0].(string)] = row.Values[1].(int64)
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, want, got, "attestation_actions must match the Go ids")

		insert, guard := outcomeMigrationStatements(t)
		require.NoError(t, deployerExecute(ctx, platform, guard), "the guard passes on the registered ids")
		require.NoError(t, deployerExecute(ctx, platform, insert), "re-applying the insert is a no-op")
		require.NoError(t, deployerExecute(ctx, platform, guard))

		// A name already registered under another id: the insert skips it and the guard fails.
		require.NoError(t, deployerExecute(ctx, platform,
			"UPDATE attestation_actions SET action_id = 99 WHERE action_name = 'value_in_range_v2'"))
		require.NoError(t, deployerExecute(ctx, platform, insert))
		err = deployerExecute(ctx, platform, guard)
		require.Error(t, err, "the guard must fail when a name maps to the wrong id")
		require.Contains(t, err.Error(), "attestation_actions must map")

		return nil
	}
}

// outcomeMigrationStatements returns the INSERT into attestation_actions and the guard that
// follows it, as written in migration 063.
func outcomeMigrationStatements(t *testing.T) (insert, guard string) {
	t.Helper()

	var path string
	for _, p := range migrations.GetSeedScriptPaths() {
		if strings.HasSuffix(p, "063-outcome-attestation-actions.sql") {
			path = p
		}
	}
	require.NotEmpty(t, path, "migration 063 not found")
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	text := string(content)

	insertAt := strings.Index(text, "INSERT INTO attestation_actions")
	require.GreaterOrEqual(t, insertAt, 0)
	insertEnd := strings.Index(text[insertAt:], ";")
	require.Greater(t, insertEnd, 0)
	insert = text[insertAt : insertAt+insertEnd+1]

	guardAt := strings.Index(text, "SELECT error(")
	require.Greater(t, guardAt, insertAt)
	guard = strings.TrimSpace(text[guardAt:])
	return insert, guard
}

// =============================================================================
// Fixture
// =============================================================================

type outcomeRecord struct {
	at    int64
	value string
}

type outcomeArgs struct {
	action                          string
	at, window, tolerance, interval int64
	min, max, threshold             *string
}

func (a outcomeArgs) withBounds(min, max string) outcomeArgs {
	a.min, a.max = &min, &max
	return a
}

func (a outcomeArgs) withMin(min string) outcomeArgs {
	a.min, a.max = &min, nil
	return a
}

func (a outcomeArgs) withMax(max string) outcomeArgs {
	a.min, a.max = nil, &max
	return a
}

func (a outcomeArgs) withThreshold(threshold string) outcomeArgs {
	a.threshold = &threshold
	return a
}

type outcomeFixture struct {
	helper       *attestationTests.AttestationTestHelper
	dataProvider string
	streamID     string
	createAt     int64
	settleTime   int64
}

// setupOutcomeFixture creates a data provider and a primitive stream holding records. Markets
// are created at now and settle an hour later; every capture runs one second after that.
func setupOutcomeFixture(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
	deployerHex string,
	streamID string,
	now int64,
	records []outcomeRecord,
) *outcomeFixture {
	t.Helper()

	// Each function test runs against a fresh container, so the balance chain restarts here.
	lastBalancePoint = nil
	lastTrufBalancePoint = nil

	deployer := util.Unsafe_NewEthereumAddressFromString(deployerHex)
	platform.Deployer = deployer.Bytes()
	helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)

	require.NoError(t, giveBalance(ctx, platform, deployer.Address(), "500000000000000000000"))
	require.NoError(t, setup.CreateDataProvider(ctx, platform, deployer.Address()))

	fx := &outcomeFixture{
		helper:       helper,
		dataProvider: deployer.Address(),
		streamID:     streamID,
		createAt:     now,
		settleTime:   now + 3600,
	}

	// create_stream and insert_records share one engine context, as the other settlement tests do.
	engineCtx := helper.NewEngineContext()
	mustCallAction(t, engineCtx, platform, "create_stream", []any{streamID, "primitive"}, nil)
	if len(records) > 0 {
		var providers, streams []string
		var times []int64
		var values []*kwilTypes.Decimal
		for _, r := range records {
			providers = append(providers, fx.dataProvider)
			streams = append(streams, streamID)
			times = append(times, r.at)
			values = append(values, mustDecimal(t, r.value))
		}
		mustCallAction(t, engineCtx, platform, "insert_records", []any{providers, streams, times, values}, nil)
	}

	return fx
}

// encode encodes an outcome action's args in the order migration 063 declares them.
func (fx *outcomeFixture) encode(t *testing.T, a outcomeArgs) []byte {
	t.Helper()

	// An absent bound is left as an untyped nil: a typed (*Decimal)(nil) does not read back as NULL.
	dec := func(v *string) any {
		if v == nil {
			return nil
		}
		return mustDecimal(t, *v)
	}

	args := []any{fx.dataProvider, fx.streamID, a.at, a.window, a.tolerance}
	switch a.action {
	case outcomeIndexChange:
		args = append(args, a.interval, dec(a.min), dec(a.max))
	case outcomeAbove, outcomeBelow:
		args = append(args, dec(a.threshold))
	case outcomeInRange:
		args = append(args, dec(a.min), dec(a.max))
	default:
		t.Fatalf("unknown outcome action %q", a.action)
	}

	encoded, err := tn_utils.EncodeActionArgs(args)
	require.NoError(t, err)
	return encoded
}

func (fx *outcomeFixture) createMarket(t *testing.T, platform *kwilTesting.Platform, action string, argsBytes []byte) int {
	t.Helper()

	components, err := encodeQueryComponentsForTests(fx.dataProvider, fx.streamID, action, argsBytes)
	require.NoError(t, err)
	return fx.createMarketFromComponents(t, platform, components, fx.settleTime)
}

func (fx *outcomeFixture) createMarketFromComponents(t *testing.T, platform *kwilTesting.Platform, components []byte, settleTime int64) int {
	t.Helper()

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = fx.createAt

	var queryID int
	mustCallAction(t, engineCtx, platform, "create_market",
		[]any{testExtensionName, components, settleTime, int64(5), int64(1)},
		func(row *common.Row) error {
			queryID = int(row.Values[0].(int64))
			return nil
		})
	require.Greater(t, queryID, 0, "queryID should be positive")
	return queryID
}

func (fx *outcomeFixture) createMarketErr(t *testing.T, platform *kwilTesting.Platform, action string, argsBytes []byte) error {
	t.Helper()

	components, err := encodeQueryComponentsForTests(fx.dataProvider, fx.streamID, action, argsBytes)
	require.NoError(t, err)

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = fx.createAt
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
		[]any{testExtensionName, components, fx.settleTime, int64(5), int64(1)}, nil)
	if err != nil {
		return err
	}
	return res.Error
}

// attest requests the capture after the settle time, as the settlement job does, and signs it.
func (fx *outcomeFixture) attest(t *testing.T, platform *kwilTesting.Platform, action string, argsBytes []byte) {
	t.Helper()
	fx.attestAt(t, platform, action, argsBytes, fx.settleTime+1)
}

// attestAt requests the capture at a block time and signs it.
func (fx *outcomeFixture) attestAt(t *testing.T, platform *kwilTesting.Platform, action string, argsBytes []byte, blockTime int64) {
	t.Helper()

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = blockTime

	var requestTxID string
	mustCallAction(t, engineCtx, platform, "request_attestation",
		[]any{fx.dataProvider, fx.streamID, action, argsBytes, false, nil},
		func(row *common.Row) error {
			requestTxID = row.Values[0].(string)
			return nil
		})
	require.NotEmpty(t, requestTxID, "request_attestation should return a tx id")
	fx.helper.SignAttestation(requestTxID)
}

func (fx *outcomeFixture) attestErr(t *testing.T, platform *kwilTesting.Platform, action string, argsBytes []byte) error {
	t.Helper()
	return fx.attestErrAt(t, platform, action, argsBytes, fx.settleTime+1)
}

func (fx *outcomeFixture) attestErrAt(t *testing.T, platform *kwilTesting.Platform, action string, argsBytes []byte, blockTime int64) error {
	t.Helper()

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = blockTime
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
		[]any{fx.dataProvider, fx.streamID, action, argsBytes, false, nil}, nil)
	if err != nil {
		return err
	}
	return res.Error
}

// settle settles the market one second after its settle time and reads back the result.
func (fx *outcomeFixture) settle(t *testing.T, platform *kwilTesting.Platform, queryID int, proposer crypto.PublicKey) (bool, *bool, int64) {
	t.Helper()
	return fx.settleAt(t, platform, queryID, proposer, fx.settleTime+1)
}

// settleAt settles the market at a block time and reads back the result.
func (fx *outcomeFixture) settleAt(t *testing.T, platform *kwilTesting.Platform, queryID int, proposer crypto.PublicKey, blockTime int64) (bool, *bool, int64) {
	t.Helper()

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = blockTime
	if proposer != nil {
		engineCtx.TxContext.BlockContext.Proposer = proposer
	}
	mustCallAction(t, engineCtx, platform, "settle_market", []any{queryID}, nil)

	var settled bool
	var outcome *bool
	var settledAt int64
	err := platform.Engine.Execute(fx.helper.NewEngineContext(), platform.DB,
		`SELECT settled, winning_outcome, settled_at FROM ob_queries WHERE id = $id`,
		map[string]any{"id": queryID},
		func(row *common.Row) error {
			settled = row.Values[0].(bool)
			if row.Values[1] != nil {
				value := row.Values[1].(bool)
				outcome = &value
			}
			if row.Values[2] != nil {
				settledAt = row.Values[2].(int64)
			}
			return nil
		})
	require.NoError(t, err)
	return settled, outcome, settledAt
}

func (fx *outcomeFixture) settleErr(t *testing.T, platform *kwilTesting.Platform, queryID int) error {
	t.Helper()

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = fx.settleTime + 1
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market", []any{queryID}, nil)
	if err != nil {
		return err
	}
	return res.Error
}

// =============================================================================
// Assertions
// =============================================================================

func requirePosition(t *testing.T, positions []Position, participantID int, outcome bool, price int16, amount int64) {
	t.Helper()

	for _, p := range positions {
		if p.ParticipantID == participantID && p.Outcome == outcome && p.Price == price {
			require.Equal(t, amount, p.Amount, "participant %d outcome %t price %d", participantID, outcome, price)
			return
		}
	}
	t.Fatalf("participant %d has no position with outcome %t at price %d", participantID, outcome, price)
}

func usdcBalances(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, wallets []*util.EthereumAddress) map[string]*big.Int {
	t.Helper()

	balances := map[string]*big.Int{}
	for _, w := range wallets {
		balance, err := getUSDCBalance(ctx, platform, w.Address())
		require.NoError(t, err)
		balances[w.Address()] = balance
	}
	return balances
}

func requireDeltaFromStart(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, w *util.EthereumAddress,
	start map[string]*big.Int, expectedUSDC string) {
	t.Helper()
	requireUSDCDelta(t, ctx, platform, w.Address(), start[w.Address()], expectedUSDC, "net USDC of "+w.Address())
}

func readContext(ctx context.Context, platform *kwilTesting.Platform) *common.EngineContext {
	return &common.EngineContext{TxContext: &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		TxID:         platform.Txid(),
	}}
}

func mustDecimal(t *testing.T, value string) *kwilTypes.Decimal {
	t.Helper()
	d, err := kwilTypes.ParseDecimalExplicit(value, 36, 18)
	require.NoError(t, err)
	return d
}

func mustBigInt(t *testing.T, value string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(value, 10)
	require.True(t, ok, "parse %q", value)
	return n
}

func boolPtr(v bool) *bool {
	return &v
}
