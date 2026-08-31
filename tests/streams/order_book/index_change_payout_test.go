//go:build kwiltest

package order_book

import (
	"context"
	"math/big"
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
	"github.com/trufnetwork/node/tests/streams/utils/feefund"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"

	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
)

// Money changing hands on a market that settles through index_change_in_range (action id 12).
//
// index_change_settlement_test.go covers how such a market resolves: the YES/NO answer, the
// agreement between the market hash and the attestation hash, and the half-open bucket rule.
// Every market in that file settles against an empty order book, so no position is ever paid.
// Nothing anywhere had put two traders on opposite sides of one and let it settle.
//
// The payout, reward and fee migrations (032, 033, 034) contain no reference to action_id and
// branch on query_id alone, so an index-change market is expected to pay exactly as a value
// market does. That expectation is what this file checks rather than assumes.

const (
	// The bucket the 2% move lands inside. Half-open, so 2% belongs to [1%, 3%) and to nothing
	// else.
	indexChangePayoutMinChange = "1"
	indexChangePayoutMaxChange = "3"

	// One split order of 100 pairs, with the NO side sold at 40 cents.
	indexChangePayoutShares    = int64(100)
	indexChangePayoutTruePrice = 60
	indexChangePayoutNoPrice   = 100 - indexChangePayoutTruePrice
)

func TestIndexChangePayout(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_IndexChangePayout",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testIndexChangeWinnerIsPaidAndLoserIsNot(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// =============================================================================
// A traded index-change market pays the winning side and clears the losing one
// =============================================================================

func testIndexChangeWinnerIsPaidAndLoserIsNot(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// The data provider stays out of the trade so neither trader collects its share of the
		// fees, which would blur the balance arithmetic below.
		dataProvider := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")
		winner := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")
		loser := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")

		// Each function test runs against a fresh container, so the balance chain restarts here.
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		platform.Deployer = dataProvider.Bytes()
		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)

		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		require.NoError(t, setup.CreateDataProvider(ctx, platform, dataProvider.Address()))

		for _, wallet := range []string{
			dataProvider.Address(), winner.Address(), loser.Address(),
		} {
			require.NoError(t, giveBalanceChained(ctx, platform, wallet, "1000000000000000000000"))
		}

		// create_stream (100 TRUF), insert_records (1 TRUF) and request_attestation (40 TRUF) all
		// bill the data provider.
		require.NoError(t, feefund.EnsureWalletFunded(
			ctx, platform, dataProvider.Address(), "200000000000000000000"))

		now := time.Now().Unix()
		fx := &indexChangePayoutFixture{
			helper:       helper,
			dataProvider: dataProvider.Address(),
			streamID:     "stindexchangepayout0000000000000",
			attestAt:     now,
			settleTime:   now + 3600,
		}
		fx.seedStreamMovingTwoPercent(t, platform)

		queryID := fx.createMarket(t, platform, now)

		winnerStart, err := getUSDCBalance(ctx, platform, winner.Address())
		require.NoError(t, err)
		loserStart, err := getUSDCBalance(ctx, platform, loser.Address())
		require.NoError(t, err)

		// The winner mints 100 YES/NO pairs and offers the NO side at 40 cents. The loser lifts
		// it, which is a real match between two wallets rather than one wallet holding both
		// sides, so settlement has someone to pay and someone to pass over.
		require.NoError(t, callPlaceSplitLimitOrder(
			ctx, platform, &winner, queryID, indexChangePayoutTruePrice, indexChangePayoutShares))
		require.NoError(t, callPlaceBuyOrder(
			ctx, platform, &loser, queryID, false, indexChangePayoutNoPrice, indexChangePayoutShares))

		requireHolding(t, ctx, platform, queryID, true, indexChangePayoutShares)
		requireHolding(t, ctx, platform, queryID, false, indexChangePayoutShares)

		// Minting cost 100 USDC and selling the NO side returned 40, so the winner is down 60 and
		// the loser is down the 40 they paid. Checked before settlement so a wrong payout below
		// cannot be mistaken for a wrong trade here.
		requireUSDCDelta(t, ctx, platform, winner.Address(), winnerStart, "-60",
			"minting 100 pairs costs 100 USDC and selling the NO side returns 40")
		requireUSDCDelta(t, ctx, platform, loser.Address(), loserStart, "-40",
			"buying 100 NO at 40 cents costs 40 USDC")

		fx.attest(t, platform)
		outcome := fx.settle(t, platform, queryID)
		require.True(t, outcome, "a 2%% change inside [1%%, 3%%) should settle YES")

		positions, err := getPositions(ctx, platform, queryID)
		require.NoError(t, err)
		require.Empty(t, positions, "settlement should clear every position on the market")

		// The winner's 100 YES redeem at a dollar less the 2% that funds LP rewards, so 98 back
		// against 100 out and 40 in leaves them up 38. The loser is still down their 40: holding
		// the losing side pays nothing, and nothing is clawed back either.
		requireUSDCDelta(t, ctx, platform, winner.Address(), winnerStart, "38",
			"100 YES redeem at 98 USDC after the 2% settlement fee")
		requireUSDCDelta(t, ctx, platform, loser.Address(), loserStart, "-40",
			"the losing side is paid nothing and charged nothing further")

		return nil
	}
}

// =============================================================================
// Fixture
// =============================================================================

// indexChangePayoutFixture is the same 2% move index_change_settlement_test.go uses: the stream
// holds 100.00 one interval before the attestation point and 102.00 just before it.
type indexChangePayoutFixture struct {
	helper       *attestationTests.AttestationTestHelper
	dataProvider string
	streamID     string
	attestAt     int64
	settleTime   int64
}

func (fx *indexChangePayoutFixture) seedStreamMovingTwoPercent(
	t *testing.T,
	platform *kwilTesting.Platform,
) {
	t.Helper()

	priorValue, err := kwilTypes.ParseDecimalExplicit("100.000000000000000000", 36, 18)
	require.NoError(t, err)
	currentValue, err := kwilTypes.ParseDecimalExplicit("102.000000000000000000", 36, 18)
	require.NoError(t, err)

	// create_stream and insert_records share ONE engine context on purpose: the records are not
	// visible to a read in another context within the same test.
	engineCtx := fx.helper.NewEngineContext()
	mustCallAction(t, engineCtx, platform, "create_stream", []any{fx.streamID, "primitive"}, nil)
	mustCallAction(t, engineCtx, platform, "insert_records", []any{
		[]string{fx.dataProvider, fx.dataProvider},
		[]string{fx.streamID, fx.streamID},
		[]int64{
			fx.attestAt - indexChangeSettlementInterval,
			fx.attestAt - indexChangeCurrentAnchorOffset,
		},
		[]*kwilTypes.Decimal{priorValue, currentValue},
	}, nil)
}

func (fx *indexChangePayoutFixture) createMarket(
	t *testing.T,
	platform *kwilTesting.Platform,
	createAt int64,
) int {
	t.Helper()

	queryComponents, err := encodeQueryComponentsForTests(
		fx.dataProvider, fx.streamID, "index_change_in_range", fx.encodeArgs(t))
	require.NoError(t, err)

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = createAt

	var queryID int
	mustCallAction(t, engineCtx, platform, "create_market",
		[]any{testExtensionName, queryComponents, fx.settleTime, int64(5), int64(1)},
		func(row *common.Row) error {
			queryID = int(row.Values[0].(int64))
			return nil
		})
	require.Greater(t, queryID, 0, "queryID should be positive")

	return queryID
}

func (fx *indexChangePayoutFixture) attest(t *testing.T, platform *kwilTesting.Platform) {
	t.Helper()

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = fx.settleTime + 1

	var requestTxID string
	mustCallAction(t, engineCtx, platform, "request_attestation",
		[]any{
			fx.dataProvider,
			fx.streamID,
			"index_change_in_range",
			fx.encodeArgs(t),
			false, // encrypt_sig
			nil,   // max_fee
		},
		func(row *common.Row) error {
			requestTxID = row.Values[0].(string)
			return nil
		})
	require.NotEmpty(t, requestTxID, "request_attestation should return a tx id")

	fx.helper.SignAttestation(requestTxID)
}

// settle settles the market and returns the outcome it settled to.
//
// Unlike the settlement tests, this market holds positions, so process_settlement runs and needs
// a proposer to attribute the validator share of the fees to.
func (fx *indexChangePayoutFixture) settle(
	t *testing.T,
	platform *kwilTesting.Platform,
	queryID int,
) bool {
	t.Helper()

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = fx.settleTime + 1
	engineCtx.TxContext.BlockContext.Proposer = NewTestProposerPub(t)
	mustCallAction(t, engineCtx, platform, "settle_market", []any{queryID}, nil)

	var settled bool
	var winningOutcome *bool
	engineCtx = fx.helper.NewEngineContext()
	err := platform.Engine.Execute(engineCtx, platform.DB,
		`SELECT settled, winning_outcome FROM ob_queries WHERE id = $id`,
		map[string]any{"id": queryID},
		func(row *common.Row) error {
			settled = row.Values[0].(bool)
			if row.Values[1] != nil {
				outcome := row.Values[1].(bool)
				winningOutcome = &outcome
			}
			return nil
		})
	require.NoError(t, err)
	require.True(t, settled, "market should be settled")
	require.NotNil(t, winningOutcome, "winning_outcome should be set")

	return *winningOutcome
}

// encodeArgs encodes action 12's arguments in the order migration 055 declares them.
func (fx *indexChangePayoutFixture) encodeArgs(t *testing.T) []byte {
	t.Helper()

	minChange, err := kwilTypes.ParseDecimalExplicit(indexChangePayoutMinChange, 36, 18)
	require.NoError(t, err)
	maxChange, err := kwilTypes.ParseDecimalExplicit(indexChangePayoutMaxChange, 36, 18)
	require.NoError(t, err)

	argsBytes, err := tn_utils.EncodeActionArgs([]any{
		fx.dataProvider,
		fx.streamID,
		fx.attestAt,
		nil, // base_time
		indexChangeSettlementInterval,
		minChange,
		maxChange,
		nil, // frozen_at
	})
	require.NoError(t, err)

	return argsBytes
}

// =============================================================================
// Assertions
// =============================================================================

// requireHolding asserts that one side of the market is held outright, which is a position at
// price 0 rather than a resting order.
func requireHolding(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
	queryID int,
	outcome bool,
	amount int64,
) {
	t.Helper()

	positions, err := getPositions(ctx, platform, queryID)
	require.NoError(t, err)

	side := "NO"
	if outcome {
		side = "YES"
	}

	for _, position := range positions {
		if position.Outcome == outcome && position.Price == 0 {
			require.Equal(t, amount, position.Amount, "%s holding amount", side)
			return
		}
	}

	t.Fatalf("no %s holding found among %d positions", side, len(positions))
}

// requireUSDCDelta asserts a wallet's movement against a starting balance, in whole USDC.
func requireUSDCDelta(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
	wallet string,
	start *big.Int,
	expectedUSDC string,
	reason string,
) {
	t.Helper()

	current, err := getUSDCBalance(ctx, platform, wallet)
	require.NoError(t, err)

	expected, ok := new(big.Int).SetString(expectedUSDC, 10)
	require.True(t, ok, "bad expected amount %q", expectedUSDC)
	expected.Mul(expected, big.NewInt(1e18))

	require.Equal(t, expected.String(), new(big.Int).Sub(current, start).String(), reason)
}
