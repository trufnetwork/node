//go:build kwiltest

package order_book

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
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

// A market's fees are distributed once, when it settles, from what the market collected. So
// trigger_fee_distribution refuses a market that has not settled, one whose fees were already
// distributed and one that collected no fees, whatever total the caller asks for.
//
// Each refusal of a network_writer below is checked with collateral in the vault, so it cannot
// pass only because the vault is empty: no balance moves and no payout is recorded.

const (
	// The total a caller asks for: 2 USDC, which is what 100 winning shares pay in fees.
	feeTriggerRequestedTotal = "2000000000000000000"

	// One split order of 100 pairs, with the NO side offered at 40 cents.
	feeTriggerShares    = int64(100)
	feeTriggerTruePrice = 60
)

func TestTriggerFeeDistribution(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_TriggerFeeDistribution",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testTriggerFeeDistributionNeedsNetworkWriter(t),
			testTriggerFeeDistributionNeedsSettledMarket(t),
			testTriggerFeeDistributionPaysAMarketOnce(t),
			testTriggerFeeDistributionNeedsCollectedFees(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// Only a network_writer may call the action.
func testTriggerFeeDistributionNeedsNetworkWriter(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		fx := newFeeTriggerFixture(t, ctx, platform)
		queryID := fx.openMarket(t, platform, "stfeetriggerwriter00000000000000")

		err := callTriggerFeeDistribution(t, ctx, platform, fx.trader, queryID, feeTriggerRequestedTotal)
		require.ErrorContains(t, err, "Only network_writer can trigger fee distribution")

		return nil
	}
}

// An open market has collected nothing: its fee is taken when it settles.
func testTriggerFeeDistributionNeedsSettledMarket(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		fx := newFeeTriggerFixture(t, ctx, platform)
		queryID := fx.openMarket(t, platform, "stfeetriggeropen0000000000000000")
		require.NoError(t, callPlaceSplitLimitOrder(
			ctx, platform, fx.trader, queryID, feeTriggerTruePrice, feeTriggerShares))

		before := fx.usdcBalances(t, ctx, platform)
		err := callTriggerFeeDistribution(t, ctx, platform, fx.dataProvider, queryID, feeTriggerRequestedTotal)
		require.ErrorContains(t, err, "has not settled")

		fx.requireUSDCUnchanged(t, ctx, platform, before)
		require.Zero(t, feeDistributionCount(t, ctx, platform, queryID), "no payout should be recorded")

		return nil
	}
}

// Settlement distributes the fees it collects, so a second payout for the same market is refused.
func testTriggerFeeDistributionPaysAMarketOnce(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		fx := newFeeTriggerFixture(t, ctx, platform)
		queryID := fx.settledMarket(t, ctx, platform, "stfeetriggerpaid0000000000000000", true)
		require.Equal(t, 1, feeDistributionCount(t, ctx, platform, queryID),
			"settlement should pay out the fee it collected")

		before := fx.usdcBalances(t, ctx, platform)
		err := callTriggerFeeDistribution(t, ctx, platform, fx.dataProvider, queryID, feeTriggerRequestedTotal)
		require.ErrorContains(t, err, "were already distributed")

		fx.requireUSDCUnchanged(t, ctx, platform, before)
		require.Equal(t, 1, feeDistributionCount(t, ctx, platform, queryID), "no second payout should be recorded")

		return nil
	}
}

// A market that settled with nothing on its book collected no fee, so it has nothing to pay,
// even while another market's collateral sits in the same vault.
func testTriggerFeeDistributionNeedsCollectedFees(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		fx := newFeeTriggerFixture(t, ctx, platform)

		otherID := fx.openMarket(t, platform, "stfeetriggerother000000000000000")
		require.NoError(t, callPlaceSplitLimitOrder(
			ctx, platform, fx.trader, otherID, feeTriggerTruePrice, feeTriggerShares))

		queryID := fx.settledMarket(t, ctx, platform, "stfeetriggerempty000000000000000", false)
		require.Zero(t, feeDistributionCount(t, ctx, platform, queryID),
			"a market that settles with an empty book collects no fee")

		before := fx.usdcBalances(t, ctx, platform)
		err := callTriggerFeeDistribution(t, ctx, platform, fx.dataProvider, queryID, feeTriggerRequestedTotal)
		require.ErrorContains(t, err, "collected no fees")

		fx.requireUSDCUnchanged(t, ctx, platform, before)
		require.Zero(t, feeDistributionCount(t, ctx, platform, queryID), "no payout should be recorded")

		return nil
	}
}

// =============================================================================
// Fixture
// =============================================================================

// feeTriggerFixture holds a data provider, who is also the network_writer calling the action and
// the wallet a payout's data provider share would go to, and a trader who keeps collateral in
// the vault.
type feeTriggerFixture struct {
	helper       *attestationTests.AttestationTestHelper
	dataProvider *util.EthereumAddress
	trader       *util.EthereumAddress
	settleTime   int64
}

func newFeeTriggerFixture(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) *feeTriggerFixture {
	t.Helper()

	dataProvider := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")
	trader := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

	// Each function test runs against a fresh container, so the balance chain restarts here.
	lastBalancePoint = nil
	lastTrufBalancePoint = nil

	// The helper makes the deployer a network_writer.
	platform.Deployer = dataProvider.Bytes()
	helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)

	require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
	require.NoError(t, setup.CreateDataProvider(ctx, platform, dataProvider.Address()))

	for _, wallet := range []string{dataProvider.Address(), trader.Address()} {
		require.NoError(t, giveBalanceChained(ctx, platform, wallet, "1000000000000000000000"))
	}

	// create_stream (100 TRUF), insert_records (1 TRUF) and request_attestation (40 TRUF) all
	// bill the data provider.
	require.NoError(t, feefund.EnsureWalletFunded(
		ctx, platform, dataProvider.Address(), "200000000000000000000"))

	return &feeTriggerFixture{
		helper:       helper,
		dataProvider: &dataProvider,
		trader:       &trader,
		settleTime:   time.Now().Add(time.Hour).Unix(),
	}
}

// openMarket opens a market that stays open for the whole test.
func (fx *feeTriggerFixture) openMarket(t *testing.T, platform *kwilTesting.Platform, streamID string) int {
	t.Helper()

	queryComponents, err := encodeQueryComponentsForTests(
		fx.dataProvider.Address(), streamID, "get_record", []byte{0x01})
	require.NoError(t, err)

	return fx.createMarket(t, platform, queryComponents, time.Now().Add(24*time.Hour).Unix())
}

// settledMarket seeds a stream that resolves YES, has it attested at the settle time, opens a
// market on it and settles it. With trade set, the trader holds 100 YES at settlement, which is
// what makes settlement collect a fee.
func (fx *feeTriggerFixture) settledMarket(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
	streamID string,
	trade bool,
) int {
	t.Helper()

	dataProvider := fx.dataProvider.Address()

	yes, err := kwilTypes.ParseDecimalExplicit("1.000000000000000000", 36, 18)
	require.NoError(t, err)

	// create_stream and insert_records share one engine context, as the settlement tests do.
	engineCtx := fx.helper.NewEngineContext()
	mustCallAction(t, engineCtx, platform, "create_stream", []any{streamID, "primitive"}, nil)
	mustCallAction(t, engineCtx, platform, "insert_records", []any{
		[]string{dataProvider},
		[]string{streamID},
		[]int64{1000},
		[]*kwilTypes.Decimal{yes},
	}, nil)

	argsBytes, err := tn_utils.EncodeActionArgs([]any{
		dataProvider,
		streamID,
		int64(1500), // before
		nil,         // frozen_at
		false,       // use_cache
	})
	require.NoError(t, err)

	// settle_market only resolves on a capture taken at or after the settle time.
	engineCtx = fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = fx.settleTime
	var requestTxID string
	mustCallAction(t, engineCtx, platform, "request_attestation",
		[]any{dataProvider, streamID, "get_last_record", argsBytes, false, nil},
		func(row *common.Row) error {
			requestTxID = row.Values[0].(string)
			return nil
		})
	require.NotEmpty(t, requestTxID, "request_attestation should return a tx id")
	fx.helper.SignAttestation(requestTxID)

	queryComponents, err := encodeQueryComponentsForTests(dataProvider, streamID, "get_last_record", argsBytes)
	require.NoError(t, err)
	queryID := fx.createMarket(t, platform, queryComponents, fx.settleTime)

	if trade {
		require.NoError(t, callPlaceSplitLimitOrder(
			ctx, platform, fx.trader, queryID, feeTriggerTruePrice, feeTriggerShares))
	}

	engineCtx = fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = fx.settleTime + 1
	mustCallAction(t, engineCtx, platform, "settle_market", []any{queryID}, nil)

	return queryID
}

func (fx *feeTriggerFixture) createMarket(
	t *testing.T,
	platform *kwilTesting.Platform,
	queryComponents []byte,
	settleTime int64,
) int {
	t.Helper()

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = time.Now().Unix()

	var queryID int
	mustCallAction(t, engineCtx, platform, "create_market",
		[]any{testExtensionName, queryComponents, settleTime, int64(5), int64(1)},
		func(row *common.Row) error {
			queryID = int(row.Values[0].(int64))
			return nil
		})
	require.Greater(t, queryID, 0, "queryID should be positive")

	return queryID
}

// usdcBalances reads the data provider's and the trader's USDC balances.
func (fx *feeTriggerFixture) usdcBalances(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
) map[string]*big.Int {
	t.Helper()

	balances := make(map[string]*big.Int)
	for _, wallet := range []string{fx.dataProvider.Address(), fx.trader.Address()} {
		balance, err := getUSDCBalance(ctx, platform, wallet)
		require.NoError(t, err)
		balances[wallet] = balance
	}

	return balances
}

func (fx *feeTriggerFixture) requireUSDCUnchanged(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
	before map[string]*big.Int,
) {
	t.Helper()

	after := fx.usdcBalances(t, ctx, platform)
	for wallet, balance := range before {
		require.Equal(t, balance.String(), after[wallet].String(), "USDC balance of %s moved", wallet)
	}
}

// callTriggerFeeDistribution calls trigger_fee_distribution and returns the action's error, if any.
func callTriggerFeeDistribution(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
	caller *util.EthereumAddress,
	queryID int,
	totalFees string,
) error {
	t.Helper()

	engineCtx := &common.EngineContext{TxContext: &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    1,
			Timestamp: time.Now().Unix(),
		},
		Signer:        caller.Bytes(),
		Caller:        caller.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}}

	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "trigger_fee_distribution",
		[]any{queryID, totalFees}, nil)
	if err != nil {
		return err
	}
	return res.Error
}

// feeDistributionCount counts the recorded fee payouts of a market.
func feeDistributionCount(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, queryID int) int {
	t.Helper()

	engineCtx := &common.EngineContext{TxContext: &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		TxID:         platform.Txid(),
	}}

	var count int64
	err := platform.Engine.Execute(engineCtx, platform.DB,
		"SELECT COUNT(*) FROM ob_fee_distributions WHERE query_id = $query_id",
		map[string]any{"$query_id": queryID},
		func(row *common.Row) error {
			count = row.Values[0].(int64)
			return nil
		})
	require.NoError(t, err)

	return int(count)
}
