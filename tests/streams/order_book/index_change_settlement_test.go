//go:build kwiltest

package order_book

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"

	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
)

// Settlement of a market on index_change_in_range (action id 12).
//
// Every other settlement test in this package resolves a numeric action, where the outcome is
// "value > 0". These are the first markets to settle through parseBinaryActionResult, which is the
// path IsBinaryAction selects. Before this suite, a market on action 12 could be created and
// attested and then failed permanently at settle_market with "unsupported action_id 12" — a string
// the settlement scheduler treats as a permanent failure, so the market was never retried.
//
// Every market here settles on the same movement: the stream holds 100.00 one interval before the
// attestation point and 102.00 just before it, so the change is exactly 2%. Exactly, not
// approximately — testIndexChangeBucketsSettleExactlyOneYes depends on the change landing on a
// bucket boundary rather than inside a bucket.

const (
	// One day, so both anchors sit inside the staleness windows the action enforces: 86,400 seconds
	// for the current anchor, one interval for the prior one.
	indexChangeSettlementInterval = int64(86400)

	// How far before the attestation point the current anchor sits. Any value under the staleness
	// window works; a minute keeps it obviously fresh.
	indexChangeCurrentAnchorOffset = int64(60)
)

func TestIndexChangeSettlement(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_IndexChangeSettlement",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testIndexChangeMarketSettlesYes(t),
			testIndexChangeMarketSettlesNo(t),
			testIndexChangeMarketHashCompatibility(t),
			testIndexChangeBucketsSettleExactlyOneYes(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// =============================================================================
// A market on action 12 settles YES and NO
// =============================================================================

func testIndexChangeMarketSettlesYes(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		fx := setupIndexChangeMarketFixture(t, ctx, platform,
			"0x2222222222222222222222222222222222222222", "stindexchangeyes0000000000000000")

		// 2% falls inside [1%, 3%).
		queryID := fx.createMarket(t, platform, ptrTo("1"), ptrTo("3"))
		fx.attest(t, platform, ptrTo("1"), ptrTo("3"))

		settled, outcome := fx.settle(t, platform, queryID)
		require.True(t, settled, "market should be settled")
		require.NotNil(t, outcome, "winning_outcome should be set")
		require.True(t, *outcome, "a 2%% change inside [1%%, 3%%) should settle YES")

		return nil
	}
}

func testIndexChangeMarketSettlesNo(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		fx := setupIndexChangeMarketFixture(t, ctx, platform,
			"0x3333333333333333333333333333333333333333", "stindexchangeno00000000000000000")

		// 2% falls outside [3%, 5%). The action returns FALSE rather than erroring, so this
		// distinguishes a real NO from a refusal: an error would fail request_attestation and the
		// market would never reach settlement at all.
		queryID := fx.createMarket(t, platform, ptrTo("3"), ptrTo("5"))
		fx.attest(t, platform, ptrTo("3"), ptrTo("5"))

		settled, outcome := fx.settle(t, platform, queryID)
		require.True(t, settled, "market should be settled")
		require.NotNil(t, outcome, "winning_outcome should be set")
		require.False(t, *outcome, "a 2%% change outside [3%%, 5%%) should settle NO")

		return nil
	}
}

// =============================================================================
// The market hash and the attestation hash agree for action 12
// =============================================================================

// A mismatch here would not fail loudly: create_market and request_attestation would both succeed,
// and settle_market would report "Attestation not found" forever after. The scheduler would retry
// such a market until its data aged out.
func testIndexChangeMarketHashCompatibility(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		fx := setupIndexChangeMarketFixture(t, ctx, platform,
			"0x4444444444444444444444444444444444444444", "stindexchangehash000000000000000")

		// An open upper tail, so the NULL bound travels through both the market hash and the
		// attestation hash rather than only through the action body.
		queryID := fx.createMarket(t, platform, ptrTo("1"), nil)

		var marketHash []byte
		engineCtx := fx.helper.NewEngineContext()
		err := platform.Engine.Execute(engineCtx, platform.DB,
			`SELECT hash FROM ob_queries WHERE id = $id`,
			map[string]any{"id": queryID},
			func(row *common.Row) error {
				marketHash = append([]byte(nil), row.Values[0].([]byte)...)
				return nil
			})
		require.NoError(t, err)
		require.Len(t, marketHash, 32, "market hash should be 32 bytes")

		attestationHash := fx.attest(t, platform, ptrTo("1"), nil)
		require.Len(t, attestationHash, 32, "attestation hash should be 32 bytes")

		require.Equal(t, marketHash, attestationHash,
			"market hash must equal attestation hash, or the market can never be settled")

		settled, outcome := fx.settle(t, platform, queryID)
		require.True(t, settled, "market should be settled")
		require.NotNil(t, outcome)
		require.True(t, *outcome, "a 2%% change is above an open-ended 1%% floor")

		return nil
	}
}

// =============================================================================
// Five buckets over one settle time produce exactly one YES
// =============================================================================

// The change is exactly 2%, which is the boundary between the third and fourth buckets. Under the
// half-open [min, max) rule the action implements, the boundary belongs to the bucket it opens and
// to no other, so precisely one market settles YES. Under the inclusive comparison the migration
// 040 family uses for value_in_range, both adjacent buckets would settle YES and a trader holding
// YES in each would be paid twice.
func testIndexChangeBucketsSettleExactlyOneYes(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		fx := setupIndexChangeMarketFixture(t, ctx, platform,
			"0x5555555555555555555555555555555555555555", "stindexchangebuckets000000000000")

		buckets := []struct {
			label    string
			min, max *string
			expected bool
		}{
			{"Below 1%", nil, ptrTo("1"), false},
			{"1% - 1.5%", ptrTo("1"), ptrTo("1.5"), false},
			{"1.5% - 2%", ptrTo("1.5"), ptrTo("2"), false}, // 2% is excluded: max is open
			{"2% - 2.5%", ptrTo("2"), ptrTo("2.5"), true},  // 2% is included: min is closed
			{"Above 2.5%", ptrTo("2.5"), nil, false},
		}

		queryIDs := make([]int, len(buckets))
		for i, bucket := range buckets {
			queryIDs[i] = fx.createMarket(t, platform, bucket.min, bucket.max)
			fx.attest(t, platform, bucket.min, bucket.max)
		}

		yesCount := 0
		for i, bucket := range buckets {
			settled, outcome := fx.settle(t, platform, queryIDs[i])
			require.True(t, settled, "%s: market should be settled", bucket.label)
			require.NotNil(t, outcome, "%s: winning_outcome should be set", bucket.label)
			require.Equal(t, bucket.expected, *outcome, "%s: unexpected outcome", bucket.label)
			if *outcome {
				yesCount++
			}
		}

		require.Equal(t, 1, yesCount,
			"the five buckets must tile the number line exactly once, including at a boundary")

		return nil
	}
}

// =============================================================================
// Fixture
// =============================================================================

// indexChangeMarketFixture is a stream that moved 2% over one interval, plus the timings every
// market in this file shares. The attestation point sits in the past so the anchors can be
// inserted as ordinary records; the settle time sits an hour ahead so create_market accepts it.
type indexChangeMarketFixture struct {
	helper       *attestationTests.AttestationTestHelper
	dataProvider string
	streamID     string
	createAt     int64
	attestAt     int64
	settleTime   int64
}

func setupIndexChangeMarketFixture(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
	deployerHex string,
	streamID string,
) *indexChangeMarketFixture {
	t.Helper()

	// Each function test runs against a fresh container, so the balance chain restarts here.
	lastBalancePoint = nil
	lastTrufBalancePoint = nil

	deployer := util.Unsafe_NewEthereumAddressFromString(deployerHex)
	platform.Deployer = deployer.Bytes()

	helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)

	err := giveBalance(ctx, platform, deployer.Address(), "500000000000000000000")
	require.NoError(t, err)

	err = setup.CreateDataProvider(ctx, platform, deployer.Address())
	require.NoError(t, err)

	now := time.Now().Unix()
	fx := &indexChangeMarketFixture{
		helper:       helper,
		dataProvider: deployer.Address(),
		streamID:     streamID,
		createAt:     now,
		attestAt:     now,
		settleTime:   now + 3600,
	}

	// create_stream and insert_records share ONE engine context on purpose: the records are not
	// visible to a read in another context within the same test.
	engineCtx := helper.NewEngineContext()
	mustCallAction(t, engineCtx, platform, "create_stream", []any{streamID, "primitive"}, nil)

	priorValue, err := kwilTypes.ParseDecimalExplicit("100.000000000000000000", 36, 18)
	require.NoError(t, err)
	currentValue, err := kwilTypes.ParseDecimalExplicit("102.000000000000000000", 36, 18)
	require.NoError(t, err)

	mustCallAction(t, engineCtx, platform, "insert_records", []any{
		[]string{fx.dataProvider, fx.dataProvider},
		[]string{streamID, streamID},
		[]int64{
			fx.attestAt - indexChangeSettlementInterval,
			fx.attestAt - indexChangeCurrentAnchorOffset,
		},
		[]*kwilTypes.Decimal{priorValue, currentValue},
	}, nil)

	return fx
}

// createMarket creates one bucket's market and returns its query id.
func (fx *indexChangeMarketFixture) createMarket(
	t *testing.T,
	platform *kwilTesting.Platform,
	minChange, maxChange *string,
) int {
	t.Helper()

	queryComponents, err := encodeQueryComponentsForTests(
		fx.dataProvider, fx.streamID, "index_change_in_range",
		fx.encodeArgs(t, minChange, maxChange))
	require.NoError(t, err)

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = fx.createAt

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

// attest requests and signs the attestation the market settles on, returning its hash.
//
// The request runs at a block timestamp past the settle time, which is both what
// validate_not_before_timestamp requires of action 12 and what the settlement scheduler does: it
// requests the attestation only once a market's settle time has passed.
func (fx *indexChangeMarketFixture) attest(
	t *testing.T,
	platform *kwilTesting.Platform,
	minChange, maxChange *string,
) []byte {
	t.Helper()

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = fx.settleTime + 1

	var requestTxID string
	var attestationHash []byte
	mustCallAction(t, engineCtx, platform, "request_attestation",
		[]any{
			fx.dataProvider,
			fx.streamID,
			"index_change_in_range",
			fx.encodeArgs(t, minChange, maxChange),
			false, // encrypt_sig
			nil,   // max_fee
		},
		func(row *common.Row) error {
			requestTxID = row.Values[0].(string)
			attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
			return nil
		})
	require.NotEmpty(t, requestTxID, "request_attestation should return a tx id")

	fx.helper.SignAttestation(requestTxID)

	return attestationHash
}

// settle settles the market and reads back what it settled to.
func (fx *indexChangeMarketFixture) settle(
	t *testing.T,
	platform *kwilTesting.Platform,
	queryID int,
) (settled bool, winningOutcome *bool) {
	t.Helper()

	engineCtx := fx.helper.NewEngineContext()
	engineCtx.TxContext.BlockContext.Timestamp = fx.settleTime + 1
	mustCallAction(t, engineCtx, platform, "settle_market", []any{queryID}, nil)

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

	return settled, winningOutcome
}

// encodeArgs encodes action 12's arguments in the order migration 055 declares them.
func (fx *indexChangeMarketFixture) encodeArgs(t *testing.T, minChange, maxChange *string) []byte {
	t.Helper()

	// An absent bound is left as an untyped nil. A typed (*Decimal)(nil) does not read back as SQL
	// NULL, which would turn an open tail into a silent zero bound.
	toArg := func(v *string) any {
		if v == nil {
			return nil
		}
		d, err := kwilTypes.ParseDecimalExplicit(*v, 36, 18)
		require.NoError(t, err)
		return d
	}

	argsBytes, err := tn_utils.EncodeActionArgs([]any{
		fx.dataProvider,
		fx.streamID,
		fx.attestAt,
		nil, // base_time
		indexChangeSettlementInterval,
		toArg(minChange),
		toArg(maxChange),
		nil, // frozen_at
	})
	require.NoError(t, err)

	return argsBytes
}

// mustCallAction runs an action and fails on either a transport error or an action error. An
// ERROR() raised inside an action arrives in res.Error rather than in err, so both are checked.
func mustCallAction(
	t *testing.T,
	engineCtx *common.EngineContext,
	platform *kwilTesting.Platform,
	action string,
	args []any,
	resultFn func(*common.Row) error,
) {
	t.Helper()

	res, err := platform.Engine.Call(engineCtx, platform.DB, "", action, args, resultFn)
	require.NoError(t, err, "%s: engine call failed", action)
	require.NotNil(t, res, "%s: nil call result", action)
	require.NoError(t, res.Error, "%s: action failed", action)
}

func ptrTo(s string) *string {
	return &s
}
