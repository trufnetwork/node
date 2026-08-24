package tests

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/testctx"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// The series every test in this file runs against. It is the fixture from
// TestIndexChange, reused on purpose: the percentages get_index_change produces
// for it are already asserted there, so agreement with those numbers is what
// this file is really testing.
//
//	| event_time | value  |   change vs t-1
//	| 1          | 100.00 |
//	| 2          | 102.00 |    2.000000000000000000
//	| 3          | 103.00 |    0.980392156862745098
//	| 4          | 101.00 |   -1.941747572815533981
//	| 6          | 106.00 |    4.950495049504950495  (compares against t=4)
//	| 7          | 105.00 |   -0.943396226415094340
//	| 8          | 108.00 |    2.857142857142857143
const indexChangeFixture = `
	| event_time | value  |
	|------------|--------|
	| 1          | 100.00 |
	| 2          | 102.00 |
	| 3          | 103.00 |
	| 4          | 101.00 |
	# gap at 5, so t=6 compares against t=4
	| 6          | 106.00 |
	| 7          | 105.00 |
	| 8          | 108.00 |
	`

func TestIndexChangeInRange(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "index_change_in_range_test",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			withTestIndexChangeSetup(testIndexChangeInRangeBuckets(t)),
			withTestIndexChangeSetup(testIndexChangeInRangeTails(t)),
			withTestIndexChangeSetup(testIndexChangeInRangeHalfOpenBoundary(t)),
			withTestIndexChangeSetup(testIndexChangeInRangeBucketsTileOnce(t)),
			withTestIndexChangeSetup(testIndexChangeInRangeMatchesIndexChange(t)),
			withTestIndexChangeSetup(testIndexChangeInRangeMatchesIndexChangeComposed(t)),
			withTestIndexChangeSetup(testIndexChangeInRangeRefusesStaleData(t)),
			withTestIndexChangeSetup(testIndexChangeInRangeArgumentErrors(t)),
		},
	}, testutils.GetTestOptionsWithCache())
}

// =============================================================================
// Bucket behaviour
// =============================================================================

func testIndexChangeInRangeBuckets(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID, err := setupIndexChangeStream(ctx, platform, "icr_buckets")
		if err != nil {
			return err
		}

		// change at t=8 is 2.857142857142857143
		call := indexChangeCall{streamID: streamID, at: 8, interval: 1}

		result, err := callIndexChangeInRange(t, ctx, platform, call.withBounds("2", "3"))
		require.NoError(t, err)
		require.True(t, result, "2.857 sits inside [2, 3)")

		result, err = callIndexChangeInRange(t, ctx, platform, call.withBounds("3", "4"))
		require.NoError(t, err)
		require.False(t, result, "2.857 sits below [3, 4)")

		result, err = callIndexChangeInRange(t, ctx, platform, call.withBounds("1", "2"))
		require.NoError(t, err)
		require.False(t, result, "2.857 sits above [1, 2)")

		// A negative change still resolves. At t=4 the index fell 1.94%.
		negative := indexChangeCall{streamID: streamID, at: 4, interval: 1}

		result, err = callIndexChangeInRange(t, ctx, platform, negative.withBounds("-2", "-1"))
		require.NoError(t, err)
		require.True(t, result, "-1.941 sits inside [-2, -1)")

		result, err = callIndexChangeInRange(t, ctx, platform, negative.withBounds("0", "1"))
		require.NoError(t, err)
		require.False(t, result, "a fall is not a rise")

		return nil
	}
}

func testIndexChangeInRangeTails(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID, err := setupIndexChangeStream(ctx, platform, "icr_tails")
		if err != nil {
			return err
		}

		// change at t=8 is 2.857142857142857143
		call := indexChangeCall{streamID: streamID, at: 8, interval: 1}

		result, err := callIndexChangeInRange(t, ctx, platform, call.withMax("3"))
		require.NoError(t, err)
		require.True(t, result, "open lower tail: 2.857 is below 3")

		result, err = callIndexChangeInRange(t, ctx, platform, call.withMax("2"))
		require.NoError(t, err)
		require.False(t, result, "open lower tail: 2.857 is not below 2")

		result, err = callIndexChangeInRange(t, ctx, platform, call.withMin("2"))
		require.NoError(t, err)
		require.True(t, result, "open upper tail: 2.857 is at or above 2")

		result, err = callIndexChangeInRange(t, ctx, platform, call.withMin("3"))
		require.NoError(t, err)
		require.False(t, result, "open upper tail: 2.857 is not at or above 3")

		// min 0 with an open upper tail is the goal's own headline market,
		// "did the rate rise at all?".
		result, err = callIndexChangeInRange(t, ctx, platform, call.withMin("0"))
		require.NoError(t, err)
		require.True(t, result, "the index rose between t=7 and t=8")

		fell := indexChangeCall{streamID: streamID, at: 7, interval: 1}
		result, err = callIndexChangeInRange(t, ctx, platform, fell.withMin("0"))
		require.NoError(t, err)
		require.False(t, result, "the index fell between t=6 and t=7")

		return nil
	}
}

// testIndexChangeInRangeHalfOpenBoundary pins the [min, max) convention. The
// change at t=2 is exactly 2, which is the only reason this assertion can be
// made without relying on a rounding accident.
func testIndexChangeInRangeHalfOpenBoundary(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID, err := setupIndexChangeStream(ctx, platform, "icr_boundary")
		if err != nil {
			return err
		}

		// change at t=2 is 2.000000000000000000, exactly
		call := indexChangeCall{streamID: streamID, at: 2, interval: 1}

		result, err := callIndexChangeInRange(t, ctx, platform, call.withBounds("2", "3"))
		require.NoError(t, err)
		require.True(t, result, "a value on the lower bound belongs to that bucket")

		result, err = callIndexChangeInRange(t, ctx, platform, call.withBounds("1", "2"))
		require.NoError(t, err)
		require.False(t, result, "a value on the upper bound belongs to the next bucket up")

		return nil
	}
}

// testIndexChangeInRangeBucketsTileOnce is the property the half-open
// convention exists for: across the five buckets of a real market, exactly one
// resolves TRUE, including when the change lands exactly on an interior
// boundary. The 040 family fails this, because value_in_range is inclusive on
// both ends.
func testIndexChangeInRangeBucketsTileOnce(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID, err := setupIndexChangeStream(ctx, platform, "icr_tiling")
		if err != nil {
			return err
		}

		// Boundaries chosen so that t=2's change of exactly 2 lands on an
		// interior boundary, the case that double-resolves under inclusive
		// bounds.
		boundaries := []string{"0", "1", "2", "3"}

		for _, at := range []int64{2, 3, 4, 6, 7, 8} {
			call := indexChangeCall{streamID: streamID, at: at, interval: 1}
			trueCount := 0

			// Below the lowest boundary.
			result, err := callIndexChangeInRange(t, ctx, platform, call.withMax(boundaries[0]))
			require.NoError(t, err)
			if result {
				trueCount++
			}

			// The three interior buckets.
			for i := 0; i < len(boundaries)-1; i++ {
				result, err = callIndexChangeInRange(t, ctx, platform, call.withBounds(boundaries[i], boundaries[i+1]))
				require.NoError(t, err)
				if result {
					trueCount++
				}
			}

			// At or above the highest boundary.
			result, err = callIndexChangeInRange(t, ctx, platform, call.withMin(boundaries[len(boundaries)-1]))
			require.NoError(t, err)
			if result {
				trueCount++
			}

			require.Equal(t, 1, trueCount, "exactly one bucket must resolve TRUE at t=%d", at)
		}

		return nil
	}
}

// =============================================================================
// Agreement with get_index_change
// =============================================================================

// testIndexChangeInRangeMatchesIndexChange is the assertion the whole action
// exists to satisfy: the number it settles on has to be the number the rest of
// the product displays.
func testIndexChangeInRangeMatchesIndexChange(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID, err := setupIndexChangeStream(ctx, platform, "icr_agrees")
		if err != nil {
			return err
		}

		locator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("icr_agrees"),
			DataProvider: defaultDeployer,
		}

		// t=6 is the interesting one: the series has no record at t=5, so both
		// this action and get_index_change have to fall back to t=4.
		for _, at := range []int64{2, 3, 4, 6, 7, 8} {
			assertAgreesWithIndexChange(t, ctx, platform, locator, streamID, at, 1)
		}

		return nil
	}
}

// testIndexChangeInRangeMatchesIndexChangeComposed runs the same agreement
// check against a composed stream with unequal children.
//
// This is the case that decides how the action reads its values. A composed
// index weights its children after indexing them, so the ratio of composed raw
// records is not the ratio of the composed index. Reading through get_index is
// what keeps the two in step; reading primitive_events directly, the way the
// 040 actions do, would not.
func testIndexChangeInRangeMatchesIndexChangeComposed(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamName := "icr_composed"
		streamID := util.GenerateStreamId(streamName)

		if err := setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: streamID,
			Height:   0,
			// Children move by different amounts, so the weighted index and the
			// weighted raw value diverge.
			MarkdownData: `
			| event_time | value_1 | value_2 |
			|------------|---------|---------|
			| 1          | 100     | 50      |
			| 2          | 102     | 60      |
			| 3          | 103     | 55      |
			| 4          | 101     | 70      |
			`,
			Weights: []string{"1", "3"},
		}); err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		locator := types.StreamLocator{
			StreamId:     streamID,
			DataProvider: defaultDeployer,
		}

		for _, at := range []int64{2, 3, 4} {
			assertAgreesWithIndexChange(t, ctx, platform, locator, streamID.String(), at, 1)
		}

		return nil
	}
}

// assertAgreesWithIndexChange proves the action computed exactly the value
// get_index_change reports, by squeezing it between two probes.
//
// The action returns a boolean, so the value cannot be read out directly. But
// `[V, ∞)` resolving TRUE means change >= V, and `[V + 1ulp, ∞)` resolving
// FALSE means change < V + 1ulp. Together, at 18 decimal places, change == V.
func assertAgreesWithIndexChange(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
	locator types.StreamLocator,
	streamID string,
	at int64,
	interval int,
) {
	t.Helper()

	from, to := at, at
	rows, err := procedure.GetIndexChange(ctx, procedure.GetIndexChangeInput{
		Platform:      platform,
		StreamLocator: locator,
		FromTime:      &from,
		ToTime:        &to,
		Interval:      &interval,
		Height:        0,
	})
	require.NoError(t, err, "get_index_change at t=%d", at)
	require.Len(t, rows, 1, "get_index_change should return one row at t=%d", at)

	expected := rows[0][1]
	call := indexChangeCall{streamID: streamID, at: at, interval: interval}

	atOrAbove, err := callIndexChangeInRange(t, ctx, platform, call.withMin(expected))
	require.NoError(t, err)
	require.True(t, atOrAbove, "change at t=%d should be at or above get_index_change's %s", at, expected)

	oneUlpHigher := nextAfterFixedPoint(t, expected)
	belowNext, err := callIndexChangeInRange(t, ctx, platform, call.withMin(oneUlpHigher))
	require.NoError(t, err)
	require.False(t, belowNext, "change at t=%d should be below %s", at, oneUlpHigher)

	t.Logf("t=%d: index_change_in_range agrees with get_index_change at %s", at, expected)
}

// =============================================================================
// Refusals
// =============================================================================

func testIndexChangeInRangeRefusesStaleData(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID, err := setupIndexChangeStream(ctx, platform, "icr_stale")
		if err != nil {
			return err
		}

		// The current anchor keeps a one-day freshness rule. The series ends at
		// t=8, so a market settling two days later has nothing fresh to settle
		// on, even though get_index would happily hand back t=8 as its LOCF
		// anchor.
		_, err = callIndexChangeInRange(t, ctx, platform, indexChangeCall{
			streamID: streamID,
			at:       2 * 86400,
			interval: 1,
		}.withMin("0"))
		require.Error(t, err, "a two-day-old value should not settle a market")
		require.Contains(t, err.Error(), "no value within")

		// The prior anchor scales with the interval asked for. Looking back one
		// second from t=8 wants a comparison point no older than t=6; the record
		// at t=7 satisfies that.
		result, err := callIndexChangeInRange(t, ctx, platform, indexChangeCall{
			streamID: streamID,
			at:       8,
			interval: 1,
		}.withMin("0"))
		require.NoError(t, err)
		require.True(t, result)

		// Reaching back further than the series exists refuses rather than
		// comparing against nothing.
		_, err = callIndexChangeInRange(t, ctx, platform, indexChangeCall{
			streamID: streamID,
			at:       8,
			interval: 100,
		}.withMin("0"))
		require.Error(t, err, "there is no record at or before t=-92")
		require.Contains(t, err.Error(), "No data at or before")

		return nil
	}
}

func testIndexChangeInRangeArgumentErrors(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID, err := setupIndexChangeStream(ctx, platform, "icr_args")
		if err != nil {
			return err
		}

		base := indexChangeCall{streamID: streamID, at: 8, interval: 1}

		_, err = callIndexChangeInRange(t, ctx, platform, base)
		require.Error(t, err, "a market with no bounds would always resolve TRUE")
		require.Contains(t, err.Error(), "at least one of min_change or max_change")

		zeroInterval := base.withMin("0")
		zeroInterval.interval = 0
		_, err = callIndexChangeInRange(t, ctx, platform, zeroInterval)
		require.Error(t, err, "a zero interval is not a change over anything")
		require.Contains(t, err.Error(), "time_interval must be positive")

		negativeInterval := base.withMin("0")
		negativeInterval.interval = -1
		_, err = callIndexChangeInRange(t, ctx, platform, negativeInterval)
		require.Error(t, err, "a negative interval would place the anchor in the future")
		require.Contains(t, err.Error(), "time_interval must be positive")

		_, err = callIndexChangeInRange(t, ctx, platform, base.withBounds("3", "2"))
		require.Error(t, err, "an inverted bucket can never resolve TRUE")
		require.Contains(t, err.Error(), "min_change must be less than max_change")

		// A market cannot resolve before its settlement time.
		future := base.withMin("0")
		future.now = 1
		_, err = callIndexChangeInRange(t, ctx, platform, future)
		require.Error(t, err, "settling before the settlement time should be refused")
		require.Contains(t, err.Error(), "Cannot resolve market before target timestamp")

		return nil
	}
}

// =============================================================================
// Helpers
// =============================================================================

// indexChangeCall is one invocation of index_change_in_range. Bounds are held
// as strings so a test can say "2" and let the helper decide whether that means
// a decimal or a NULL.
type indexChangeCall struct {
	streamID  string
	at        int64
	interval  int
	minChange *string
	maxChange *string
	// now overrides the block timestamp. Zero means "the settlement time",
	// which is the normal case: a market settles at or after its settle_time.
	now int64
}

func (c indexChangeCall) withBounds(min, max string) indexChangeCall {
	c.minChange, c.maxChange = &min, &max
	return c
}

func (c indexChangeCall) withMin(min string) indexChangeCall {
	c.minChange, c.maxChange = &min, nil
	return c
}

func (c indexChangeCall) withMax(max string) indexChangeCall {
	c.minChange, c.maxChange = nil, &max
	return c
}

func setupIndexChangeStream(ctx context.Context, platform *kwilTesting.Platform, name string) (string, error) {
	streamID := util.GenerateStreamId(name)
	if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
		Platform:     platform,
		StreamId:     streamID,
		Height:       0,
		MarkdownData: indexChangeFixture,
	}); err != nil {
		return "", errors.Wrap(err, "error setting up primitive stream")
	}
	return streamID.String(), nil
}

// callIndexChangeInRange returns the action's boolean, or the error it raised.
// Action errors surface in res.Error rather than in err, so both are checked.
func callIndexChangeInRange(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
	call indexChangeCall,
) (bool, error) {
	t.Helper()

	engineCtx := testctx.NewEngineContext(ctx, platform, defaultDeployer, 0)
	blockTimestamp := call.now
	if blockTimestamp == 0 {
		blockTimestamp = call.at
	}
	engineCtx.TxContext.BlockContext.Timestamp = blockTimestamp

	// Left as an untyped nil when absent. A typed (*Decimal)(nil) does not read
	// back as SQL NULL, which would turn an open tail into a silent zero bound.
	toArg := func(v *string) (any, error) {
		if v == nil {
			return nil, nil
		}
		d, err := kwilTypes.ParseDecimalExplicit(*v, 36, 18)
		if err != nil {
			return nil, err
		}
		return d, nil
	}

	minArg, err := toArg(call.minChange)
	if err != nil {
		return false, errors.Wrap(err, "parse min_change")
	}
	maxArg, err := toArg(call.maxChange)
	if err != nil {
		return false, errors.Wrap(err, "parse max_change")
	}

	var result bool
	var gotRow bool
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "index_change_in_range",
		[]any{
			defaultDeployer.Address(),
			call.streamID,
			call.at,
			nil, // base_time
			call.interval,
			minArg,
			maxArg,
			nil, // frozen_at
		},
		func(row *common.Row) error {
			result = row.Values[0].(bool)
			gotRow = true
			return nil
		})
	if err != nil {
		return false, err
	}
	if res.Error != nil {
		return false, res.Error
	}
	require.True(t, gotRow, "index_change_in_range returned no row")
	return result, nil
}

// nextAfterFixedPoint adds one unit in the last place of an 18-decimal
// fixed-point string, so "2.857142857142857143" becomes
// "2.857142857142857144" and "-1.941747572815533981" becomes
// "-1.941747572815533980".
func nextAfterFixedPoint(t *testing.T, value string) string {
	t.Helper()

	const scale = 18

	negative := strings.HasPrefix(value, "-")
	digits := strings.Replace(strings.TrimPrefix(value, "-"), ".", "", 1)

	scaled, ok := new(big.Int).SetString(digits, 10)
	require.True(t, ok, "parse %q as fixed point", value)
	if negative {
		scaled.Neg(scaled)
	}
	scaled.Add(scaled, big.NewInt(1))

	sign := ""
	if scaled.Sign() < 0 {
		sign = "-"
		scaled.Neg(scaled)
	}

	padded := scaled.String()
	if len(padded) <= scale {
		padded = strings.Repeat("0", scale-len(padded)+1) + padded
	}
	split := len(padded) - scale
	return sign + padded[:split] + "." + padded[split:]
}
