package tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/testctx"
	"github.com/trufnetwork/sdk-go/core/util"
)

// A stream that stops moving. Nothing is published after t=4, which is the shape
// every stream takes once it only broadcasts on a change — and the shape every
// stream takes for the pruned part of its history.
//
//	| event_time | value  |
//	| 1          | 100.00 |
//	| 2          | 105.00 |
//	| 3          |  95.00 |
//	| 4          | 101.00 |   last value published; stands from here on
const highLowFixture = `
	| event_time | value  |
	|------------|--------|
	| 1          | 100.00 |
	| 2          | 105.00 |
	| 3          |  95.00 |
	| 4          | 101.00 |
	`

// highLowSeedHeight is the block height the fixture is written at, so a test can
// pass a frozen_at below it and hide the whole stream.
const highLowSeedHeight = 10

func TestHighLowValue(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "high_low_value_test",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			withTestIndexChangeSetup(testHighLowQuietRange(t)),
			withTestIndexChangeSetup(testHighLowAgreesWithComposed(t)),
			withTestIndexChangeSetup(testHighLowMixesAnchorAndRange(t)),
			withTestIndexChangeSetup(testHighLowBeforeFirstRecord(t)),
			withTestIndexChangeSetup(testHighLowFrozenAtHidesAnchor(t)),
			withTestIndexChangeSetup(testHighLowUnchangedInsideRange(t)),
		},
	}, testutils.GetTestOptionsWithCache())
}

// =============================================================================
// The defect this file exists for
// =============================================================================

// A range in which the stream published nothing must report the value that was
// standing, not an empty answer. Before the anchor was added both actions
// returned zero rows here — and because the attestation path encodes an empty
// row set rather than raising, that produced a signed, fee-charged attestation
// carrying no datapoint at all.
func testHighLowQuietRange(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID, err := setupHighLowStream(ctx, platform, "hl_quiet", highLowFixture)
		if err != nil {
			return err
		}

		high, err := callHighLow(t, ctx, platform, "get_high_value", streamID, 10, 20, nil)
		require.NoError(t, err)
		require.NotNil(t, high, "get_high_value must not go silent over a quiet range")
		require.Equal(t, "101.000000000000000000", high.value)
		require.EqualValues(t, 4, high.eventTime, "the anchor keeps its own event_time")

		low, err := callHighLow(t, ctx, platform, "get_low_value", streamID, 10, 20, nil)
		require.NoError(t, err)
		require.NotNil(t, low, "get_low_value must not go silent over a quiet range")
		require.Equal(t, "101.000000000000000000", low.value)
		require.EqualValues(t, 4, low.eventTime)

		return nil
	}
}

// The primitive and composed branches of the same public action have to answer
// the same question the same way. The composed branch has always anchored,
// because it walks get_record; this is the assertion that pins the two together.
func testHighLowAgreesWithComposed(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		primitiveID, err := setupHighLowStream(ctx, platform, "hl_agree_primitive", highLowFixture)
		if err != nil {
			return err
		}

		composedID := util.GenerateStreamId("hl_agree_composed")
		if err := setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedID,
			Height:   highLowSeedHeight,
			MarkdownData: `
			| event_time | hl_agree_child |
			|------------|----------------|
			| 1          | 100.00         |
			| 2          | 105.00         |
			| 3          |  95.00         |
			| 4          | 101.00         |
			`,
		}); err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		for _, action := range []string{"get_high_value", "get_low_value"} {
			fromPrimitive, err := callHighLow(t, ctx, platform, action, primitiveID, 10, 20, nil)
			require.NoError(t, err)
			fromComposed, err := callHighLow(t, ctx, platform, action, composedID.String(), 10, 20, nil)
			require.NoError(t, err)

			require.NotNil(t, fromPrimitive, "%s went silent on the primitive stream", action)
			require.NotNil(t, fromComposed, "%s went silent on the composed stream", action)
			require.Equal(t, fromComposed.value, fromPrimitive.value,
				"%s disagrees between the primitive and composed branches", action)
			require.Equal(t, fromComposed.eventTime, fromPrimitive.eventTime,
				"%s reports a different event_time on each branch", action)
		}

		return nil
	}
}

// =============================================================================
// The anchor competing with in-range rows
// =============================================================================

// With one in-range record below the carried-forward value, the high is the
// anchor and the low is the in-range row. This is the case that proves the
// anchor joins the comparison rather than replacing it.
func testHighLowMixesAnchorAndRange(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID, err := setupHighLowStream(ctx, platform, "hl_mixed", `
			| event_time | value  |
			|------------|--------|
			| 1          | 100.00 |
			| 4          | 101.00 |
			| 12         |  90.00 |
			`)
		if err != nil {
			return err
		}

		high, err := callHighLow(t, ctx, platform, "get_high_value", streamID, 10, 20, nil)
		require.NoError(t, err)
		require.NotNil(t, high)
		require.Equal(t, "101.000000000000000000", high.value, "the anchor is higher than anything in range")
		require.EqualValues(t, 4, high.eventTime)

		low, err := callHighLow(t, ctx, platform, "get_low_value", streamID, 10, 20, nil)
		require.NoError(t, err)
		require.NotNil(t, low)
		require.Equal(t, "90.000000000000000000", low.value, "the in-range row is lower than the anchor")
		require.EqualValues(t, 12, low.eventTime)

		return nil
	}
}

// A range whose extreme ties the carried-forward value reports the anchor's
// event_time, which sits before `from`. That is deliberate: it is what the
// composed branch already does, and after duplicate pruning the in-range row
// that ties will usually not exist at all.
func testHighLowUnchangedInsideRange(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID, err := setupHighLowStream(ctx, platform, "hl_tie", `
			| event_time | value  |
			|------------|--------|
			| 4          | 101.00 |
			| 12         | 101.00 |
			`)
		if err != nil {
			return err
		}

		high, err := callHighLow(t, ctx, platform, "get_high_value", streamID, 10, 20, nil)
		require.NoError(t, err)
		require.NotNil(t, high)
		require.Equal(t, "101.000000000000000000", high.value)
		require.EqualValues(t, 4, high.eventTime, "on a tie the earliest row wins, and that is the anchor")

		return nil
	}
}

// =============================================================================
// Cases that must still answer with nothing
// =============================================================================

// Nothing was ever published at or before the range, so there is no value to
// carry forward. Anchoring must not invent one.
func testHighLowBeforeFirstRecord(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID, err := setupHighLowStream(ctx, platform, "hl_before_first", highLowFixture)
		if err != nil {
			return err
		}

		for _, action := range []string{"get_high_value", "get_low_value"} {
			got, err := callHighLow(t, ctx, platform, action, streamID, 0, 0, nil)
			require.NoError(t, err)
			require.Nil(t, got, "%s must return nothing before the stream's first record", action)
		}

		return nil
	}
}

// frozen_at applies to the anchor too. A caller reading the chain as it stood
// before the stream existed must not see through the freeze.
func testHighLowFrozenAtHidesAnchor(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID, err := setupHighLowStream(ctx, platform, "hl_frozen", highLowFixture)
		if err != nil {
			return err
		}

		frozenBeforeSeed := int64(highLowSeedHeight - 1)
		for _, action := range []string{"get_high_value", "get_low_value"} {
			got, err := callHighLow(t, ctx, platform, action, streamID, 10, 20, &frozenBeforeSeed)
			require.NoError(t, err)
			require.Nil(t, got, "%s let the anchor through a frozen_at that predates it", action)
		}

		return nil
	}
}

// =============================================================================
// Helpers
// =============================================================================

// highLowResult is the single row these actions return. A nil *highLowResult
// means the action returned no row at all, which is the state this file is
// mostly about.
type highLowResult struct {
	eventTime int64
	value     string
}

func setupHighLowStream(ctx context.Context, platform *kwilTesting.Platform, name, fixture string) (string, error) {
	streamID := util.GenerateStreamId(name)
	if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
		Platform:     platform,
		StreamId:     streamID,
		Height:       highLowSeedHeight,
		MarkdownData: fixture,
	}); err != nil {
		return "", errors.Wrap(err, "error setting up primitive stream")
	}
	return streamID.String(), nil
}

// callHighLow invokes get_high_value or get_low_value and returns the row, or
// nil when the action produced none. Action errors surface in res.Error rather
// than in err, so both are checked.
func callHighLow(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
	action string,
	streamID string,
	from, to int64,
	frozenAt *int64,
) (*highLowResult, error) {
	t.Helper()

	engineCtx := testctx.NewEngineContext(ctx, platform, defaultDeployer, 0)

	// Left as an untyped nil when absent, so the action sees SQL NULL and applies
	// its own max_int8 default rather than a silent zero.
	var frozenArg any
	if frozenAt != nil {
		frozenArg = *frozenAt
	}

	var out *highLowResult
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", action,
		[]any{
			defaultDeployer.Address(),
			streamID,
			from,
			to,
			frozenArg,
		},
		func(row *common.Row) error {
			out = &highLowResult{
				eventTime: row.Values[0].(int64),
				value:     row.Values[1].(*kwilTypes.Decimal).String(),
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, res.Error
	}
	return out, nil
}
