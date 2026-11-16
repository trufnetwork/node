//go:build kwiltest

package tests

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/feefund"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/testctx"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// The outcome actions of migration 063 (ids 13 to 16), called directly. Each answers in one row:
// value 1 YES, 0 NO or -1 INVALID. Before block time T, and while the record it needs is missing,
// it errors with 'awaiting print', so a capture reverts and is asked for again; from block time
// T + W on it answers INVALID, so a market whose print never arrives still ends (website#5250).
//
// Every capture here sets the block time, because the deadline is read from @block_timestamp.

const (
	v2Day = int64(86400)

	// The defaults published for month-start series such as Truflation CPI.
	v2MonthStartTolerance = 2 * v2Day
	v2MonthStartWindow    = 90 * v2Day
	v2YearInterval        = 365 * v2Day

	v2IndexChange = "index_change_in_range_v2"
	v2Above       = "price_above_threshold_v2"
	v2Below       = "price_below_threshold_v2"
	v2InRange     = "value_in_range_v2"

	v2Yes     = 1
	v2No      = 0
	v2Invalid = -1
)

func TestOutcomeActions(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "outcome_actions_test",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			withTestIndexChangeSetup(testOutcomeCPIOctober2026(t)),
			withTestIndexChangeSetup(testOutcomeCPINovember2026(t)),
			withTestIndexChangeSetup(testOutcomeCPIDecember2026(t)),
			withTestIndexChangeSetup(testOutcomeReleaseTimeSeries(t)),
			withTestIndexChangeSetup(testOutcomeLatePrintBeforeDeadline(t)),
			withTestIndexChangeSetup(testOutcomeInvalidAtDeadline(t)),
			withTestIndexChangeSetup(testOutcomeWaitsForTimestamp(t)),
			withTestIndexChangeSetup(testOutcomeSkippedCurrentMonth(t)),
			withTestIndexChangeSetup(testOutcomeMissingPriorWaitsThenInvalid(t)),
			withTestIndexChangeSetup(testOutcomeZeroPrior(t)),
			withTestIndexChangeSetup(testOutcomeNearestPrior(t)),
			withTestIndexChangeSetup(testOutcomeDeletedStream(t)),
			withTestIndexChangeSetup(testOutcomeComposedStream(t)),
			withTestIndexChangeSetup(testOutcomePrivateStream(t)),
			withTestIndexChangeSetup(testOutcomeBinaryBoundaries(t)),
			withTestIndexChangeSetup(testOutcomeIndexChangeBoundaries(t)),
			withTestIndexChangeSetup(testOutcomeArgumentErrors(t)),
		},
	}, testutils.GetTestOptionsWithCache())
}

// =============================================================================
// Truflation CPI, with the records the mainnet stream holds
// =============================================================================

// v2CPIRecords are the records of Truflation's CPI stream on mainnet from September 2025, as
// read on 16-Nov-2026: month-start stamps, no 2025-10-01 record because BLS never published
// October 2025, and one stray record stamped at a release run, 2026-11-12T23:30:07Z, that
// cannot be removed.
func v2CPIRecords() []v2Record {
	return []v2Record{
		{v2Date(2025, 9, 1), "324.800"},
		{v2Date(2025, 11, 1), "324.122"},
		{v2Date(2025, 12, 1), "324.054"},
		{v2Date(2026, 1, 1), "325.252"},
		{v2Date(2026, 2, 1), "326.785"},
		{v2Date(2026, 3, 1), "330.213"},
		{v2Date(2026, 4, 1), "333.020"},
		{v2Date(2026, 5, 1), "335.123"},
		{v2Date(2026, 6, 1), "333.952"},
		{v2Date(2026, 7, 1), "333.918"},
		{v2Date(2026, 8, 1), "334.980"},
		{v2Date(2026, 9, 1), "336.940"},
		{v2Date(2026, 10, 1), "337.511"},
		{v2Stamp("2026-11-12T23:30:07Z"), "337.511"},
	}
}

// v2CPILadder is a year-over-year bucket set on one month, with the published month-start
// defaults.
func v2CPILadder(locator types.StreamLocator, at int64) []v2Call {
	edges := []string{"2", "2.5", "3", "3.5"}
	base := v2Call{
		action:    v2IndexChange,
		locator:   locator,
		at:        at,
		window:    v2MonthStartWindow,
		tolerance: v2MonthStartTolerance,
		interval:  v2YearInterval,
	}

	ladder := []v2Call{base.withMax(edges[0])}
	for i := 0; i < len(edges)-1; i++ {
		ladder = append(ladder, base.withBounds(edges[i], edges[i+1]))
	}
	return append(ladder, base.withMin(edges[len(edges)-1]))
}

// October 2026 compares with October 2025, which BLS never published. The outcome action waits
// until the deadline, in case a backfill is still writing the history, and then ends every bucket
// INVALID.
func testOutcomeCPIOctober2026(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		cpi := v2SetupStream(t, ctx, platform, "v2_cpi_october", v2CPIRecords())

		at := v2Date(2026, 10, 1)
		deadline := at + v2MonthStartWindow

		// What the 055 action does with the hole: it compares with September 2025 and settles a
		// 13-month change, (337.511 - 324.800) * 100 / 324.800 = 3.9134...%.
		old := indexChangeCall{streamID: cpi.StreamId.String(), at: at, interval: int(v2YearInterval)}
		resolved, err := callIndexChangeInRange(t, ctx, platform, old.withMin("3.91"))
		require.NoError(t, err)
		require.True(t, resolved, "index_change_in_range reads September 2025 for the missing October")
		resolved, err = callIndexChangeInRange(t, ctx, platform, old.withMin("3.92"))
		require.NoError(t, err)
		require.False(t, resolved)

		for _, call := range v2CPILadder(cpi, at) {
			v2RequireAwaiting(t, ctx, platform, call, v2Stamp("2026-11-16T12:00:00Z"), "no prior record",
				"the October print is there but October 2025 is not")
			v2RequireAwaiting(t, ctx, platform, call, deadline-1, "no prior record",
				"one second before the deadline the market still waits")
			v2RequireAnswer(t, ctx, platform, call, deadline, v2Invalid, at,
				"from the deadline on a missing year-ago record is INVALID, stamped with the current record")
		}

		return nil
	}
}

// November 2026 has no print yet. The only newer record is the stray, 11 days 23 hours past
// T, outside the 2-day tolerance, so the market waits instead of settling October against
// November 2025. If no print comes it ends INVALID; if the print lands before the deadline it
// settles against November 2025 exactly.
func testOutcomeCPINovember2026(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		cpi := v2SetupStream(t, ctx, platform, "v2_cpi_november", v2CPIRecords())

		at := v2Date(2026, 11, 1)
		deadline := at + v2MonthStartWindow
		ladder := v2CPILadder(cpi, at)

		for _, call := range ladder {
			v2RequireAwaiting(t, ctx, platform, call, v2Stamp("2026-11-16T12:00:00Z"),
				fmt.Sprintf("no readable record in [%d, %d]", at, at+v2MonthStartTolerance),
				"the stray record stamped 2026-11-12T23:30:07Z must not stand in for November")
			// Read before the print below is written: had nothing arrived by the deadline, every
			// bucket would end INVALID, stamped with the deadline itself.
			v2RequireAnswer(t, ctx, platform, call, deadline, v2Invalid, deadline,
				"no November print by the deadline")
		}

		// The November print arrives late, before the deadline. Its value is made up.
		v2Insert(t, ctx, platform, cpi, v2Record{at, "338.000"}, 2)
		settleAt := v2Stamp("2026-12-12T23:30:00Z")

		// (338.000 - 324.122) * 100 / 324.122 = 4.2817...%, so only the top bucket is YES.
		for i, call := range ladder {
			want := v2No
			if i == len(ladder)-1 {
				want = v2Yes
			}
			v2RequireAnswer(t, ctx, platform, call, settleAt, want, at, "November against November 2025")
		}
		v2RequireExactChange(t, ctx, platform, ladder[0], settleAt, "338.000", "324.122")

		return nil
	}
}

// December 2026 reads its own print and December 2025 exactly. The stray is stamped before T, so
// it is never read.
func testOutcomeCPIDecember2026(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		records := append(v2CPIRecords(), v2Record{v2Date(2026, 12, 1), "338.200"}) // made-up value
		cpi := v2SetupStream(t, ctx, platform, "v2_cpi_december", records)

		at := v2Date(2026, 12, 1)
		settleAt := v2Stamp("2027-01-15T00:00:00Z")
		ladder := v2CPILadder(cpi, at)

		// (338.200 - 324.054) * 100 / 324.054 = 4.3653...%
		for i, call := range ladder {
			want := v2No
			if i == len(ladder)-1 {
				want = v2Yes
			}
			v2RequireAnswer(t, ctx, platform, call, settleAt, want, at, "December against December 2025")
		}
		v2RequireExactChange(t, ctx, platform, ladder[0], settleAt, "338.200", "324.054")

		return nil
	}
}

// =============================================================================
// A series stamped at release time
// =============================================================================

// T sits after the previous release and before the earliest plausible stamp of the next one,
// and the tolerance covers the latest plausible stamp. The prior is whichever release sits
// nearest a year back, within the tolerance.
func testOutcomeReleaseTimeSeries(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		stream := v2SetupStream(t, ctx, platform, "v2_release_time", []v2Record{
			{v2Stamp("2025-10-15T12:30:00Z"), "100.0"},
			{v2Stamp("2025-11-13T13:30:00Z"), "100.5"},
			{v2Stamp("2025-12-11T13:30:00Z"), "101.0"},
			{v2Stamp("2026-10-14T12:30:00Z"), "103.0"},
			{v2Stamp("2026-11-12T13:30:00Z"), "104.0"},
			{v2Stamp("2026-12-10T13:30:00Z"), "104.2"},
		})

		// The November 2026 release, scheduled 2026-11-12T13:30Z: T is a week before it and the
		// tolerance is 20 days.
		release := v2Stamp("2026-11-12T13:30:00Z")
		call := v2Call{
			action:    v2IndexChange,
			locator:   stream,
			at:        release - 7*v2Day,
			window:    90 * v2Day,
			tolerance: 20 * v2Day,
			interval:  v2YearInterval,
		}
		settleAt := release + v2Day

		// The prior target is 2025-11-12T13:30Z: 2025-11-13 is a day away, 2025-10-15 and
		// 2025-12-11 are 28 and 29 days away, outside the tolerance. 3.5 / 100.5 = 3.4825...%.
		v2RequireAnswer(t, ctx, platform, call.withBounds("3", "3.5"), settleAt, v2Yes, release,
			"the release a year back, a day off the target, is the prior")
		v2RequireAnswer(t, ctx, platform, call.withMin("3.5"), settleAt, v2No, release, "below 3.5%")
		v2RequireExactChange(t, ctx, platform, call, settleAt, "104.0", "100.5")

		return nil
	}
}

// =============================================================================
// Waiting and the deadline
// =============================================================================

func testOutcomeLatePrintBeforeDeadline(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		at := v2Date(2026, 6, 1)
		// An old record only: none of the actions falls back to it.
		stream := v2SetupStream(t, ctx, platform, "v2_late_print", []v2Record{{at - 10*v2Day, "50"}})

		above := v2Call{action: v2Above, locator: stream, at: at, window: 3 * v2Day, tolerance: 3600}.withThreshold("100")
		inRange := v2Call{action: v2InRange, locator: stream, at: at, window: 3 * v2Day, tolerance: 3600}.withBounds("110", "130")

		v2RequireAwaiting(t, ctx, platform, above, at+600, "no readable record", "the print has not arrived")
		v2RequireAwaiting(t, ctx, platform, inRange, at+600, "no readable record", "the print has not arrived")

		// The print is stamped half an hour before T and written after it.
		v2Insert(t, ctx, platform, stream, v2Record{at - 1800, "120"}, 2)

		v2RequireAnswer(t, ctx, platform, above, at+2*v2Day, v2Yes, at-1800, "120 is above 100")
		v2RequireAnswer(t, ctx, platform, inRange, at+2*v2Day, v2Yes, at-1800, "120 is in [110, 130)")

		return nil
	}
}

func testOutcomeInvalidAtDeadline(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		at := v2Date(2026, 6, 1)
		// Records a month either side, outside every window below.
		stream := v2SetupStream(t, ctx, platform, "v2_deadline", []v2Record{
			{at - 30*v2Day, "100"},
			{at + 30*v2Day, "100"},
		})

		window := 2 * v2Day
		deadline := at + window
		for _, call := range v2AllActions(stream, at, window, 3600) {
			v2RequireAwaiting(t, ctx, platform, call, at, "awaiting print", call.action)
			v2RequireAwaiting(t, ctx, platform, call, deadline-1, "awaiting print", call.action)
			v2RequireAnswer(t, ctx, platform, call, deadline, v2Invalid, deadline, call.action)
			v2RequireAnswer(t, ctx, platform, call, deadline+v2Day, v2Invalid, deadline, call.action)
		}

		return nil
	}
}

// A capture before block time T waits even when every window already holds a record: a market
// may set its settle time before T, and a record stamped at or before T can still arrive. From
// T the binary actions read the value as of T, not the record that was there before.
func testOutcomeWaitsForTimestamp(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		at := v2Date(2026, 6, 1)
		// A record before T for the binary windows; one stamped ahead of T for the forward window
		// of index_change_in_range_v2, with its prior ten days earlier.
		stream := v2SetupStream(t, ctx, platform, "v2_before_t", []v2Record{
			{at + 60 - 10*v2Day, "100"},
			{at - 600, "100"},
			{at + 60, "100"},
		})

		window := 2 * v2Day
		for _, call := range v2AllActions(stream, at, window, 3600) {
			v2RequireAwaiting(t, ctx, platform, call, at-300, "is before the timestamp", call.action)
		}

		// The value as of T lands after the early capture.
		v2Insert(t, ctx, platform, stream, v2Record{at - 60, "50"}, 2)

		above := v2Call{action: v2Above, locator: stream, at: at, window: window, tolerance: 3600}.withThreshold("80")
		v2RequireAwaiting(t, ctx, platform, above, at-1, "is before the timestamp", "one second before T")
		v2RequireAnswer(t, ctx, platform, above, at, v2No, at-60, "50, the value as of T, is not above 80")

		for _, call := range v2AllActions(stream, at, window, 3600) {
			want := at - 60
			if call.action == v2IndexChange {
				want = at + 60
			}
			v2RequireAnswer(t, ctx, platform, call, at, v2Yes, want, call.action+" answers from T")
		}

		return nil
	}
}

// A skipped month with the next month present: the next print is outside the window, so the
// market waits and then ends INVALID instead of settling on the wrong month.
func testOutcomeSkippedCurrentMonth(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		stream := v2SetupStream(t, ctx, platform, "v2_skipped_month", []v2Record{
			{v2Date(2025, 2, 1), "100"},
			{v2Date(2025, 3, 1), "100"},
			{v2Date(2025, 4, 1), "100"},
			{v2Date(2026, 2, 1), "103"},
			// no 2026-03-01
			{v2Date(2026, 4, 1), "105"},
		})

		at := v2Date(2026, 3, 1)
		deadline := at + v2MonthStartWindow
		for _, call := range v2AllActions(stream, at, v2MonthStartWindow, v2MonthStartTolerance) {
			if call.action == v2IndexChange {
				call.interval = v2YearInterval
			}
			v2RequireAwaiting(t, ctx, platform, call, at+20*v2Day, "no readable record", call.action)
			v2RequireAnswer(t, ctx, platform, call, deadline, v2Invalid, deadline, call.action)
		}

		// April is there and compares with April 2025: +5%.
		april := v2Call{
			action:    v2IndexChange,
			locator:   stream,
			at:        v2Date(2026, 4, 1),
			window:    v2MonthStartWindow,
			tolerance: v2MonthStartTolerance,
			interval:  v2YearInterval,
		}
		v2RequireAnswer(t, ctx, platform, april.withBounds("5", "6"), v2Date(2026, 4, 20), v2Yes, v2Date(2026, 4, 1),
			"the month after the gap settles on its own print")

		return nil
	}
}

// A missing prior waits as a missing current record does: a history backfill can still be
// writing it.
func testOutcomeMissingPriorWaitsThenInvalid(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		at := v2Date(2026, 6, 1)
		interval := 10 * v2Day
		stream := v2SetupStream(t, ctx, platform, "v2_missing_prior", []v2Record{{at + 60, "102"}})

		call := v2Call{
			action:    v2IndexChange,
			locator:   stream,
			at:        at,
			window:    5 * v2Day,
			tolerance: 3600,
			interval:  interval,
		}.withBounds("1", "3")
		deadline := at + 5*v2Day

		v2RequireAwaiting(t, ctx, platform, call, at+3600, "no prior record", "the prior is missing")
		v2RequireAwaiting(t, ctx, platform, call, deadline-1, "no prior record", "the prior is missing")
		v2RequireAnswer(t, ctx, platform, call, deadline, v2Invalid, at+60,
			"no prior by the deadline: INVALID, stamped with the current record")

		// The backfill lands before the deadline: 102 against 100 is +2%.
		v2Insert(t, ctx, platform, stream, v2Record{at + 60 - interval, "100"}, 2)
		v2RequireAnswer(t, ctx, platform, call, at+v2Day, v2Yes, at+60, "the backfilled prior is read")

		return nil
	}
}

// A zero prior makes the change undefined, and waiting does not change that: INVALID at once.
func testOutcomeZeroPrior(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		at := v2Date(2026, 6, 1)
		interval := 10 * v2Day

		locator := types.StreamLocator{StreamId: util.GenerateStreamId("v2_zero_prior"), DataProvider: defaultDeployer}
		require.NoError(t, createStreamWithAllowZeros(ctx, platform, locator, true))
		v2Insert(t, ctx, platform, locator, v2Record{at + 60 - interval, "0"}, 1)
		v2Insert(t, ctx, platform, locator, v2Record{at + 60, "5"}, 1)

		call := v2Call{action: v2IndexChange, locator: locator, at: at, window: 5 * v2Day, tolerance: 3600, interval: interval}
		for _, bounded := range []v2Call{call.withMin("0"), call.withMax("0"), call.withBounds("-1000", "1000")} {
			v2RequireAnswer(t, ctx, platform, bounded, at+600, v2Invalid, at+60, "a zero prior is INVALID before the deadline")
		}

		return nil
	}
}

// The prior is the record nearest current.event_time - interval, within the tolerance; the
// earlier one wins a tie.
func testOutcomeNearestPrior(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		at := v2Date(2026, 6, 1)
		interval := 10 * v2Day
		current := at + 100
		target := current - interval

		priorCall := func(name string, priors []v2Record) v2Call {
			stream := v2SetupStream(t, ctx, platform, name, append(priors, v2Record{current, "100"}))
			return v2Call{
				action:    v2IndexChange,
				locator:   stream,
				at:        at,
				window:    5 * v2Day,
				tolerance: 3600,
				interval:  interval,
			}
		}
		settleAt := at + v2Day

		// 50 gives +100%, 200 gives -50%, 80 gives +25%.
		tie := priorCall("v2_prior_tie", []v2Record{{target - 600, "50"}, {target + 600, "200"}})
		v2RequireAnswer(t, ctx, platform, tie.withBounds("100", "101"), settleAt, v2Yes, current,
			"equal distances: the earlier record wins")

		nearer := priorCall("v2_prior_nearer", []v2Record{{target - 600, "50"}, {target + 300, "200"}})
		v2RequireAnswer(t, ctx, platform, nearer.withBounds("-50", "-49"), settleAt, v2Yes, current,
			"the nearer record wins even when it is later")

		exact := priorCall("v2_prior_exact", []v2Record{{target - 10, "50"}, {target, "80"}, {target + 10, "200"}})
		v2RequireAnswer(t, ctx, platform, exact.withBounds("25", "26"), settleAt, v2Yes, current,
			"a record on the target wins")

		edge := priorCall("v2_prior_edge", []v2Record{{target + 3600, "80"}})
		v2RequireAnswer(t, ctx, platform, edge.withBounds("25", "26"), settleAt, v2Yes, current,
			"a record exactly one tolerance away is read")

		outside := priorCall("v2_prior_outside", []v2Record{{target - 3601, "50"}, {target + 3601, "200"}})
		v2RequireAwaiting(t, ctx, platform, outside.withMin("0"), settleAt, "no prior record",
			"records just past the tolerance are not read")

		return nil
	}
}

// =============================================================================
// Streams that cannot be read count as "no record"
// =============================================================================

func testOutcomeDeletedStream(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		at := v2Date(2026, 6, 1)
		interval := 10 * v2Day
		stream := v2SetupStream(t, ctx, platform, "v2_deleted", []v2Record{
			{at + 60 - interval, "100"},
			{at - 60, "120"},
			{at + 60, "102"},
		})

		window := 2 * v2Day
		deadline := at + window
		calls := []v2Call{
			v2Call{action: v2IndexChange, locator: stream, at: at, window: window, tolerance: 3600, interval: interval}.withBounds("1", "3"),
			v2Call{action: v2Above, locator: stream, at: at, window: window, tolerance: 3600}.withThreshold("100"),
			v2Call{action: v2Below, locator: stream, at: at, window: window, tolerance: 3600}.withThreshold("200"),
			v2Call{action: v2InRange, locator: stream, at: at, window: window, tolerance: 3600}.withBounds("100", "200"),
		}

		for _, call := range calls {
			_, err := v2Capture(t, ctx, platform, call, at+600)
			require.NoError(t, err, "%s resolves while the stream exists", call.action)
		}

		res, err := setup.DeleteStream(ctx, platform, stream)
		require.NoError(t, err)
		require.NoError(t, res.Error, "delete_stream")

		for _, call := range calls {
			v2RequireAwaiting(t, ctx, platform, call, at+600, "no readable record", call.action+" on a deleted stream")
			v2RequireAnswer(t, ctx, platform, call, deadline, v2Invalid, deadline, call.action+" on a deleted stream")
		}

		return nil
	}
}

func testOutcomeComposedStream(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		composedID := util.GenerateStreamId("v2_composed")
		require.NoError(t, setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedID,
			Height:   1,
			MarkdownData: `
			| event_time | v2_composed_child_a | v2_composed_child_b |
			|------------|---------------------|---------------------|
			| 1          | 100                 | 50                  |
			| 2          | 102                 | 60                  |
			| 3          | 103                 | 55                  |
			| 13         | 104                 | 70                  |
			`,
			Weights: []string{"1", "3"},
		}))

		composed := types.StreamLocator{StreamId: composedID, DataProvider: defaultDeployer}
		child := types.StreamLocator{StreamId: util.GenerateStreamId("v2_composed_child_a"), DataProvider: defaultDeployer}

		// T = 3, tolerance 1, W 100, interval 10: every window holds a composed event.
		at, window, tolerance := int64(3), int64(100), int64(1)
		deadline := at + window
		for _, call := range v2AllActions(composed, at, window, tolerance) {
			if call.action == v2IndexChange {
				call.interval = 10
			}
			v2RequireAwaiting(t, ctx, platform, call, 50, "no readable record", call.action+" on a composed stream")
			v2RequireAnswer(t, ctx, platform, call, deadline, v2Invalid, deadline, call.action+" on a composed stream")
		}

		// The primitive child resolves on the same arguments.
		childCall := v2Call{action: v2InRange, locator: child, at: at, window: window, tolerance: tolerance}.withBounds("100", "200")
		v2RequireAnswer(t, ctx, platform, childCall, 50, v2Yes, 3, "the primitive child is read")

		return nil
	}
}

// Read permissions are not checked: a wallet that may not read a private stream gets the same
// answer as the owner, so the outcome never depends on who requests the capture.
func testOutcomePrivateStream(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		at := v2Date(2026, 6, 1)
		stream := v2SetupStream(t, ctx, platform, "v2_private", []v2Record{{at - 60, "120"}})
		require.NoError(t, procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  stream,
			Key:      "read_visibility",
			Value:    "1",
			ValType:  "int",
			Height:   2,
		}))

		window := 2 * v2Day
		call := v2Call{action: v2Above, locator: stream, at: at, window: window, tolerance: 3600}.withThreshold("100")
		v2RequireAnswer(t, ctx, platform, call, at+600, v2Yes, at-60, "the owner reads the private stream")

		stranger := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000456")
		call.caller = &stranger
		v2RequireAnswer(t, ctx, platform, call, at+600, v2Yes, at-60, "a stranger gets the same answer")
		v2RequireAnswer(t, ctx, platform, call, at+window, v2Yes, at-60, "and still does after the deadline")

		return nil
	}
}

// =============================================================================
// Selection and comparison rules
// =============================================================================

// The binary actions read "the value as of T": the latest record in [T - tolerance, T], with the
// latest created_at winning at an event time. No older record stands in.
func testOutcomeBinaryBoundaries(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		at := v2Date(2026, 6, 1)
		window := 2 * v2Day
		stream := v2SetupStream(t, ctx, platform, "v2_binary_bounds", []v2Record{
			{at - 7200, "50"}, // outside the tolerance
			{at - 1800, "90"}, // in the window, not the latest
			{at - 600, "100"}, // the value as of T
			{at + 600, "999"}, // after T: never read
		})
		settleAt := at + 3600

		above := v2Call{action: v2Above, locator: stream, at: at, window: window, tolerance: 3600}
		below := v2Call{action: v2Below, locator: stream, at: at, window: window, tolerance: 3600}
		inRange := v2Call{action: v2InRange, locator: stream, at: at, window: window, tolerance: 3600}

		cases := []struct {
			call v2Call
			want int
			why  string
		}{
			{above.withThreshold("100"), v2No, "above is strict"},
			{above.withThreshold("99.999999999999999999"), v2Yes, "one unit below the value"},
			{below.withThreshold("100"), v2No, "below is strict"},
			{below.withThreshold("100.000000000000000001"), v2Yes, "one unit above the value"},
			{inRange.withBounds("100", "101"), v2Yes, "min is inclusive"},
			{inRange.withBounds("99", "100"), v2No, "max is exclusive"},
			{inRange.withMax("100"), v2No, "open lower tail, max exclusive"},
			{inRange.withMax("100.000000000000000001"), v2Yes, "open lower tail"},
			{inRange.withMin("100"), v2Yes, "open upper tail, min inclusive"},
			{inRange.withMin("100.000000000000000001"), v2No, "open upper tail"},
		}
		for _, c := range cases {
			v2RequireAnswer(t, ctx, platform, c.call, settleAt, c.want, at-600, c.why)
		}

		// A correction at the same event time, written later, wins.
		v2Insert(t, ctx, platform, stream, v2Record{at - 600, "80"}, 5)
		v2RequireAnswer(t, ctx, platform, inRange.withMax("100"), settleAt, v2Yes, at-600, "the corrected value 80 is read")
		v2RequireAnswer(t, ctx, platform, above.withThreshold("85"), settleAt, v2No, at-600, "the corrected value 80 is read")

		// No stale fallback: a record just past the tolerance is not read, one on its edge is.
		stale := v2SetupStream(t, ctx, platform, "v2_binary_stale", []v2Record{{at - 3601, "120"}})
		staleCall := v2Call{action: v2Above, locator: stale, at: at, window: window, tolerance: 3600}.withThreshold("100")
		v2RequireAwaiting(t, ctx, platform, staleCall, settleAt, "no readable record", "3601 seconds before T is stale")
		v2RequireAnswer(t, ctx, platform, staleCall, at+window, v2Invalid, at+window, "a stale record ends INVALID")

		v2Insert(t, ctx, platform, stale, v2Record{at - 3600, "120"}, 2)
		v2RequireAnswer(t, ctx, platform, staleCall, settleAt, v2Yes, at-3600, "a record one tolerance before T is read")

		// Tolerance 0 reads a record stamped at T only.
		exact := v2Call{action: v2Above, locator: stale, at: at - 3600, window: window, tolerance: 0}.withThreshold("100")
		v2RequireAnswer(t, ctx, platform, exact, settleAt, v2Yes, at-3600, "tolerance 0 reads the record at T")

		return nil
	}
}

// The change is (current - prior) * 100 / prior, YES when min <= change < max, compared without
// dividing so a negative prior flips each comparison and no value overflows.
func testOutcomeIndexChangeBoundaries(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		at := v2Date(2026, 6, 1)
		interval := 10 * v2Day
		settleAt := at + v2Day

		call := func(name string, records []v2Record) v2Call {
			stream := v2SetupStream(t, ctx, platform, name, records)
			return v2Call{action: v2IndexChange, locator: stream, at: at, window: 5 * v2Day, tolerance: 3600, interval: interval}
		}

		// Exactly +2%. The current record is the first at or after T: the later record in the
		// window and the one before T are not read.
		two := call("v2_change_two", []v2Record{
			{at - 10, "500"},
			{at + 10 - interval, "100"},
			{at + 10, "102"},
			{at + 20, "150"},
		})
		for _, c := range []struct {
			call v2Call
			want int
		}{
			{two.withBounds("2", "3"), v2Yes},
			{two.withBounds("1", "2"), v2No},
			{two.withMax("2"), v2No},
			{two.withMin("2"), v2Yes},
			{two.withMin("2.000000000000000001"), v2No},
		} {
			v2RequireAnswer(t, ctx, platform, c.call, settleAt, c.want, at+10, "exactly +2%")
		}

		// A negative prior: -100 to -98 is (2 * 100) / -100 = -2%.
		negative := call("v2_change_negative", []v2Record{
			{at + 10 - interval, "-100"},
			{at + 10, "-98"},
		})
		for _, c := range []struct {
			call v2Call
			want int
		}{
			{negative.withBounds("-2", "-1"), v2Yes},
			{negative.withBounds("-3", "-2"), v2No},
			{negative.withMax("-2"), v2No},
			{negative.withMin("-2"), v2Yes},
			{negative.withMin("0"), v2No},
		} {
			v2RequireAnswer(t, ctx, platform, c.call, settleAt, c.want, at+10, "a negative prior")
		}

		// The extremes of NUMERIC(36,18): (current - prior) * 100 alone overflows NUMERIC(36,18).
		huge := call("v2_change_huge", []v2Record{
			{at + 10 - interval, "0.000000000000000001"},
			{at + 10, "999999999999999999.999999999999999999"},
		})
		v2RequireAnswer(t, ctx, platform, huge.withMin("999999999999999999.999999999999999999"), settleAt, v2Yes, at+10,
			"a change of about 1e38 percent is above every bound")
		v2RequireAnswer(t, ctx, platform, huge.withMax("0"), settleAt, v2No, at+10, "and below none")

		fall := call("v2_change_fall", []v2Record{
			{at + 10 - interval, "999999999999999999.999999999999999999"},
			{at + 10, "-999999999999999999.999999999999999999"},
		})
		v2RequireAnswer(t, ctx, platform, fall.withBounds("-200", "-199.999999999999999999"), settleAt, v2Yes, at+10,
			"from the top of the range to the bottom is exactly -200%")

		return nil
	}
}

// The SQL refuses what the Go check in validate_attestation_date_range refuses, so a direct call
// fails as a capture would.
func testOutcomeArgumentErrors(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		at := v2Date(2026, 6, 1)
		stream := v2SetupStream(t, ctx, platform, "v2_arguments", []v2Record{{at - 60, "120"}})
		blockAt := at + 600

		index := v2Call{action: v2IndexChange, locator: stream, at: at, window: v2Day, tolerance: 3600, interval: 10 * v2Day}.withMin("0")
		above := v2Call{action: v2Above, locator: stream, at: at, window: v2Day, tolerance: 3600}.withThreshold("100")
		inRange := v2Call{action: v2InRange, locator: stream, at: at, window: v2Day, tolerance: 3600}.withBounds("1", "2")

		modify := func(c v2Call, f func(*v2Call)) v2Call {
			f(&c)
			return c
		}
		cases := []struct {
			call v2Call
			want string
		}{
			{modify(above, func(c *v2Call) { c.at = -1 }), "timestamp must not be negative"},
			{modify(above, func(c *v2Call) { c.at = blockAt + 315360001 }), "is more than 315360000 seconds after the block time"},
			{modify(above, func(c *v2Call) { c.window = 0 }), "window must be in (0, 315360000]"},
			{modify(above, func(c *v2Call) { c.window = 315360001 }), "window must be in (0, 315360000]"},
			{modify(above, func(c *v2Call) { c.tolerance = -1 }), "tolerance must be in [0, 3888000]"},
			{modify(above, func(c *v2Call) { c.tolerance = 3888001; c.window = 4000000 }), "tolerance must be in [0, 3888000]"},
			{modify(above, func(c *v2Call) { c.tolerance = 7200; c.window = 3600 }), "must not be less than tolerance"},
			{above.withThreshold(""), "threshold is required"},
			{modify(index, func(c *v2Call) { c.interval = 0 }), "time_interval must be in (0, 315360000]"},
			{modify(index, func(c *v2Call) { c.interval = 315360001 }), "time_interval must be in (0, 315360000]"},
			{modify(index, func(c *v2Call) { c.interval = 7200 }), "must be more than twice tolerance"},
			{index.withBounds("", ""), "at least one of min_change or max_change is required"},
			{index.withBounds("2", "2"), "min_change must be less than max_change"},
			{inRange.withBounds("", ""), "at least one of min_value or max_value is required"},
			{inRange.withBounds("3", "2"), "min_value must be less than max_value"},
		}
		for _, c := range cases {
			_, err := v2Capture(t, ctx, platform, c.call, blockAt)
			require.Error(t, err, "%s should refuse: %s", c.call.action, c.want)
			require.Contains(t, err.Error(), c.want)
		}

		// The largest accepted values still resolve.
		edge := modify(above, func(c *v2Call) { c.window = 315360000; c.tolerance = 3888000 })
		v2RequireAnswer(t, ctx, platform, edge, blockAt, v2Yes, at-60, "the largest window and tolerance are accepted")

		return nil
	}
}

// =============================================================================
// Helpers
// =============================================================================

type v2Record struct {
	at    int64
	value string
}

// v2Call is one call of an outcome action. Bounds and thresholds are strings; nil, or an empty
// string, is a NULL argument.
type v2Call struct {
	action    string
	locator   types.StreamLocator
	at        int64
	window    int64
	tolerance int64
	interval  int64 // index_change_in_range_v2 only
	min, max  *string
	threshold *string
	// caller defaults to the stream's data provider.
	caller *util.EthereumAddress
}

func (c v2Call) withBounds(min, max string) v2Call {
	c.min, c.max = &min, &max
	return c
}

func (c v2Call) withMin(min string) v2Call {
	c.min, c.max = &min, nil
	return c
}

func (c v2Call) withMax(max string) v2Call {
	c.min, c.max = nil, &max
	return c
}

func (c v2Call) withThreshold(threshold string) v2Call {
	c.threshold = &threshold
	return c
}

func (c v2Call) args(t *testing.T) []any {
	t.Helper()

	// Left as an untyped nil when absent: a typed (*Decimal)(nil) does not read back as NULL.
	dec := func(v *string) any {
		if v == nil || *v == "" {
			return nil
		}
		d, err := kwilTypes.ParseDecimalExplicit(*v, 36, 18)
		require.NoError(t, err)
		return d
	}

	base := []any{c.locator.DataProvider.Address(), c.locator.StreamId.String(), c.at, c.window, c.tolerance}
	switch c.action {
	case v2IndexChange:
		return append(base, c.interval, dec(c.min), dec(c.max))
	case v2Above, v2Below:
		return append(base, dec(c.threshold))
	case v2InRange:
		return append(base, dec(c.min), dec(c.max))
	}
	t.Fatalf("unknown outcome action %q", c.action)
	return nil
}

// v2AllActions returns one call of each outcome action on a stream, with bounds wide enough that
// any record would resolve YES.
func v2AllActions(locator types.StreamLocator, at, window, tolerance int64) []v2Call {
	base := v2Call{locator: locator, at: at, window: window, tolerance: tolerance}

	index := base
	index.action, index.interval = v2IndexChange, 10*v2Day
	above := base
	above.action = v2Above
	below := base
	below.action = v2Below
	inRange := base
	inRange.action = v2InRange

	return []v2Call{
		index.withMin("-1000"),
		above.withThreshold("-1000"),
		below.withThreshold("1000000"),
		inRange.withBounds("-1000", "1000000"),
	}
}

type v2Answer struct {
	eventTime int64
	code      int
}

// v2Capture calls the action at a block time and returns its one row, or the error it raised.
// Action errors arrive in res.Error rather than in err, so both are checked.
func v2Capture(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, c v2Call, blockTime int64) (v2Answer, error) {
	t.Helper()

	caller := c.locator.DataProvider
	if c.caller != nil {
		caller = *c.caller
	}
	engineCtx := testctx.NewEngineContext(ctx, platform, caller, 1)
	engineCtx.TxContext.BlockContext.Timestamp = blockTime

	var answers []v2Answer
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", c.action, c.args(t), func(row *common.Row) error {
		require.Len(t, row.Values, 2, "an outcome action returns (event_time, value)")
		value, ok := new(big.Rat).SetString(row.Values[1].(*kwilTypes.Decimal).String())
		require.True(t, ok, "parse value %v", row.Values[1])

		answer := v2Answer{eventTime: row.Values[0].(int64)}
		switch {
		case value.Cmp(big.NewRat(1, 1)) == 0:
			answer.code = v2Yes
		case value.Sign() == 0:
			answer.code = v2No
		case value.Cmp(big.NewRat(-1, 1)) == 0:
			answer.code = v2Invalid
		default:
			t.Fatalf("%s returned %s, not 1, 0 or -1", c.action, value.FloatString(18))
		}
		answers = append(answers, answer)
		return nil
	})
	if err != nil {
		return v2Answer{}, err
	}
	if res.Error != nil {
		return v2Answer{}, res.Error
	}
	require.Len(t, answers, 1, "%s must return exactly one row", c.action)
	return answers[0], nil
}

func (c v2Call) describe() string {
	bound := func(v *string) string {
		if v == nil || *v == "" {
			return "NULL"
		}
		return *v
	}
	switch c.action {
	case v2Above, v2Below:
		return fmt.Sprintf("%s(threshold %s)", c.action, bound(c.threshold))
	default:
		return fmt.Sprintf("%s[%s, %s)", c.action, bound(c.min), bound(c.max))
	}
}

func v2RequireAnswer(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, c v2Call, blockTime int64,
	wantCode int, wantEventTime int64, msg string) {
	t.Helper()

	answer, err := v2Capture(t, ctx, platform, c, blockTime)
	require.NoError(t, err, "%s at block time %d: %s", c.describe(), blockTime, msg)
	require.Equal(t, wantCode, answer.code, "%s at block time %d: %s", c.describe(), blockTime, msg)
	require.Equal(t, wantEventTime, answer.eventTime, "%s at block time %d: event_time: %s", c.describe(), blockTime, msg)
}

func v2RequireAwaiting(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, c v2Call, blockTime int64,
	wantDetail string, msg string) {
	t.Helper()

	_, err := v2Capture(t, ctx, platform, c, blockTime)
	require.Error(t, err, "%s at block time %d should wait: %s", c.describe(), blockTime, msg)
	require.Contains(t, err.Error(), "awaiting print", "%s: %s", c.describe(), msg)
	require.Contains(t, err.Error(), wantDetail, "%s: %s", c.describe(), msg)
}

// v2RequireExactChange proves the action compared exactly (current - prior) * 100 / prior: the
// change rounded down to 18 decimals is at or above it, and one unit more is not.
func v2RequireExactChange(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, c v2Call, blockTime int64,
	current, prior string) {
	t.Helper()

	cur, ok := new(big.Rat).SetString(current)
	require.True(t, ok)
	pri, ok := new(big.Rat).SetString(prior)
	require.True(t, ok)
	change := new(big.Rat).Quo(new(big.Rat).Mul(new(big.Rat).Sub(cur, pri), big.NewRat(100, 1)), pri)

	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	floor := new(big.Int).Div(new(big.Int).Mul(change.Num(), scale), change.Denom())
	next := new(big.Int).Add(floor, big.NewInt(1))

	answer, err := v2Capture(t, ctx, platform, c.withMin(v2FixedPoint(floor)), blockTime)
	require.NoError(t, err)
	require.Equal(t, v2Yes, answer.code, "change should be at or above %s", v2FixedPoint(floor))

	answer, err = v2Capture(t, ctx, platform, c.withMin(v2FixedPoint(next)), blockTime)
	require.NoError(t, err)
	require.Equal(t, v2No, answer.code, "change should be below %s", v2FixedPoint(next))
}

// v2FixedPoint formats an integer count of 1e-18 units as an 18-decimal string.
func v2FixedPoint(units *big.Int) string {
	sign := ""
	abs := new(big.Int).Set(units)
	if abs.Sign() < 0 {
		sign = "-"
		abs.Neg(abs)
	}
	digits := abs.String()
	if len(digits) <= 18 {
		digits = strings.Repeat("0", 19-len(digits)) + digits
	}
	return sign + digits[:len(digits)-18] + "." + digits[len(digits)-18:]
}

func v2SetupStream(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, name string, records []v2Record) types.StreamLocator {
	t.Helper()

	locator := types.StreamLocator{StreamId: util.GenerateStreamId(name), DataProvider: defaultDeployer}
	require.NoError(t, setup.CreateStream(ctx, platform, setup.StreamInfo{
		Type:    setup.ContractTypePrimitive,
		Locator: locator,
	}))
	for _, record := range records {
		v2Insert(t, ctx, platform, locator, record, 1)
	}
	return locator
}

// v2Insert writes one record with its exact decimal value, created at the given height.
func v2Insert(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, locator types.StreamLocator, record v2Record, height int64) {
	t.Helper()

	require.NoError(t, feefund.EnsureWalletFunded(ctx, platform, locator.DataProvider.Address(), feefund.WriteFeeWei))

	value, err := kwilTypes.ParseDecimalExplicit(record.value, 36, 18)
	require.NoError(t, err)

	engineCtx := testctx.NewEngineContext(ctx, platform, locator.DataProvider, height)
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "insert_record",
		[]any{locator.DataProvider.Address(), locator.StreamId.String(), record.at, value}, nil)
	require.NoError(t, err)
	require.NoError(t, errors.Wrapf(res.Error, "insert_record %d %s", record.at, record.value))
}

func v2Date(year int, month time.Month, day int) int64 {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Unix()
}

func v2Stamp(value string) int64 {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return parsed.Unix()
}
