package tests

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	extauth "github.com/trufnetwork/kwil-db/extensions/auth"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// The pruner deletes records that restate the value already standing at their
// event time. Its whole claim is that no read changes, so most of what follows
// reads the stream before and after and asserts the two agree — not that a
// particular row survived.

const (
	pruneStreamName      = "prune_test_stream"
	pruneSecondName      = "prune_test_stream_b"
	pruneEmptyStreamData = `
		| event_time | value |
		|------------|-------|
		| 1          | 1     |
		`
)

var (
	pruneStreamId = util.GenerateStreamId(pruneStreamName)
	pruneSecondId = util.GenerateStreamId(pruneSecondName)
)

func TestPruneActions(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "prune_actions_test",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			WithPruneStream(testPruneCollapsesRunsAndLeavesReadsAlone(t)),
			WithPruneStream(testPruneKeepsTheFirstAndNewestRecords(t)),
			WithPruneStream(testPruneKeepsTheTruflationWatermark(t)),
			WithPruneStream(testPruneLeavesRecentRecordsAlone(t)),
			WithPruneStream(testPruneResumesAfterTheCap(t)),
			WithPruneStream(testPruneKeepsOneRecordPerRetentionWindow(t)),
			WithPruneStream(testPruneKeepsStreamsApart(t)),
			WithPruneStream(testPruneTakesEveryRevisionAtAnEventTime(t)),
			WithPruneStream(testPruneTakesTheMarkerWithTheRecord(t)),
			WithPruneStream(testPruneLeavesADigestedDayReadable(t)),
			WithPruneStream(testAutoPruneSweepsAndMovesTheCursor(t)),
			WithSignerAndProvider(testPruneConfigShipsDisabled(t)),
			WithSignerAndProvider(testPruneRejectsBadArguments(t)),
			WithSignerAndProvider(testPruneIsLeaderOnly(t)),
		},
	}, testutils.GetTestOptionsWithCache())
}

// =============================================================================
// The claim the pruner rests on
// =============================================================================

// The spec's worked example. Eight daily records holding 10,10,10,12,12,10,10,10
// collapse to four, and a read at every one of the eight days answers exactly
// what it answered before.
func testPruneCollapsesRunsAndLeavesReadsAlone(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, pruneStreamName)
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}
		if err := seedPruneRecords(ctx, platform, streamRef, []pruneRecord{
			{Day: 1, Value: "10"},
			{Day: 2, Value: "10"},
			{Day: 3, Value: "10"},
			{Day: 4, Value: "12"},
			{Day: 5, Value: "12"},
			{Day: 6, Value: "10"},
			{Day: 7, Value: "10"},
			{Day: 8, Value: "10"},
		}); err != nil {
			return err
		}

		days := []int64{1, 2, 3, 4, 5, 6, 7, 8}
		before, err := readEachDay(ctx, platform, days)
		if err != nil {
			return errors.Wrap(err, "read before pruning")
		}

		res, err := callBatchPrune(ctx, platform, []int{streamRef}, retentionReaching(9), 1000)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, res, `
			| deleted_event_times | deleted_rows | has_more_to_delete |
			|---------------------|--------------|--------------------|
			| 4                   | 4            | false              |
		`)

		surviving, err := readStoredRecords(ctx, platform, streamRef)
		if err != nil {
			return err
		}
		// Day 8 survives on rule 4a rather than rule 1 — it repeats day 6, but it
		// is the newest record the stream has.
		if got, want := surviving, "86400=10 345600=12 518400=10 691200=10"; got != want {
			return errors.Errorf("wrong records survived:\n got: %s\nwant: %s", got, want)
		}

		after, err := readEachDay(ctx, platform, days)
		if err != nil {
			return errors.Wrap(err, "read after pruning")
		}
		if before != after {
			return errors.Errorf("pruning moved a read:\nbefore: %s\n after: %s", before, after)
		}

		// Nothing is left to do, and running again must not find any.
		res, err = callBatchPrune(ctx, platform, []int{streamRef}, retentionReaching(9), 1000)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, res, `
			| deleted_event_times | deleted_rows | has_more_to_delete |
			|---------------------|--------------|--------------------|
			| 0                   | 0            | false              |
		`)
		return nil
	}
}

// =============================================================================
// The records that are never candidates
// =============================================================================

// Rules 3 and 4a. A stream that has published one value forever keeps exactly
// two records: the anchor every read falls back to, and the newest one that
// makes "the current value" honest.
func testPruneKeepsTheFirstAndNewestRecords(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, pruneStreamName)
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}
		if err := seedPruneRecords(ctx, platform, streamRef, []pruneRecord{
			{Day: 1, Value: "7"},
			{Day: 2, Value: "7"},
			{Day: 3, Value: "7"},
			{Day: 4, Value: "7"},
		}); err != nil {
			return err
		}

		if _, err := callBatchPrune(ctx, platform, []int{streamRef}, retentionReaching(5), 1000); err != nil {
			return err
		}

		surviving, err := readStoredRecords(ctx, platform, streamRef)
		if err != nil {
			return err
		}
		if got, want := surviving, "86400=7 345600=7"; got != want {
			return errors.Errorf("first and newest must both survive:\n got: %s\nwant: %s", got, want)
		}
		return nil
	}
}

// Rule 4b. The Truflation provider reads its fetch watermark from the greatest
// truflation_created_at a stream holds. Delete the row carrying it and the
// watermark walks backwards, the provider republishes what was just deleted, and
// the next pass deletes it again. Rule 4a does not cover this: a late backfill
// can carry the greatest timestamp at an old event time.
func testPruneKeepsTheTruflationWatermark(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, pruneStreamName)
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}
		if err := seedPruneRecords(ctx, platform, streamRef, []pruneRecord{
			{Day: 1, Value: "5", Truflation: "2026-01-01T00:00:00Z"},
			// A backfill written last, sitting at an old event time.
			{Day: 2, Value: "5", Truflation: "2026-09-01T00:00:00Z"},
			{Day: 3, Value: "5", Truflation: "2026-01-03T00:00:00Z"},
			{Day: 4, Value: "5", Truflation: "2026-01-04T00:00:00Z"},
		}); err != nil {
			return err
		}

		if _, err := callBatchPrune(ctx, platform, []int{streamRef}, retentionReaching(5), 1000); err != nil {
			return err
		}

		surviving, err := readStoredRecords(ctx, platform, streamRef)
		if err != nil {
			return err
		}
		if got, want := surviving, "86400=5 172800=5 345600=5"; got != want {
			return errors.Errorf("the watermark record must survive:\n got: %s\nwant: %s", got, want)
		}
		return nil
	}
}

// Rule 2. Nothing inside the retention window is a candidate, however redundant
// it looks, so a run straddling the boundary collapses only on the old side.
func testPruneLeavesRecentRecordsAlone(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, pruneStreamName)
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}
		if err := seedPruneRecords(ctx, platform, streamRef, []pruneRecord{
			{Day: 1, Value: "3"},
			{Day: 2, Value: "3"},
			{Day: 3, Value: "3"},
			{Day: 4, Value: "3"},
			{Day: 5, Value: "3"},
		}); err != nil {
			return err
		}

		// A cutoff reaching only day 4 leaves days 4 and 5 out of scope.
		if _, err := callBatchPrune(ctx, platform, []int{streamRef}, retentionReaching(4), 1000); err != nil {
			return err
		}

		surviving, err := readStoredRecords(ctx, platform, streamRef)
		if err != nil {
			return err
		}
		if got, want := surviving, "86400=3 345600=3 432000=3"; got != want {
			return errors.Errorf("retention window not respected:\n got: %s\nwant: %s", got, want)
		}
		return nil
	}
}

// =============================================================================
// Bounding the work
// =============================================================================

// A capped call reports that it left something behind, and repeated calls
// converge. Each pass deletes at most the cap, and the run shortens by exactly
// what was deleted rather than re-qualifying rows that already went.
func testPruneResumesAfterTheCap(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, pruneStreamName)
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}
		if err := seedPruneRecords(ctx, platform, streamRef, []pruneRecord{
			{Day: 1, Value: "10"},
			{Day: 2, Value: "10"},
			{Day: 3, Value: "10"},
			{Day: 4, Value: "10"},
			{Day: 5, Value: "12"},
		}); err != nil {
			return err
		}

		expected := []string{
			"| 1 | 1 | true |",
			"| 1 | 1 | true |",
			"| 1 | 1 | false |",
			"| 0 | 0 | false |",
		}
		for i, want := range expected {
			res, err := callBatchPrune(ctx, platform, []int{streamRef}, retentionReaching(6), 1)
			if err != nil {
				return errors.Wrapf(err, "prune pass %d", i+1)
			}
			assertMarkdownEquals(t, res, `
				| deleted_event_times | deleted_rows | has_more_to_delete |
				|---------------------|--------------|--------------------|
				`+want+`
			`)
		}

		surviving, err := readStoredRecords(ctx, platform, streamRef)
		if err != nil {
			return err
		}
		if got, want := surviving, "86400=10 432000=12"; got != want {
			return errors.Errorf("resume did not converge:\n got: %s\nwant: %s", got, want)
		}
		return nil
	}
}

// =============================================================================
// How far apart survivors may sit
// =============================================================================

// Rule 6. Collapsing a flat run all the way to its head would leave an anchor
// arbitrarily older than the point it answers for, and get_indexed_value_at
// (migration 055) rejects an anchor older than the staleness window it was given
// rather than carrying it forward. Bucketing by the retention window keeps one
// record per window, which bounds how old an anchor can be.
func testPruneKeepsOneRecordPerRetentionWindow(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, pruneStreamName)
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}
		if err := seedPruneRecords(ctx, platform, streamRef, []pruneRecord{
			{Day: 1, Value: "10"},
			{Day: 2, Value: "10"},
			{Day: 3, Value: "10"},
			{Day: 4, Value: "10"},
			{Day: 5, Value: "10"},
			{Day: 6, Value: "10"},
			{Day: 7, Value: "10"},
		}); err != nil {
			return err
		}

		// Three days of retention puts every fixture in scope and makes the bucket
		// three days wide, so days 1-2 fall in one, 3-5 in the next, 6-7 in the
		// last. The first record of each bucket survives.
		threeDays := int64(3 * daySecs)
		res, err := callBatchPrune(ctx, platform, []int{streamRef}, threeDays, 1000)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, res, `
			| deleted_event_times | deleted_rows | has_more_to_delete |
			|---------------------|--------------|--------------------|
			| 3                   | 3            | false              |
		`)

		surviving, err := readStoredRecords(ctx, platform, streamRef)
		if err != nil {
			return err
		}
		if got, want := surviving, "86400=10 259200=10 518400=10 604800=10"; got != want {
			return errors.Errorf("the run did not keep one record per window:\n got: %s\nwant: %s", got, want)
		}

		// And it settles there: the survivors are already one per bucket, so a
		// second pass has nothing left to take.
		res, err = callBatchPrune(ctx, platform, []int{streamRef}, threeDays, 1000)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, res, `
			| deleted_event_times | deleted_rows | has_more_to_delete |
			|---------------------|--------------|--------------------|
			| 0                   | 0            | false              |
		`)
		return nil
	}
}

// =============================================================================
// More than one stream at a time
// =============================================================================

// Batching is the action's whole purpose, and the one thing a single-stream
// fixture cannot check is that the streams stay apart. Stream B opens on the
// value stream A closes on, so a LAG that forgot to partition by stream would
// delete B's first record — the anchor every read of B falls back to.
func testPruneKeepsStreamsApart(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		first, err := setup.GetStreamIdForDeployer(ctx, platform, pruneStreamName)
		if err != nil {
			return errors.Wrap(err, "resolve first stream ref")
		}
		if err := SetupStreamMD(ctx, platform, pruneSecondId, 1, pruneEmptyStreamData); err != nil {
			return errors.Wrap(err, "set up second stream")
		}
		second, err := setup.GetStreamIdForDeployer(ctx, platform, pruneSecondName)
		if err != nil {
			return errors.Wrap(err, "resolve second stream ref")
		}
		if err := clearPruneRecords(ctx, platform, second); err != nil {
			return err
		}
		if first >= second {
			return errors.Errorf("expected the first stream to hold the lower ref, got %d and %d", first, second)
		}

		if err := seedPruneRecords(ctx, platform, first, []pruneRecord{
			{Day: 1, Value: "10"},
			{Day: 2, Value: "10"},
			{Day: 3, Value: "20"},
		}); err != nil {
			return err
		}
		if err := seedPruneRecords(ctx, platform, second, []pruneRecord{
			{Day: 1, Value: "20"},
			{Day: 2, Value: "20"},
			{Day: 3, Value: "30"},
		}); err != nil {
			return err
		}

		// A cap of one takes the lowest (stream_ref, event_time) and says so.
		// Which one that is has to be the same on every node, which is why the
		// ordering is total rather than left to the planner.
		res, err := callBatchPrune(ctx, platform, []int{second, first}, retentionReaching(4), 1)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, res, `
			| deleted_event_times | deleted_rows | has_more_to_delete |
			|---------------------|--------------|--------------------|
			| 1                   | 1            | true               |
		`)
		firstRows, err := readStoredRecords(ctx, platform, first)
		if err != nil {
			return err
		}
		if got, want := firstRows, "86400=10 259200=20"; got != want {
			return errors.Errorf("the cap did not start at the lowest stream:\n got: %s\nwant: %s", got, want)
		}
		secondRows, err := readStoredRecords(ctx, platform, second)
		if err != nil {
			return err
		}
		if got, want := secondRows, "86400=20 172800=20 259200=30"; got != want {
			return errors.Errorf("the cap reached past its bound:\n got: %s\nwant: %s", got, want)
		}

		res, err = callBatchPrune(ctx, platform, []int{second, first}, retentionReaching(4), 1000)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, res, `
			| deleted_event_times | deleted_rows | has_more_to_delete |
			|---------------------|--------------|--------------------|
			| 1                   | 1            | false              |
		`)

		secondRows, err = readStoredRecords(ctx, platform, second)
		if err != nil {
			return err
		}
		// Day 1 of the second stream repeats the last value of the first and is
		// still its own stream's first record, so it stays.
		if got, want := secondRows, "86400=20 259200=30"; got != want {
			return errors.Errorf("the streams were not judged apart:\n got: %s\nwant: %s", got, want)
		}
		return nil
	}
}

// =============================================================================
// What travels with a pruned record
// =============================================================================

// Rule 5, first half. Several rows can share an event time, distinguished by
// created_at, and a read resolves to the newest of them. Leaving a shadowed one
// behind would resurrect an older value, so they all go together — which is also
// why the cap counts event times rather than rows.
func testPruneTakesEveryRevisionAtAnEventTime(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, pruneStreamName)
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}
		if err := seedPruneRecords(ctx, platform, streamRef, []pruneRecord{
			{Day: 1, Value: "10", CreatedAt: 100},
			// Day 2 was published as 99 and then revised to 10, so the value a read
			// resolves to there repeats day 1 even though a row at that event time
			// does not.
			{Day: 2, Value: "99", CreatedAt: 100},
			{Day: 2, Value: "10", CreatedAt: 200},
			{Day: 3, Value: "12", CreatedAt: 100},
		}); err != nil {
			return err
		}

		res, err := callBatchPrune(ctx, platform, []int{streamRef}, retentionReaching(4), 1000)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, res, `
			| deleted_event_times | deleted_rows | has_more_to_delete |
			|---------------------|--------------|--------------------|
			| 1                   | 2            | false              |
		`)

		surviving, err := readStoredRecords(ctx, platform, streamRef)
		if err != nil {
			return err
		}
		if got, want := surviving, "86400=10 259200=12"; got != want {
			return errors.Errorf("a shadowed revision was left behind:\n got: %s\nwant: %s", got, want)
		}
		return nil
	}
}

// Rule 5, second half. primitive_event_type is keyed on (stream_ref, event_time)
// with no foreign key to the record itself, so a marker left behind after its
// record goes points at nothing and get_daily_ohlc reads it as a digested day.
func testPruneTakesTheMarkerWithTheRecord(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, pruneStreamName)
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}
		if err := seedPruneRecords(ctx, platform, streamRef, []pruneRecord{
			{Day: 1, Value: "10"},
			{Day: 2, Value: "10"},
			{Day: 3, Value: "12"},
		}); err != nil {
			return err
		}
		// Every one-record day digest has touched carries type 15 — open, high, low
		// and close on the same record. That is 79% of the markers on mainnet.
		for _, day := range []int64{1, 2, 3} {
			if err := insertMarker(ctx, platform, streamRef, day*daySecs, FlagOpen|FlagHigh|FlagLow|FlagClose); err != nil {
				return err
			}
		}

		res, err := callBatchPrune(ctx, platform, []int{streamRef}, retentionReaching(4), 1000)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, res, `
			| deleted_event_times | deleted_rows | has_more_to_delete |
			|---------------------|--------------|--------------------|
			| 1                   | 2            | false              |
		`)

		gone, err := countMarkers(ctx, platform, streamRef, 2)
		if err != nil {
			return err
		}
		if gone != 0 {
			return errors.Errorf("marker for the pruned record survived: %d", gone)
		}
		for _, day := range []int64{1, 3} {
			kept, err := countMarkers(ctx, platform, streamRef, day)
			if err != nil {
				return err
			}
			if kept != 1 {
				return errors.Errorf("marker for a surviving record on day %d was removed", day)
			}
		}
		return nil
	}
}

// A digested day carries up to four marked records, one per OHLC role, and
// get_daily_ohlc reads each role from its own marker bit. Taking one of those
// records out and leaving the other markers would keep the day on the digested
// branch with nothing carrying the missing bit, so that role would answer NULL
// beside three real values — worse than answering nothing, because it reads as
// corruption. The whole day's markers go instead, and the raw branch recomputes.
func testPruneLeavesADigestedDayReadable(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, pruneStreamName)
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}
		// Day 2 opens on the value day 1 closed at, so its open record is a
		// carry-forward duplicate while its high and low are not.
		dayTwo := 2 * daySecs
		if err := seedPruneRecords(ctx, platform, streamRef, []pruneRecord{
			{Day: 1, Value: "10"},
			{Day: 3, Value: "20"},
		}); err != nil {
			return err
		}
		if err := seedPruneRecordsAt(ctx, platform, streamRef, []atRecord{
			{EventTime: dayTwo, Value: "10"},
			{EventTime: dayTwo + 3600, Value: "12"},
			{EventTime: dayTwo + 7200, Value: "8"},
		}); err != nil {
			return err
		}
		for _, marker := range []struct {
			eventTime int64
			flags     int
		}{
			{1 * daySecs, FlagOpen | FlagHigh | FlagLow | FlagClose},
			{dayTwo, FlagOpen},
			{dayTwo + 3600, FlagHigh},
			{dayTwo + 7200, FlagLow | FlagClose},
			{3 * daySecs, FlagOpen | FlagHigh | FlagLow | FlagClose},
		} {
			if err := insertMarker(ctx, platform, streamRef, marker.eventTime, marker.flags); err != nil {
				return err
			}
		}

		res, err := callBatchPrune(ctx, platform, []int{streamRef}, retentionReaching(4), 1000)
		if err != nil {
			return err
		}
		// One event time, and with it every marker day 2 carried: three of them.
		assertMarkdownEquals(t, res, `
			| deleted_event_times | deleted_rows | has_more_to_delete |
			|---------------------|--------------|--------------------|
			| 1                   | 4            | false              |
		`)

		markers, err := countMarkers(ctx, platform, streamRef, 2)
		if err != nil {
			return err
		}
		if markers != 0 {
			return errors.Errorf("day 2 kept %d markers; a day that loses a record loses them all", markers)
		}

		// Read from the raw branch now, over what is left of the day.
		ohlc, err := callGetDailyOHLC(ctx, platform, streamRef, 2)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, ohlc, `
			| open_value | high_value | low_value | close_value |
			|------------|------------|-----------|-------------|
			| 12.000000000000000000 | 12.000000000000000000 | 8.000000000000000000 | 8.000000000000000000 |
		`)
		return nil
	}
}

// =============================================================================
// The sweep
// =============================================================================

// auto_prune_duplicates has no queue to drain: it walks streams.id from wherever
// the cursor left off, prunes that slice, and writes the cursor back.
func testAutoPruneSweepsAndMovesTheCursor(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, pruneStreamName)
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}
		if err := seedPruneRecords(ctx, platform, streamRef, []pruneRecord{
			{Day: 1, Value: "4"},
			{Day: 2, Value: "4"},
			{Day: 3, Value: "4"},
			{Day: 4, Value: "6"},
		}); err != nil {
			return err
		}

		cursor, err := pruneCursor(ctx, platform)
		if err != nil {
			return err
		}
		if cursor != 0 {
			return errors.Errorf("the seeded cursor should start at 0, got %d", cursor)
		}

		// A capped batch has not finished the streams it touched, so the cursor
		// stays where it was and the next call picks them up again.
		res, logs, err := callAutoPrune(ctx, platform, 1, 100, nil)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, res, `
			| swept_streams | deleted_event_times | deleted_rows | has_more_to_delete |
			|---------------|---------------------|--------------|--------------------|
			| 1             | 1                   | 1            | true               |
		`)
		cursor, err = pruneCursor(ctx, platform)
		if err != nil {
			return err
		}
		if cursor != 0 {
			return errors.Errorf("a capped batch moved the cursor to %d; it should still be 0", cursor)
		}

		// The NOTICE is the only channel a scheduler has: an action's return value
		// is not visible to an SDK caller.
		want := `auto_prune_duplicates:{"swept_streams":1,"deleted_event_times":1,"deleted_rows":1,"has_more_to_delete":true}`
		if !slices.Contains(logs, want) {
			return errors.Errorf("auto_prune_duplicates did not emit its NOTICE\nwant: %s\n got: %v", want, logs)
		}

		// Retention is left NULL so this reads the configured 30 days. Every
		// fixture here sits in 1970, so all of it is comfortably out of the window.
		res, _, err = callAutoPrune(ctx, platform, 1000, 100, nil)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, res, `
			| swept_streams | deleted_event_times | deleted_rows | has_more_to_delete |
			|---------------|---------------------|--------------|--------------------|
			| 1             | 1                   | 1            | false              |
		`)

		cursor, err = pruneCursor(ctx, platform)
		if err != nil {
			return err
		}
		if cursor != streamRef {
			return errors.Errorf("the cursor should sit on the last stream of the pass: got %d, want %d", cursor, streamRef)
		}

		surviving, err := readStoredRecords(ctx, platform, streamRef)
		if err != nil {
			return err
		}
		if got, want := surviving, "86400=4 345600=6"; got != want {
			return errors.Errorf("the sweep pruned the wrong records:\n got: %s\nwant: %s", got, want)
		}
		return nil
	}
}

// =============================================================================
// Configuration
// =============================================================================

// The row is seeded by migration 056, disabled. digest_config is not seeded, and
// the consequence is a network where digest has never run because nobody noticed
// the row was missing — this asserts we did not repeat that.
func testPruneConfigShipsDisabled(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		res, err := callActionAsStrings(ctx, platform, "get_duplicate_prune_config", 4)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, res, `
			| enabled | retention_days | prune_schedule | last_stream_ref |
			|---------|----------------|----------------|-----------------|
			| false   | 30             | 0 */6 * * *    | 0               |
		`)
		return nil
	}
}

// =============================================================================
// Refusals
// =============================================================================

func testPruneRejectsBadArguments(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		cases := []struct {
			action string
			args   []any
			want   string
		}{
			{"batch_prune_duplicates", []any{[]int{1}, int64(0), 10}, "retention_seconds must be a positive integer"},
			{"batch_prune_duplicates", []any{[]int{1}, int64(86400), 0}, "delete_cap must be a positive integer"},
			{"auto_prune_duplicates", []any{10, 0, 30}, "stream_batch_size must be a positive integer"},
			{"auto_prune_duplicates", []any{10, 100, 0}, "retention_days must be a positive integer"},
		}
		for _, c := range cases {
			// Column counts differ between the two actions. They only matter if a
			// call stops erroring, which is exactly when this should be noticed.
			columns := 3
			if c.action == "auto_prune_duplicates" {
				columns = 4
			}
			if _, err := callActionAsStrings(ctx, platform, c.action, columns, c.args...); err == nil {
				return errors.Errorf("%s accepted %v", c.action, c.args)
			} else if !strings.Contains(err.Error(), c.want) {
				return errors.Errorf("%s: expected %q, got %v", c.action, c.want, err)
			}
		}

		// An empty batch is not an error — it is the shape auto_prune hands over
		// when a slice of the sweep holds nothing.
		res, err := callBatchPrune(ctx, platform, []int{}, 86400, 10)
		if err != nil {
			return err
		}
		assertMarkdownEquals(t, res, `
			| deleted_event_times | deleted_rows | has_more_to_delete |
			|---------------------|--------------|--------------------|
			| 0                   | 0            | false              |
		`)
		return nil
	}
}

// Both actions delete consensus state, so only the block leader may run them.
func testPruneIsLeaderOnly(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		if err != nil {
			return errors.Wrap(err, "generate secp256k1 key")
		}
		pub, ok := pubGeneric.(*crypto.Secp256k1PublicKey)
		if !ok {
			return errors.New("unexpected pubkey type")
		}

		callAs := func(action string, args []any, signer []byte) (*common.CallResult, error) {
			caller := ""
			if ident, e := extauth.GetIdentifier(coreauth.EthPersonalSignAuth, signer); e == nil {
				caller = ident
			}
			tx := &common.TxContext{
				Ctx:           ctx,
				BlockContext:  &common.BlockContext{Height: 1, Timestamp: time.Now().Unix(), Proposer: pub},
				Signer:        signer,
				Caller:        caller,
				TxID:          platform.Txid(),
				Authenticator: coreauth.EthPersonalSignAuth,
			}
			return platform.Engine.Call(&common.EngineContext{TxContext: tx}, platform.DB, "", action, args,
				func(*common.Row) error { return nil })
		}

		leader := crypto.EthereumAddressFromPubKey(pub)
		for _, c := range []struct {
			action string
			args   []any
		}{
			{"batch_prune_duplicates", []any{[]int{}, int64(86400), 10}},
			{"auto_prune_duplicates", []any{10, 100, 30}},
		} {
			r, err := callAs(c.action, c.args, platform.Deployer)
			if err != nil {
				return errors.Wrapf(err, "%s non-leader call", c.action)
			}
			if r == nil || r.Error == nil || !strings.Contains(r.Error.Error(), "Only the current block leader") {
				return errors.Errorf("%s ran for a non-leader", c.action)
			}

			r, err = callAs(c.action, c.args, leader)
			if err != nil {
				return errors.Wrapf(err, "%s leader call", c.action)
			}
			if r != nil && r.Error != nil {
				return errors.Wrapf(r.Error, "%s refused the leader", c.action)
			}
		}
		return nil
	}
}

// =============================================================================
// Helpers
// =============================================================================

// WithPruneStream registers the provider and creates an empty primitive stream.
// Records are seeded per test rather than from markdown, because most of these
// cases need control over created_at or truflation_created_at.
func WithPruneStream(next func(context.Context, *kwilTesting.Platform) error) func(context.Context, *kwilTesting.Platform) error {
	return WithSignerAndProvider(WithStreamMD(pruneStreamId, 1, pruneEmptyStreamData,
		func(ctx context.Context, platform *kwilTesting.Platform) error {
			streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, pruneStreamName)
			if err != nil {
				return errors.Wrap(err, "resolve stream ref")
			}
			if err := clearPruneRecords(ctx, platform, streamRef); err != nil {
				return err
			}
			return next(ctx, platform)
		}))
}

type pruneRecord struct {
	Day        int64
	Value      string
	CreatedAt  int64
	Truflation string
	// Seconds reads Day as an event time rather than a day number.
	Seconds bool
}

// seedPruneRecords writes records straight to primitive_events. Going through
// insert_records would stamp created_at from the block height and leave no way
// to write a revision, and truflation_created_at only reaches the table through
// the Truflation-specific insert.
func seedPruneRecords(ctx context.Context, platform *kwilTesting.Platform, streamRef int, records []pruneRecord) error {
	kit, err := newCtxKit(ctx, platform, true)
	if err != nil {
		return err
	}
	for _, record := range records {
		value, err := kwilTypes.ParseDecimalExplicit(record.Value, 36, 18)
		if err != nil {
			return errors.Wrapf(err, "parse value %q", record.Value)
		}
		createdAt := record.CreatedAt
		if createdAt == 0 {
			createdAt = 100
		}
		eventTime := record.Day * daySecs
		if record.Seconds {
			eventTime = record.Day
		}
		args := map[string]any{
			"$sr": streamRef,
			"$et": eventTime,
			"$v":  value,
			"$ca": createdAt,
		}
		// The column is left out rather than bound to nil, so a record with no
		// Truflation timestamp is written the way a plain insert_records writes it.
		sql := `INSERT INTO primitive_events (stream_ref, event_time, value, created_at)
			 VALUES ($sr, $et, $v, $ca)`
		if record.Truflation != "" {
			args["$tca"] = record.Truflation
			sql = `INSERT INTO primitive_events (stream_ref, event_time, value, created_at, truflation_created_at)
			 VALUES ($sr, $et, $v, $ca, $tca)`
		}
		if err := platform.Engine.Execute(kit.eng, platform.DB, sql, args,
			func(*common.Row) error { return nil }); err != nil {
			return errors.Wrapf(err, "insert record on day %d", record.Day)
		}
	}
	return nil
}

// atRecord is the same as pruneRecord but keyed on an explicit event time, for
// the sub-daily fixtures a digested day needs.
type atRecord struct {
	EventTime int64
	Value     string
}

func seedPruneRecordsAt(ctx context.Context, platform *kwilTesting.Platform, streamRef int, records []atRecord) error {
	converted := make([]pruneRecord, 0, len(records))
	for _, record := range records {
		converted = append(converted, pruneRecord{Day: record.EventTime, Value: record.Value, Seconds: true})
	}
	return seedPruneRecords(ctx, platform, streamRef, converted)
}

func clearPruneRecords(ctx context.Context, platform *kwilTesting.Platform, streamRef int) error {
	kit, err := newCtxKit(ctx, platform, true)
	if err != nil {
		return err
	}
	for _, table := range []string{"primitive_events", "primitive_event_type"} {
		err = platform.Engine.Execute(kit.eng, platform.DB,
			fmt.Sprintf("DELETE FROM %s WHERE stream_ref = $sr", table),
			map[string]any{"$sr": streamRef},
			func(*common.Row) error { return nil })
		if err != nil {
			return errors.Wrapf(err, "clear %s", table)
		}
	}
	return nil
}

func insertMarker(ctx context.Context, platform *kwilTesting.Platform, streamRef int, eventTime int64, markerType int) error {
	kit, err := newCtxKit(ctx, platform, true)
	if err != nil {
		return err
	}
	return platform.Engine.Execute(kit.eng, platform.DB,
		`INSERT INTO primitive_event_type (stream_ref, event_time, type) VALUES ($sr, $et, $t)
		 ON CONFLICT (stream_ref, event_time) DO UPDATE SET type = EXCLUDED.type`,
		map[string]any{"$sr": streamRef, "$et": eventTime, "$t": markerType},
		func(*common.Row) error { return nil })
}

// readStoredRecords renders every row the stream holds as "event_time=value",
// oldest first, so a whole expectation fits on one line.
func readStoredRecords(ctx context.Context, platform *kwilTesting.Platform, streamRef int) (string, error) {
	kit, err := newCtxKit(ctx, platform, true)
	if err != nil {
		return "", err
	}
	var parts []string
	err = platform.Engine.Execute(kit.eng, platform.DB,
		`SELECT event_time, value FROM primitive_events
		 WHERE stream_ref = $sr ORDER BY event_time ASC, created_at ASC`,
		map[string]any{"$sr": streamRef},
		func(row *common.Row) error {
			eventTime, ok := row.Values[0].(int64)
			if !ok {
				return errors.Errorf("unexpected event_time type %T", row.Values[0])
			}
			value, ok := row.Values[1].(*kwilTypes.Decimal)
			if !ok {
				return errors.Errorf("unexpected value type %T", row.Values[1])
			}
			parts = append(parts, fmt.Sprintf("%d=%s", eventTime, trimDecimal(value.String())))
			return nil
		})
	return strings.Join(parts, " "), err
}

// trimDecimal drops the NUMERIC(36,18) tail so an expectation reads as "10"
// rather than "10.000000000000000000".
func trimDecimal(value string) string {
	if !strings.Contains(value, ".") {
		return value
	}
	value = strings.TrimRight(value, "0")
	return strings.TrimSuffix(value, ".")
}

// readEachDay asks get_record for the value standing on each day. A one-second
// window would do; from == to works because get_record answers with the anchor,
// the newest record at or before `from`.
func readEachDay(ctx context.Context, platform *kwilTesting.Platform, days []int64) (string, error) {
	address, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return "", errors.Wrap(err, "deployer address")
	}
	var parts []string
	for _, day := range days {
		at := day * daySecs
		rows, err := callActionAsStrings(ctx, platform, "get_record", 2,
			address.Address(), pruneStreamId.String(), at, at, nil)
		if err != nil {
			return "", errors.Wrapf(err, "get_record at %d", at)
		}
		if len(rows) != 1 {
			// Without this the before/after comparison would pass on two empty
			// strings, which is the one way it could look green while proving
			// nothing.
			return "", errors.Errorf("get_record answered %d rows for day %d; expected exactly one", len(rows), day)
		}
		parts = append(parts, fmt.Sprintf("%d->%s", day, rows[0][1]))
	}
	return strings.Join(parts, " "), nil
}

func callBatchPrune(ctx context.Context, platform *kwilTesting.Platform, streamRefs []int, retentionSeconds int64, deleteCap int) ([]procedure.ResultRow, error) {
	return callActionAsStrings(ctx, platform, "batch_prune_duplicates", 3, streamRefs, retentionSeconds, deleteCap)
}

// callAutoPrune also returns the action's logs, because the NOTICE it emits is
// the only thing a scheduler broadcasting this as a transaction can read.
// retentionDays is `any` so a test can pass nil and exercise the configured value.
func callAutoPrune(
	ctx context.Context,
	platform *kwilTesting.Platform,
	deleteCap int,
	streamBatchSize int,
	retentionDays any,
) ([]procedure.ResultRow, []string, error) {
	kit, err := newCtxKit(ctx, platform, false)
	if err != nil {
		return nil, nil, err
	}
	var out []procedure.ResultRow
	r, err := platform.Engine.Call(kit.eng, platform.DB, "", "auto_prune_duplicates",
		[]any{deleteCap, streamBatchSize, retentionDays},
		func(row *common.Row) error {
			if len(row.Values) != 4 {
				return errors.Errorf("auto_prune_duplicates: expected 4 columns, got %d", len(row.Values))
			}
			rowOut := make(procedure.ResultRow, 4)
			for i := range rowOut {
				rowOut[i] = fmt.Sprintf("%v", row.Values[i])
			}
			out = append(out, rowOut)
			return nil
		})
	if err != nil {
		return nil, nil, err
	}
	if r != nil && r.Error != nil {
		return nil, nil, errors.Wrap(r.Error, "auto_prune_duplicates failed")
	}
	return out, r.Logs, nil
}

func pruneCursor(ctx context.Context, platform *kwilTesting.Platform) (int, error) {
	return queryCount(ctx, platform, "SELECT last_stream_ref FROM duplicate_prune_config WHERE id = 1", map[string]any{})
}

// retentionReaching turns a day into the retention window that puts the cutoff
// just before it, so a fixture written in 1970 can be split at any day without
// caring what today's date is. Days strictly below the named one are candidates;
// the named day and everything after it are out of scope.
//
// The cutoff lands half a day short rather than exactly on the boundary. The
// action reads its own @block_timestamp a moment after this samples the clock,
// and a record sitting exactly on the cutoff would change sides if the second
// ticked in between.
func retentionReaching(day int64) int64 {
	return time.Now().Unix() - (day*daySecs - daySecs/2)
}
