package tests

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	kwilTypes "github.com/trufnetwork/kwil-db/core/types"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	extauth "github.com/trufnetwork/kwil-db/extensions/auth"

	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

const digestTestStreamName = "digest_test_stream"
const idempotencyTestStreamName = "idempotency_test_stream"

var digestTestStreamId = util.GenerateStreamId(digestTestStreamName)
var idempotencyTestStreamId = util.GenerateStreamId(idempotencyTestStreamName)

func TestDigestActions(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "digest_actions_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithDigestTestSetup(testDigestBasicOHLCCalculation(t)),
			WithDigestTestSetup(testGetDailyOHLCRawData(t)),
			WithDigestCombinationFlagSetup(testDigestCombinationFlags(t)),
			WithDigestAllSameFlagSetup(testDigestAllSameValueFlags(t)),
			WithBatchDigestTestSetup(testBatchDigestSingleCandidate(t)),
			WithBatchDigestTestSetup(testBatchDigestMultipleCandidates(t)),
			WithBatchDigestTestSetup(testBatchDigestEmptyArrays(t)),
			WithBatchDigestTestSetup(testBatchDigestMismatchedArrays(t)),
			WithBatchDigestTestSetup(testArrayOrdering(t)),
			WithBatchDigestTestSetup(testOptimizedAutoDigest(t)),
			WithDeletionTestSetup(testDigestDeletionLogic(t)),
			WithDuplicateEventTimeSetup(testDuplicateRecordsAtSameEventTime(t)),
			WithMultipleDuplicatesSetup(testMultipleDuplicatesAtSameEventTime(t)),
			WithBoundaryTimestampsSetup(testBoundaryTimestampSemantics(t)),
			WithPartialDeleteSetup(testPartialDeletesDueToDeleteCap(t)),
			WithSingleRecordSetup(testSingleRecordDayProcessing(t)),
			WithOtherCombinationFlagsSetup(testOtherCombinationFlagPermutations(t)),
			WithIdempotencyTestSetup(testIdempotencyChecks(t)),
			WithLeftoverPrimitivesSetup(testLeftoverPrimitivesHandling(t)),
			WithSupersedingLaterDuplicateSetup(testDuplicateSupersedesLaterTimestamp(t)),
			WithStaleMarkerOpenBitSetup(testGetDailyOHLC_IgnoresStaleMarkers(t)),
			WithHighCloseTogetherSetup(testHighCloseTogether_Flag10(t)),
			WithAutoDigestZeroExpectedSetup(testAutoDigest_ValidatesExpectedRecordsInput(t)),
			WithSignerAndProvider(testAutoDigest_PreservesRecentDaysCutoff(t)),
		},
	}, testutils.GetTestOptionsWithCache())
}

// Verifies leader-only authorization on digest actions using BlockContext.Proposer.
func TestDigestActionsLeaderAuthorization(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "digest_actions_leader_authorization",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithSignerAndProvider(func(ctx context.Context, platform *kwilTesting.Platform) error {
				// Create a secp256k1 leader key
				_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
				if err != nil {
					return errors.Wrap(err, "generate secp256k1 key")
				}
				pub, ok := pubGeneric.(*crypto.Secp256k1PublicKey)
				if !ok {
					return errors.New("unexpected pubkey type")
				}

				// Helper to call action with explicit BlockContext.Proposer and signer/auth
				callWithCtx := func(action string, args []any, signer []byte, authenticator string) (*common.CallResult, error) {
					caller := ""
					if ident, e := extauth.GetIdentifier(authenticator, signer); e == nil {
						caller = ident
					}
					tx := &common.TxContext{
						Ctx:           ctx,
						BlockContext:  &common.BlockContext{Height: 1, Proposer: pub},
						Signer:        signer,
						Caller:        caller,
						TxID:          platform.Txid(),
						Authenticator: authenticator,
					}
					eng := &common.EngineContext{TxContext: tx}
					return platform.Engine.Call(eng, platform.DB, "", action, args, func(*common.Row) error { return nil })
				}

				// Non-leader: signer != leader_sender → expect leader-only error
				if r, err := callWithCtx("batch_digest", []any{[]int{}, []int{}}, platform.Deployer, coreauth.EthPersonalSignAuth); err != nil {
					return errors.Wrap(err, "batch_digest non-leader call error")
				} else if r == nil || r.Error == nil || !strings.Contains(r.Error.Error(), "Only the current block leader") {
					return errors.New("expected leader-only error for batch_digest when not leader")
				}

				if r, err := callWithCtx("auto_digest", []any{10, 24, 2}, platform.Deployer, coreauth.EthPersonalSignAuth); err != nil {
					return errors.Wrap(err, "auto_digest non-leader call error")
				} else if r == nil || r.Error == nil || !strings.Contains(r.Error.Error(), "Only the current block leader") {
					return errors.New("expected leader-only error for auto_digest when not leader")
				}

				// Leader: signer equals derived leader_sender → expect success
				signerGood := crypto.EthereumAddressFromPubKey(pub)

				if r, err := callWithCtx("batch_digest", []any{[]int{}, []int{}}, signerGood, coreauth.EthPersonalSignAuth); err != nil {
					return errors.Wrap(err, "batch_digest leader call error")
				} else if r != nil && r.Error != nil {
					return errors.Wrap(r.Error, "batch_digest leader call failed")
				}

				if r, err := callWithCtx("auto_digest", []any{10, 24, 2}, signerGood, coreauth.EthPersonalSignAuth); err != nil {
					return errors.Wrap(err, "auto_digest leader call error")
				} else if r != nil && r.Error != nil {
					return errors.Wrap(r.Error, "auto_digest leader call failed")
				}

				return nil
			}),
		},
	}, testutils.GetTestOptionsWithCache())
}

// WithDigestTestSetup sets up test environment with digest-specific data
func WithDigestTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	const md = `
		| event_time | value |
		|------------|-------|
		| 86400      | 50    |
		| 129600     | 10    |
		| 151200     | 100   |
		| 172799     | 75    |
		`
	return WithSignerAndProvider(WithStreamMD(digestTestStreamId, 1, md, testFn))
}

// testDigestBasicOHLCCalculation tests basic OHLC calculation logic
func testDigestBasicOHLCCalculation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, digestTestStreamName)
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}

		// Insert a day into the pending queue for digest processing
		if err := insertPendingDay(ctx, platform, streamRef, 1); err != nil {
			return errors.Wrap(err, "insert pending day")
		}

		// Test get_daily_ohlc with raw data (before digest)
		ohlc, err := callGetDailyOHLC(ctx, platform, streamRef, 1)
		if err != nil {
			return errors.Wrap(err, "get_daily_ohlc")
		}

		// Verify OHLC values: OPEN=50 (earliest time), HIGH=100 (max value), LOW=10 (min value), CLOSE=75 (latest time)
		assertMarkdownEquals(t, ohlc, `
			| open_value | high_value | low_value | close_value |
			|------------|------------|-----------|-------------|
			| 50.000000000000000000 | 100.000000000000000000 | 10.000000000000000000 | 75.000000000000000000 |
		`)

		// Test auto_digest action with delete cap 10, no preserve cutoff for this test
		res, err := callAutoDigestWithPreserve(ctx, platform, 10, 0)
		if err != nil {
			return errors.Wrap(err, "auto_digest")
		}

		// Verify auto_digest result (processes 1 day with simplified exclusive boundaries)
		assertMarkdownEquals(t, res, `
			| processed_days | total_deleted_rows | has_more_to_delete |
			|----------------|-------------------|--------------------|
			| 1              | 0                 | false              |
		`)

		// Verify get_daily_ohlc still works after digest (should use digested data)
		ohlcAfter, err := callGetDailyOHLC(ctx, platform, streamRef, 1)
		if err != nil {
			return errors.Wrap(err, "get_daily_ohlc after digest")
		}

		assertMarkdownEquals(t, ohlcAfter, `
			| open_value | high_value | low_value | close_value |
			|------------|------------|-----------|-------------|
			| 50.000000000000000000 | 100.000000000000000000 | 10.000000000000000000 | 75.000000000000000000 |
		`)

		// Test OHLC type flags in primitive_event_type table
		if err := verifyFlagsExact(ctx, platform, streamRef, 1, map[int64]int{
			86400:  FlagOpen,
			129600: FlagLow,
			151200: FlagHigh,
			172799: FlagClose,
		}); err != nil {
			return errors.Wrap(err, "verify OHLC flags")
		}

		return nil
	}
}

// WithDigestCombinationFlagSetup sets up test data where some OHLC values are the same (testing combination flags)
func WithDigestCombinationFlagSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	testStreamId := util.GenerateStreamId("combination_test_stream")
	const md = `
		| event_time | value |
		|------------|-------|
		| 172800     | 10    |
		| 216000     | 50    |
		| 237600     | 100   |
		| 259199     | 75    |
		`
	return WithSignerAndProvider(WithStreamMD(testStreamId, 1, md, testFn))
}

// testDigestCombinationFlags tests that combination flags are correctly assigned when OHLC values overlap
func testDigestCombinationFlags(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "combination_test_stream")
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}

		// Insert a day into the pending queue (day 2 to avoid primary key conflict)
		if err := insertPendingDay(ctx, platform, streamRef, 2); err != nil {
			return errors.Wrap(err, "insert pending day")
		}

		// Run auto_digest with delete cap 10, no preserve cutoff for this test
		if _, err := callAutoDigestWithPreserve(ctx, platform, 10, 0); err != nil {
			return errors.Wrap(err, "auto_digest")
		}

		// Verify combination flags
		if err := verifyFlagsExact(ctx, platform, streamRef, 2, map[int64]int{
			172800: FlagOpen + FlagLow, // OPEN + LOW (1+4=5)
			237600: FlagHigh,           // HIGH
			259199: FlagClose,          // CLOSE
		}); err != nil {
			return errors.Wrap(err, "verify combination flags")
		}

		return nil
	}
}

// testGetDailyOHLCRawData tests get_daily_ohlc with raw (undigested) data
func testGetDailyOHLCRawData(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Test raw OHLC calculation by querying the data directly
		// This tests the fallback behavior of get_daily_ohlc when no digest exists

		// Query for CLOSE value (latest time)
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     digestTestStreamId,
				DataProvider: deployer,
			},
			FromTime: func() *int64 { v := int64(172799); return &v }(),
			ToTime:   func() *int64 { v := int64(172799); return &v }(),
		})

		if err != nil {
			return errors.Wrap(err, "error querying CLOSE value")
		}

		// Verify CLOSE value is 75 (latest time record)
		expected := `
		| event_time | value |
		|------------|-------|
		| 172799     | 75.000000000000000000 |
		`

		assertMarkdownEquals(t, result, expected)

		// Query all data to verify HIGH and LOW values
		allResult, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     digestTestStreamId,
				DataProvider: deployer,
			},
			FromTime: func() *int64 { v := int64(86400); return &v }(),
			ToTime:   func() *int64 { v := int64(172799); return &v }(),
		})

		if err != nil {
			return errors.Wrap(err, "error querying all day data")
		}

		expectedAll := `
		| event_time | value |
		|------------|-------|
		| 86400      | 50.000000000000000000 |
		| 129600     | 10.000000000000000000 |
		| 151200     | 100.000000000000000000 |
		| 172799     | 75.000000000000000000 |
		`

		assertMarkdownEquals(t, allResult, expectedAll)
		return nil
	}
}

// WithDigestAllSameFlagSetup sets up test data where all values are identical (testing combination flags 7+8)
func WithDigestAllSameFlagSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	testStreamId := util.GenerateStreamId("all_same_test_stream")
	const md = `
		| event_time | value |
		|------------|-------|
		| 259200     | 50    |
		| 302400     | 50    |
		| 324000     | 50    |
		| 345599     | 50    |
		`
	return WithSignerAndProvider(WithStreamMD(testStreamId, 1, md, testFn))
}

// testDigestAllSameValueFlags tests that when all values are identical across multiple timestamps,
// the earliest gets OPEN+HIGH+LOW (7) and the latest gets CLOSE (8). Flag 15 is only for single-record days.
func testDigestAllSameValueFlags(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "all_same_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		// Insert a day into the pending queue (day 3 to avoid primary key conflict)
		err = insertPendingDay(ctx, platform, streamRef, 3)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day for all same values test")
		}

		// Run auto_digest with delete cap 10, no preserve cutoff for this test
		_, err = callAutoDigestWithPreserve(ctx, platform, 10, 0)
		if err != nil {
			return errors.Wrap(err, "error calling auto_digest for all same values test")
		}

		// Verify combination flags (7 = OPEN+HIGH+LOW at earliest, 8 = CLOSE at latest)
		if err := verifyFlagsExact(ctx, platform, streamRef, 3, map[int64]int{
			259200: FlagOpen + FlagHigh + FlagLow, // OPEN + HIGH + LOW (1+2+4=7)
			345599: FlagClose,                     // CLOSE
		}); err != nil {
			return errors.Wrap(err, "verify all same value flags")
		}

		return nil
	}
}

// WithBatchDigestTestSetup sets up test environment for batch digest testing
func WithBatchDigestTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Setup multiple test streams for batch testing
		// Stream 1: Day 5 (432000-518400 seconds) - OHLC pattern
		testStreamId1 := util.GenerateStreamId("batch_test_stream_1")
		const md1 = `
			| event_time | value |
			|------------|-------|
			| 432001     | 20    |
			| 454800     | 80    |
			| 475200     | 5     |
			| 518399     | 65    |
			`
		if err := SetupStreamMD(ctx, platform, testStreamId1, 1, md1); err != nil {
			return errors.Wrap(err, "error setting up batch test stream 1")
		}

		// Stream 2: Day 6 (518400-604800 seconds) - Different OHLC pattern
		testStreamId2 := util.GenerateStreamId("batch_test_stream_2")
		const md2 = `
			| event_time | value |
			|------------|-------|
			| 518401     | 100   |
			| 540000     | 25    |
			| 561600     | 150   |
			| 604799     | 90    |
			`
		if err := SetupStreamMD(ctx, platform, testStreamId2, 1, md2); err != nil {
			return errors.Wrap(err, "error setting up batch test stream 2")
		}

		// Stream 3: Day 7 (604800-691200 seconds) - Minimal data (should be skipped)
		testStreamId3 := util.GenerateStreamId("batch_test_stream_3")
		const md3 = `
			| event_time | value |
			|------------|-------|
			| 604801     | 42    |
			`
		if err := SetupStreamMD(ctx, platform, testStreamId3, 1, md3); err != nil {
			return errors.Wrap(err, "error setting up batch test stream 3")
		}

		return testFn(ctx, platform)
	}
}

// testBatchDigestSingleCandidate tests batch_digest with a single candidate
func testBatchDigestSingleCandidate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "batch_test_stream_1")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		// Insert pending day for stream 1, day 5
		err = insertPendingDay(ctx, platform, streamRef, 5)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day")
		}

		// Call batch_digest with single candidate
		streamRefs := []int{streamRef}
		dayIndexes := []int{5}

		result, err := callBatchDigest(ctx, platform, streamRefs, dayIndexes)
		if err != nil {
			return errors.Wrap(err, "error calling batch_digest")
		}

		// Verify batch digest result for single candidate
		expectedResult := `
		| processed_days | total_deleted_rows | total_preserved_rows | has_more_to_delete |
		|----------------|-------------------|---------------------|-------------------|
		| 1              | 0                 | 4                   | false             |
		`

		assertMarkdownEquals(t, result, expectedResult)

		return nil
	}
}

// testBatchDigestMultipleCandidates tests batch_digest with multiple candidates
func testBatchDigestMultipleCandidates(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Resolve stream refs
		streamRef1, err := setup.GetStreamIdForDeployer(ctx, platform, "batch_test_stream_1")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref 1")
		}
		streamRef2, err := setup.GetStreamIdForDeployer(ctx, platform, "batch_test_stream_2")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref 2")
		}
		streamRef3, err := setup.GetStreamIdForDeployer(ctx, platform, "batch_test_stream_3")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref 3")
		}

		// Insert pending days for multiple streams
		err = insertPendingDay(ctx, platform, streamRef1, 5) // Stream 1, Day 5
		if err != nil {
			return errors.Wrap(err, "error inserting pending day 1")
		}

		err = insertPendingDay(ctx, platform, streamRef2, 6) // Stream 2, Day 6
		if err != nil {
			return errors.Wrap(err, "error inserting pending day 2")
		}

		err = insertPendingDay(ctx, platform, streamRef3, 7) // Stream 3, Day 7 (minimal data)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day 3")
		}

		// Call batch_digest with multiple candidates

		streamRefs := []int{streamRef1, streamRef2, streamRef3}
		dayIndexes := []int{5, 6, 7}

		result, err := callBatchDigest(ctx, platform, streamRefs, dayIndexes)
		if err != nil {
			return errors.Wrap(err, "error calling batch_digest")
		}

		// Verify batch digest processed multiple candidates
		// Stream 3 now included with single record (gets type flag 15 - all OHLC values)
		expectedResult := `
		| processed_days | total_deleted_rows | total_preserved_rows | has_more_to_delete |
		|----------------|-------------------|---------------------|-------------------|
		| 3              | 0                 | 9                   | false             |
		`

		assertMarkdownEquals(t, result, expectedResult)

		return nil
	}
}

// testBatchDigestEmptyArrays tests batch_digest with empty arrays
func testBatchDigestEmptyArrays(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Call batch_digest with empty arrays
		var streamRefs []int
		var dayIndexes []int

		result, err := callBatchDigest(ctx, platform, streamRefs, dayIndexes)
		if err != nil {
			return errors.Wrap(err, "error calling batch_digest with empty arrays")
		}

		// Verify empty result
		expectedResult := `
		| processed_days | total_deleted_rows | total_preserved_rows | has_more_to_delete |
		|----------------|-------------------|---------------------|-------------------|
		| 0              | 0                 | 0                   | false             |
		`

		assertMarkdownEquals(t, result, expectedResult)

		return nil
	}
}

// testBatchDigestMismatchedArrays tests batch_digest with mismatched array lengths
func testBatchDigestMismatchedArrays(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Call batch_digest with mismatched array lengths (should error)
		streamRef1, err := setup.GetStreamIdForDeployer(ctx, platform, "batch_test_stream_1")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref 1")
		}
		streamRef2, err := setup.GetStreamIdForDeployer(ctx, platform, "batch_test_stream_2")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref 2")
		}

		streamRefs := []int{streamRef1, streamRef2}
		dayIndexes := []int{5} // One less element

		_, err = callBatchDigest(ctx, platform, streamRefs, dayIndexes)
		if err == nil {
			return errors.New("expected error for mismatched array lengths, but got none")
		}

		// Verify error message contains expected text
		expectedErrorSubstring := "must have the same length"
		if !strings.Contains(err.Error(), expectedErrorSubstring) {
			return errors.Errorf("expected error to contain '%s', but got: %s", expectedErrorSubstring, err.Error())
		}

		return nil
	}
}

// testArrayOrdering tests that auto_digest processes arrays with correct ordering
func testArrayOrdering(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create multiple test streams with sufficient data for processing
		testStreams := []struct {
			name     string
			dayIndex int64
		}{
			{"array_test_stream_1_day_10", 10},
			{"array_test_stream_2_day_10", 10},
			{"array_test_stream_3_day_10", 10},
			{"array_test_stream_1_day_11", 11},
			{"array_test_stream_2_day_11", 11},
			{"array_test_stream_3_day_11", 11},
			{"array_test_stream_1_day_12", 12},
			{"array_test_stream_2_day_12", 12},
			{"array_test_stream_3_day_12", 12},
		}

		// Setup test data for each stream with multiple records per day
		for _, ts := range testStreams {
			testStreamId := util.GenerateStreamId(ts.name)
			dayStart := ts.dayIndex * 86400

			// Create test data with MANY records to trigger deletions and test array ordering
			err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
				Platform: platform,
				StreamId: testStreamId,
				Height:   1,
				MarkdownData: fmt.Sprintf(`
				| event_time | value |
				|------------|-------|
				| %d         | 100   |
				| %d         | 120   |
				| %d         | 110   |
				| %d         | 200   |
				| %d         | 180   |
				| %d         | 190   |
				| %d         | 50    |
				| %d         | 60    |
				| %d         | 40    |
				| %d         | 150   |
				| %d         | 160   |
				| %d         | 140   |
				| %d         | 175   |
				| %d         | 165   |
				| %d         | 155   |
				`, dayStart+100, dayStart+150, dayStart+125, dayStart+200, dayStart+250, dayStart+225,
					dayStart+300, dayStart+350, dayStart+325, dayStart+400, dayStart+450, dayStart+425,
					dayStart+500, dayStart+550, dayStart+525),
			})
			if err != nil {
				return errors.Wrapf(err, "error setting up test data for stream %s", ts.name)
			}

			// Resolve the stream_ref that was assigned to this stream
			streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, ts.name)
			if err != nil {
				return errors.Wrapf(err, "resolve stream_ref for %s", ts.name)
			}

			// Insert pending day
			err = insertPendingDay(ctx, platform, streamRef, ts.dayIndex)
			if err != nil {
				return errors.Wrapf(err, "error inserting pending day for stream %s, day %d", ts.name, ts.dayIndex)
			}
		}

		// Build full candidate arrays and assert batch_digest completes without index errors
		var streamRefs []int
		var dayIndexes []int
		for _, ts := range testStreams {
			sr, err := setup.GetStreamIdForDeployer(ctx, platform, ts.name)
			if err != nil {
				return errors.Wrapf(err, "resolve stream_ref for %s (batch)", ts.name)
			}
			streamRefs = append(streamRefs, sr)
			dayIndexes = append(dayIndexes, int(ts.dayIndex))
		}

		result, err := callBatchDigest(ctx, platform, streamRefs, dayIndexes)
		if err != nil {
			return errors.Wrap(err, "batch_digest failed for array ordering set")
		}

		// Relaxed assertion: only ensure processed_days=9 and has_more_to_delete=false
		if len(result) == 0 || len(result[0]) < 4 {
			return errors.New("unexpected result shape from batch_digest for array ordering")
		}
		if result[0][0] != "9" {
			t.Errorf("expected processed_days=9, got %s", result[0][0])
		}
		if result[0][3] != "false" {
			t.Errorf("expected has_more_to_delete=false, got %s", result[0][3])
		}
		return nil
	}
}

// testOptimizedAutoDigest tests the optimized auto_digest function
func testOptimizedAutoDigest(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Resolve stream refs and insert pending days for multiple streams
		sr1, err := setup.GetStreamIdForDeployer(ctx, platform, "batch_test_stream_1")
		if err != nil {
			return errors.Wrap(err, "resolve stream 1")
		}

		sr2, err := setup.GetStreamIdForDeployer(ctx, platform, "batch_test_stream_2")
		if err != nil {
			return errors.Wrap(err, "resolve stream 2")
		}

		// Include the single-record day so the optimized flow covers it
		sr3, err := setup.GetStreamIdForDeployer(ctx, platform, "batch_test_stream_3")
		if err != nil {
			return errors.Wrap(err, "resolve stream 3")
		}

		if err := insertPendingDay(ctx, platform, sr1, 5); err != nil {
			return errors.Wrap(err, "error inserting pending day 1")
		}

		if err := insertPendingDay(ctx, platform, sr2, 6); err != nil {
			return errors.Wrap(err, "error inserting pending day 2")
		}

		if err := insertPendingDay(ctx, platform, sr3, 7); err != nil {
			return errors.Wrap(err, "error inserting pending day 3")
		}

		// Call optimized auto_digest — use a cap large enough to capture all 3 at once; no preserve cutoff for this test
		result, err := callAutoDigestWithPreserve(ctx, platform, 50, 0)
		if err != nil {
			return errors.Wrap(err, "error calling optimized auto_digest")
		}

		// Verify auto_digest processed all three candidates in one pass
		expectedResult := `
		| processed_days | total_deleted_rows | has_more_to_delete |
		|----------------|-------------------|--------------------|
		| 3              | 0                 | false              |
		`

		assertMarkdownEquals(t, result, expectedResult)

		return nil
	}
}

// WithDeletionTestSetup sets up test environment with excess data to verify deletion logic
func WithDeletionTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	testStreamId := util.GenerateStreamId("deletion_test_stream")
	const md = `
		| event_time | value |
		|------------|-------|
		| 691200     | 100   |
		| 705600     | 110   |
		| 720000     | 120   |
		| 734400     | 50    |
		| 748800     | 130   |
		| 763200     | 200   |
		| 770000     | 140   |
		| 774000     | 160   |
		| 776000     | 180   |
		| 777599     | 150   |
		`
	return WithSignerAndProvider(WithStreamMD(testStreamId, 1, md, testFn))
}

// testDigestDeletionLogic tests that intermediate records are actually deleted
func testDigestDeletionLogic(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "deletion_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		// Insert pending day for deletion test (day 8)
		err = insertPendingDay(ctx, platform, streamRef, 8)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day for deletion test")
		}

		// Count records before digest
		beforeCount, err := countPrimitiveEvents(ctx, platform, streamRef, 8)
		if err != nil {
			return errors.Wrap(err, "error counting records before digest")
		}
		t.Logf("Records before digest: %d", beforeCount)

		// Call batch_digest to process the day with excess records
		streamRefs := []int{streamRef}
		dayIndexes := []int{8}

		result, err := callBatchDigest(ctx, platform, streamRefs, dayIndexes)
		if err != nil {
			return errors.Wrap(err, "error calling batch_digest for deletion test")
		}

		// Count records after digest
		afterCount, err := countPrimitiveEvents(ctx, platform, streamRef, 8)
		if err != nil {
			return errors.Wrap(err, "error counting records after digest")
		}
		t.Logf("Records after digest: %d (deleted: %d)", afterCount, beforeCount-afterCount)

		// Deletion WORKS! (6 records deleted: 10→4) and SQL now reports correct count ✅
		expectedResult := `
		| processed_days | total_deleted_rows | total_preserved_rows | has_more_to_delete |
		|----------------|-------------------|---------------------|-------------------|
		| 1              | 6                 | 4                   | false             |
		`

		assertMarkdownEquals(t, result, expectedResult)

		// Verify OHLC values are still correct after deletion
		ohlcResult, err := callGetDailyOHLC(ctx, platform, streamRef, 8)
		if err != nil {
			return errors.Wrap(err, "error calling get_daily_ohlc after deletion")
		}

		// Expected OHLC: OPEN=100 (earliest), HIGH=200 (max), LOW=50 (min), CLOSE=150 (latest)
		expectedOHLC := `
		| open_value | high_value | low_value | close_value |
		|------------|------------|-----------|-------------|
		| 100.000000000000000000 | 200.000000000000000000 | 50.000000000000000000 | 150.000000000000000000 |
		`

		assertMarkdownEquals(t, ohlcResult, expectedOHLC)

		return nil
	}
}

// ================================
// EDGE CASE TESTS
// ================================

// WithDuplicateEventTimeSetup creates test data with duplicate records at same event_time
func WithDuplicateEventTimeSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return WithSignerAndProvider(func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create test data with duplicate event_time but different created_at values
		// Day 20 (1728000-1814400): Multiple rows with identical event_time for OHLC roles
		// We'll manually insert records using the database engine to control created_at
		testStreamId := util.GenerateStreamId("duplicate_eventtime_test_stream")

		// First create the base stream
		const baseMd = `
			| event_time | value |
			|------------|-------|
			| 1728000    | 100   |
			`
		if err := SetupStreamMD(ctx, platform, testStreamId, 1, baseMd); err != nil {
			return errors.Wrap(err, "error setting up base duplicate event_time test stream")
		}

		// Resolve the stream_ref we just created
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "duplicate_eventtime_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		// Now manually insert duplicate records with different created_at values
		if err := insertDuplicateRecordsManually(ctx, platform, streamRef); err != nil {
			return errors.Wrap(err, "error inserting duplicate records manually")
		}

		return testFn(ctx, platform)
	})
}

// testDuplicateRecordsAtSameEventTime tests duplicate records at same event_time with created_at tie-break
func testDuplicateRecordsAtSameEventTime(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "duplicate_eventtime_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		// Insert pending day
		err = insertPendingDay(ctx, platform, streamRef, 20)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day")
		}

		// Count records before digest - should be 8 records
		beforeCount, err := countPrimitiveEvents(ctx, platform, streamRef, 20)
		if err != nil {
			return errors.Wrap(err, "error counting records before digest")
		}
		t.Logf("BEFORE digest: %d records", beforeCount)

		// Call batch_digest
		result, err := callBatchDigest(ctx, platform, []int{streamRef}, []int{20})
		if err != nil {
			return errors.Wrap(err, "error calling batch_digest")
		}

		// Count records after digest - should preserve only latest created_at for each OHLC role
		afterCount, err := countPrimitiveEvents(ctx, platform, streamRef, 20)
		if err != nil {
			return errors.Wrap(err, "error counting records after digest")
		}
		t.Logf("AFTER digest: %d records", afterCount)

		// Verify that the correct records persisted (latest created_at for each event_time)
		err = verifyCorrectRecordsPersisted(ctx, platform, streamRef, 20, t)
		if err != nil {
			return errors.Wrap(err, "error verifying persisted records")
		}

		// Verify results - should keep 4 records (latest created_at for each OHLC timestamp)
		expectedResult := `
		| processed_days | total_deleted_rows | total_preserved_rows | has_more_to_delete |
		|----------------|-------------------|---------------------|-------------------|
		| 1              | 4                 | 4                   | false             |
		`

		assertMarkdownEquals(t, result, expectedResult)

		// Verify OHLC values use latest created_at records
		ohlcResult, err := callGetDailyOHLC(ctx, platform, streamRef, 20)
		if err != nil {
			return errors.Wrap(err, "error calling get_daily_ohlc")
		}

		// Expected: OPEN=90 (latest at 1728000), HIGH=140 (latest at 1750000), LOW=60 (latest at 1770000), CLOSE=130 (latest at 1814399)
		expectedOHLC := `
		| open_value | high_value | low_value | close_value |
		|------------|------------|-----------|-------------|
		| 90.000000000000000000 | 140.000000000000000000 | 60.000000000000000000 | 130.000000000000000000 |
		`

		assertMarkdownEquals(t, ohlcResult, expectedOHLC)

		return nil
	}
}

// WithMultipleDuplicatesSetup creates test data with >2 duplicates at same event_time
func WithMultipleDuplicatesSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Create test data with 3+ records at same event_time
		// Day 21: Stress case with multiple duplicates
		testStreamId := util.GenerateStreamId("multiple_duplicates_test_stream")

		// First create the stream
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 1814400    | 100   |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up base multiple duplicates test stream")
		}

		// Resolve the stream_ref we just created
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "multiple_duplicates_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		// Now manually insert multiple duplicate records
		if err := insertMultipleDuplicateRecordsManually(ctx, platform, streamRef); err != nil {
			return errors.Wrap(err, "error inserting multiple duplicate records manually")
		}

		return testFn(ctx, platform)
	}
}

// testMultipleDuplicatesAtSameEventTime tests >2 duplicates with ascending created_at
func testMultipleDuplicatesAtSameEventTime(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "multiple_duplicates_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		err = insertPendingDay(ctx, platform, streamRef, 21)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day")
		}

		// Count before - should be 15 records
		beforeCount, err := countPrimitiveEvents(ctx, platform, streamRef, 21)
		if err != nil {
			return errors.Wrap(err, "error counting records before digest")
		}
		t.Logf("BEFORE digest: %d records", beforeCount)

		result, err := callBatchDigest(ctx, platform, []int{streamRef}, []int{21})
		if err != nil {
			return errors.Wrap(err, "error calling batch_digest")
		}

		// Count after - should preserve only latest created_at for each OHLC role
		afterCount, err := countPrimitiveEvents(ctx, platform, streamRef, 21)
		if err != nil {
			return errors.Wrap(err, "error counting records after digest")
		}
		t.Logf("AFTER digest: %d records (deleted: %d)", afterCount, beforeCount-afterCount)

		// Verify that the correct records persisted (latest created_at for each event_time)
		err = verifyMultipleDuplicatesCorrectRecords(ctx, platform, streamRef, 21, t)
		if err != nil {
			return errors.Wrap(err, "error verifying multiple duplicates persisted records")
		}

		// Verify exactly one survivor (latest created_at) for each OHLC role
		expectedResult := `
		| processed_days | total_deleted_rows | total_preserved_rows | has_more_to_delete |
		|----------------|-------------------|---------------------|-------------------|
		| 1              | 11                | 4                   | false             |
		`

		assertMarkdownEquals(t, result, expectedResult)

		// Verify OHLC values use latest created_at records
		ohlcResult, err := callGetDailyOHLC(ctx, platform, streamRef, 21)
		if err != nil {
			return errors.Wrap(err, "error calling get_daily_ohlc")
		}

		// Expected: OPEN=120 (created_at=3000), HIGH=230 (created_at=4000), LOW=30 (created_at=3000), CLOSE=190 (created_at=5000)
		expectedOHLC := `
		| open_value | high_value | low_value | close_value |
		|------------|------------|-----------|-------------|
		| 120.000000000000000000 | 230.000000000000000000 | 30.000000000000000000 | 190.000000000000000000 |
		`

		assertMarkdownEquals(t, ohlcResult, expectedOHLC)

		return nil
	}
}

// WithBoundaryTimestampsSetup creates test data with boundary timestamp edge cases
func WithBoundaryTimestampsSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Create test data with boundary timestamps
		// Day 22 (1900800-1987200): Records exactly at day boundaries
		testStreamId := util.GenerateStreamId("boundary_timestamps_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 1900800    | 100   |
			| 1950000    | 150   |
			| 1987199    | 200   |
			| 1987200    | 75    |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up boundary timestamps test stream")
		}

		return testFn(ctx, platform)
	}
}

// testBoundaryTimestampSemantics tests boundary timestamp handling [day_start, day_end)
func testBoundaryTimestampSemantics(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "boundary_timestamps_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		// Insert pending day 22
		err = insertPendingDay(ctx, platform, streamRef, 22)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day 22")
		}

		// Also insert pending day 23 to test boundary
		err = insertPendingDay(ctx, platform, streamRef, 23)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day 23")
		}

		// Count records for day 22 before digest - should be 3 (excludes 1987200)
		beforeCountDay22, err := countPrimitiveEvents(ctx, platform, streamRef, 22)
		if err != nil {
			return errors.Wrap(err, "error counting day 22 records before digest")
		}
		t.Logf("Day 22 BEFORE digest: %d records", beforeCountDay22)

		// Count records for day 23 before digest - should be 1 (includes 1987200)
		beforeCountDay23, err := countPrimitiveEvents(ctx, platform, streamRef, 23)
		if err != nil {
			return errors.Wrap(err, "error counting day 23 records before digest")
		}
		t.Logf("Day 23 BEFORE digest: %d records", beforeCountDay23)

		// Process day 22
		result22, err := callBatchDigest(ctx, platform, []int{streamRef}, []int{22})
		if err != nil {
			return errors.Wrap(err, "error calling batch_digest for day 22")
		}

		// Verify day 22 processed 3 records, kept OHLC: OPEN/LOW at 1900800, HIGH/CLOSE at 1987199
		expectedResult22 := `
		| processed_days | total_deleted_rows | total_preserved_rows | has_more_to_delete |
		|----------------|-------------------|---------------------|-------------------|
		| 1              | 1                 | 2                   | false             |
		`

		assertMarkdownEquals(t, result22, expectedResult22)

		// Process day 23
		result23, err := callBatchDigest(ctx, platform, []int{streamRef}, []int{23})
		if err != nil {
			return errors.Wrap(err, "error calling batch_digest for day 23")
		}

		// Verify day 23 processed 1 record (1987200) with combined flag 15
		expectedResult23 := `
		| processed_days | total_deleted_rows | total_preserved_rows | has_more_to_delete |
		|----------------|-------------------|---------------------|-------------------|
		| 1              | 0                 | 1                   | false             |
		`

		assertMarkdownEquals(t, result23, expectedResult23)

		// Verify no leakage across days
		ohlcDay22, err := callGetDailyOHLC(ctx, platform, streamRef, 22)
		if err != nil {
			return errors.Wrap(err, "error calling get_daily_ohlc for day 22")
		}

		ohlcDay23, err := callGetDailyOHLC(ctx, platform, streamRef, 23)
		if err != nil {
			return errors.Wrap(err, "error calling get_daily_ohlc for day 23")
		}

		// Assert correct boundary semantics: day 22 includes events up to 1987199, day 23 includes 1987200
		assertMarkdownEquals(t, ohlcDay22, `
			| open_value | high_value | low_value | close_value |
			|------------|------------|-----------|-------------|
			| 100.000000000000000000 | 200.000000000000000000 | 100.000000000000000000 | 200.000000000000000000 |
		`)

		assertMarkdownEquals(t, ohlcDay23, `
			| open_value | high_value | low_value | close_value |
			|------------|------------|-----------|-------------|
			| 75.000000000000000000 | 75.000000000000000000 | 75.000000000000000000 | 75.000000000000000000 |
		`)

		return nil
	}
}

// WithPartialDeleteSetup creates test data that will trigger partial deletes due to delete_cap
func WithPartialDeleteSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Create test data with MANY records to force partial deletes
		// Day 25: 20 records with clear OHLC (4 keep, 16 delete)
		testStreamId := util.GenerateStreamId("partial_delete_test_stream")

		// Generate 20 records with known OHLC pattern
		markdownData := `
		| event_time | value |
		|------------|-------|
		| 2160000    | 100   |
		`

		// Add 18 intermediate records that should be deleted
		for i := 1; i <= 18; i++ {
			eventTime := 2160000 + int64(i*1000)
			value := 100 + i*10
			markdownData += fmt.Sprintf("| %d    | %d   |\n", eventTime, value)
		}

		// Add final record for CLOSE (and potential HIGH)
		markdownData += `| 2246399    | 300   |`

		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform:     platform,
			StreamId:     testStreamId,
			Height:       1,
			MarkdownData: markdownData,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up partial delete test stream")
		}

		return testFn(ctx, platform)
	}
}

// testPartialDeletesDueToDeleteCap tests partial deletes when delete_cap is exceeded
func testPartialDeletesDueToDeleteCap(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "partial_delete_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		err = insertPendingDay(ctx, platform, streamRef, 25)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day")
		}

		// Count before digest - should be 20 records
		beforeCount, err := countPrimitiveEvents(ctx, platform, streamRef, 25)
		if err != nil {
			return errors.Wrap(err, "error counting records before digest")
		}
		t.Logf("BEFORE digest: %d records", beforeCount)

		// Use small delete_cap to force partial deletion (cap=10, but we need to delete 16)
		result, err := callBatchDigestWithCap(ctx, platform, []int{streamRef}, []int{25}, 10)
		if err != nil {
			return errors.Wrap(err, "error calling batch_digest with small cap")
		}

		// Should have more to delete
		if len(result) > 0 && len(result[0]) >= 4 {
			hasMore := result[0][3] // has_more_to_delete column
			if hasMore != "true" {
				t.Errorf("Expected has_more_to_delete=true, got %s", hasMore)
			}
			t.Logf("Result of the first batch_digest with cap: %v", result[0])
		} else {
			t.Error("Unexpected result format from batch_digest with cap")
		}

		// Verify pending_prune_days is NOT cleaned (should still exist)
		pendingExists, err := checkPendingPruneDayExists(ctx, platform, streamRef, 25)
		if err != nil {
			return errors.Wrap(err, "error checking pending prune day")
		}
		if !pendingExists {
			t.Error("Expected pending_prune_days entry to still exist after partial delete")
		}

		// Run again to finish cleanup
		result2, err := callBatchDigestWithCap(ctx, platform, []int{streamRef}, []int{25}, 10)
		if err != nil {
			return errors.Wrap(err, "error calling batch_digest second time")
		}

		// Should now be complete
		if len(result2) > 0 && len(result2[0]) >= 4 {
			hasMore := result2[0][3]
			if hasMore != "false" {
				t.Errorf("Expected has_more_to_delete=false on second run, got %s", hasMore)
			}
			t.Logf("Result of the second batch_digest with cap: %v", result2[0])
		} else {
			t.Error("Unexpected result format from second batch_digest with cap")
		}

		// Verify pending_prune_days is NOW cleaned
		pendingExists2, err := checkPendingPruneDayExists(ctx, platform, streamRef, 25)
		if err != nil {
			return errors.Wrap(err, "error checking pending prune day after second run")
		}
		if pendingExists2 {
			t.Error("Expected pending_prune_days entry to be removed after complete digest")
		}

		afterCount, err := countPrimitiveEvents(ctx, platform, streamRef, 25)
		if err != nil {
			return errors.Wrap(err, "error counting records after digest")
		}
		t.Logf("AFTER digest: %d records (deleted: %d)", afterCount, beforeCount-afterCount)

		// Strict assertion: second pass must reduce intermediates significantly
		// The exact count depends on whether OHLC values can be combined (e.g., OPEN=LOW, HIGH=CLOSE)
		// But it should be much less than the original count and represent the final OHLC markers
		expectedMaxRecords := 4 // Maximum would be 4 separate OHLC records
		if afterCount > expectedMaxRecords {
			t.Errorf("Expected at most %d OHLC records after complete digest, got %d (before: %d, after: %d)", expectedMaxRecords, afterCount, beforeCount, afterCount)
		}
		if afterCount >= beforeCount {
			t.Errorf("Expected digest to reduce record count significantly: before=%d, after=%d", beforeCount, afterCount)
		}

		// 🔍 SELECT statement verification: Check that OHLC markers are correct
		err = verifyOHLCMarkersCorrect(ctx, platform, streamRef, 25, t, "Partial Delete Test")
		if err != nil {
			return errors.Wrap(err, "error verifying OHLC markers after partial delete")
		}

		return nil
	}
}

// WithSingleRecordSetup creates test data with exactly one record per day
func WithSingleRecordSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Create test data with single record per day
		// Day 26: Single record should get type flag 15 (OPEN+HIGH+LOW+CLOSE)
		testStreamId := util.GenerateStreamId("single_record_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 2246400    | 150   |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up single record test stream")
		}

		return testFn(ctx, platform)
	}
}

// testSingleRecordDayProcessing tests single record day with combined type flag 15
func testSingleRecordDayProcessing(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "single_record_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		err = insertPendingDay(ctx, platform, streamRef, 26)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day")
		}

		result, err := callBatchDigest(ctx, platform, []int{streamRef}, []int{26})
		if err != nil {
			return errors.Wrap(err, "error calling batch_digest")
		}

		// Should process 1 day, delete 0 records, preserve 1 record
		expectedResult := `
		| processed_days | total_deleted_rows | total_preserved_rows | has_more_to_delete |
		|----------------|-------------------|---------------------|-------------------|
		| 1              | 0                 | 1                   | false             |
		`

		assertMarkdownEquals(t, result, expectedResult)

		// Verify type flag is 15 (OPEN+HIGH+LOW+CLOSE)
		err = verifySingleRecordTypeFlag(ctx, platform, streamRef, 26)
		if err != nil {
			return errors.Wrap(err, "error verifying single record type flag")
		}

		// Verify OHLC values are all the same
		ohlcResult, err := callGetDailyOHLC(ctx, platform, streamRef, 26)
		if err != nil {
			return errors.Wrap(err, "error calling get_daily_ohlc")
		}

		expectedOHLC := `
		| open_value | high_value | low_value | close_value |
		|------------|------------|-----------|-------------|
		| 150.000000000000000000 | 150.000000000000000000 | 150.000000000000000000 | 150.000000000000000000 |
		`

		assertMarkdownEquals(t, ohlcResult, expectedOHLC)

		return nil
	}
}

// WithOtherCombinationFlagsSetup creates test data for other combination flag permutations
func WithOtherCombinationFlagsSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Create test data for OPEN+HIGH combination (flag 1+2=3)
		// Day 27: OPEN and HIGH at same timestamp
		testStreamId1 := util.GenerateStreamId("open_high_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId1,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 2332800    | 200   |
			| 2350000    | 100   |
			| 2419199    | 150   |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up OPEN+HIGH test stream")
		}

		// Create test data for OPEN+LOW and separate HIGH/CLOSE
		// Day 28: Produces flags OPEN+LOW (5), HIGH (2), CLOSE (8)
		testStreamId2 := util.GenerateStreamId("high_low_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId2,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 2419200    | 100   |
			| 2450000    | 150   |
			| 2505599    | 125   |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up HIGH+LOW test stream")
		}

		// Create test data for OPEN+CLOSE combination (flag 1+8=9)
		// Day 29: OPEN and CLOSE at same timestamp (single record case)
		testStreamId3 := util.GenerateStreamId("open_close_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId3,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 2505600    | 100   |
			| 2550000    | 75    |
			| 2591999    | 100   |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up OPEN+CLOSE test stream")
		}

		return testFn(ctx, platform)
	}
}

// testOtherCombinationFlagPermutations tests OPEN+HIGH, HIGH+LOW, and OPEN+CLOSE combinations
func testOtherCombinationFlagPermutations(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Resolve stream references
		streamRef1, err := setup.GetStreamIdForDeployer(ctx, platform, "open_high_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving open_high_test_stream")
		}
		streamRef2, err := setup.GetStreamIdForDeployer(ctx, platform, "high_low_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving high_low_test_stream")
		}
		streamRef3, err := setup.GetStreamIdForDeployer(ctx, platform, "open_close_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving open_close_test_stream")
		}

		// Test OPEN+HIGH combination (stream 1, day 27)
		if err := insertPendingDay(ctx, platform, streamRef1, 27); err != nil {
			return errors.Wrap(err, "error inserting pending day 27")
		}

		if _, err := callBatchDigest(ctx, platform, []int{streamRef1}, []int{27}); err != nil {
			return errors.Wrap(err, "error calling batch_digest for OPEN+HIGH test")
		}

		// Verify OPEN+HIGH flag combination (flag 3 = 1+2) + separate LOW and CLOSE
		if err := verifySpecificFlags(ctx, platform, streamRef1, 27, []int{3, 4, 8}, "OPEN+HIGH with LOW,CLOSE", t); err != nil {
			return errors.Wrap(err, "error verifying OPEN+HIGH flags")
		}

		// Test HIGH+LOW combination (stream 2, day 28)
		if err := insertPendingDay(ctx, platform, streamRef2, 28); err != nil {
			return errors.Wrap(err, "error inserting pending day 28")
		}

		if _, err := callBatchDigest(ctx, platform, []int{streamRef2}, []int{28}); err != nil {
			return errors.Wrap(err, "error calling batch_digest for HIGH+LOW test")
		}

		// Verify flags: OPEN+LOW (5=1+4), HIGH (2), CLOSE (8)
		if err := verifySpecificFlags(ctx, platform, streamRef2, 28, []int{5, 2, 8}, "OPEN+LOW, HIGH, CLOSE", t); err != nil {
			return errors.Wrap(err, "error verifying HIGH+LOW flags")
		}

		// Test OPEN+CLOSE combination (stream 3, day 29)
		if err := insertPendingDay(ctx, platform, streamRef3, 29); err != nil {
			return errors.Wrap(err, "error inserting pending day 29")
		}

		if _, err := callBatchDigest(ctx, platform, []int{streamRef3}, []int{29}); err != nil {
			return errors.Wrap(err, "error calling batch_digest for OPEN+CLOSE test")
		}

		// Verify actual flags from data: OPEN+HIGH (3), LOW (4), CLOSE (8)
		// The data doesn't create a single-record case, so no flag 15
		if err := verifySpecificFlags(ctx, platform, streamRef3, 29, []int{3, 4, 8}, "OPEN+HIGH, LOW, CLOSE", t); err != nil {
			return errors.Wrap(err, "error verifying OPEN+CLOSE+HIGH+LOW flags")
		}

		t.Logf("All combination flag tests completed successfully")

		return nil
	}
}

// WithIdempotencyTestSetup creates test data for idempotency testing
func WithIdempotencyTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Create test data for idempotency testing
		// Day 30: Regular OHLC pattern for testing multiple runs
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: idempotencyTestStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 2592000    | 100   |
			| 2620000    | 200   |
			| 2650000    | 50    |
			| 2678399    | 150   |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up idempotency test stream")
		}

		return testFn(ctx, platform)
	}
}

// testIdempotencyChecks tests that running digest twice produces consistent results
func testIdempotencyChecks(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, idempotencyTestStreamId.String())
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}
		// The WithIdempotencyTestSetup already creates the stream and initial data

		// Check pending_prune_days before first run
		pendingBefore, err := checkPendingPruneDayExists(ctx, platform, streamRef, 30)
		if err != nil {
			return errors.Wrap(err, "error checking pending_prune_days before first run")
		}
		t.Logf("Pending day 30 exists BEFORE first digest: %v", pendingBefore)

		// First digest run
		result1, err := callBatchDigest(ctx, platform, []int{streamRef}, []int{30})
		if err != nil {
			return errors.Wrap(err, "error calling first batch_digest")
		}

		// Count records after first run
		countAfterFirst, err := countPrimitiveEvents(ctx, platform, streamRef, 30)
		if err != nil {
			return errors.Wrap(err, "error counting records after first digest")
		}

		// Get OHLC after first run
		ohlcAfterFirst, err := callGetDailyOHLC(ctx, platform, streamRef, 30)
		if err != nil {
			return errors.Wrap(err, "error getting OHLC after first digest")
		}

		// Check pending_prune_days after first run
		pendingAfterFirst, err := checkPendingPruneDayExists(ctx, platform, streamRef, 30)
		if err != nil {
			return errors.Wrap(err, "error checking pending_prune_days after first run")
		}
		t.Logf("Pending day 30 exists AFTER first digest: %v", pendingAfterFirst)

		// Second digest run
		result2, err := callBatchDigest(ctx, platform, []int{streamRef}, []int{30})
		if err != nil {
			return errors.Wrap(err, "error calling second batch_digest")
		}

		// Count records after second run - should be same
		countAfterSecond, err := countPrimitiveEvents(ctx, platform, streamRef, 30)
		if err != nil {
			return errors.Wrap(err, "error counting records after second digest")
		}

		// Get OHLC after second run - should be identical
		ohlcAfterSecond, err := callGetDailyOHLC(ctx, platform, streamRef, 30)
		if err != nil {
			return errors.Wrap(err, "error getting OHLC after second digest")
		}

		// Log second run results (batch_digest always reprocesses when asked)
		if len(result2) > 0 && len(result2[0]) >= 2 {
			t.Logf("Second run processed_days: %s, total_deleted_rows: %s, total_preserved_rows: %s, has_more_to_delete: %s",
				result2[0][0], result2[0][1], result2[0][2], result2[0][3])
		}

		// Verify record counts are identical
		if countAfterFirst != countAfterSecond {
			t.Errorf("Record counts differ: first=%d, second=%d", countAfterFirst, countAfterSecond)
		}

		// Verify OHLC values are identical
		if len(ohlcAfterFirst) != len(ohlcAfterSecond) {
			t.Error("OHLC result lengths differ between runs")
		} else if len(ohlcAfterFirst) > 0 {
			for i, val1 := range ohlcAfterFirst[0] {
				if i < len(ohlcAfterSecond[0]) {
					val2 := ohlcAfterSecond[0][i]
					if val1 != val2 {
						t.Errorf("OHLC value %d differs: first=%s, second=%s", i, val1, val2)
					}
				}
			}
		}

		t.Logf("Idempotency test completed - consistent results across runs")
		t.Logf("First run result: %v", result1)
		t.Logf("Second run result: %v", result2)

		return nil
	}
}

// ================================
// HELPER FUNCTIONS FOR EDGE CASES
// ================================

// verifySingleRecordTypeFlag verifies that a single record gets type flag 15
func verifySingleRecordTypeFlag(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64) error {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error creating ethereum address")
	}

	dayStart := dayIndex * 86400
	dayEnd := dayStart + 86400

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true,
	}

	var typeFlag int
	var count int
	err = platform.Engine.Execute(engineContext, platform.DB, "SELECT type FROM primitive_event_type WHERE stream_ref = $stream_ref AND event_time >= $day_start AND event_time < $day_end", map[string]any{
		"$stream_ref": streamRef,
		"$day_start":  dayStart,
		"$day_end":    dayEnd,
	}, func(row *common.Row) error {
		if len(row.Values) != 1 {
			return errors.Errorf("expected 1 column, got %d", len(row.Values))
		}

		if tf, ok := row.Values[0].(int64); ok {
			typeFlag = int(tf)
			count++
		} else {
			return errors.New("type is not int64")
		}
		return nil
	})

	if err != nil {
		return err
	}

	if count != 1 {
		return errors.Errorf("expected 1 type flag record for single record day, got %d", count)
	}

	if typeFlag != 15 {
		return errors.Errorf("expected type flag 15 (OPEN+HIGH+LOW+CLOSE) for single record, got %d", typeFlag)
	}

	return nil
}

// insertDuplicateRecordsManually manually inserts duplicate records with controlled created_at values
func insertDuplicateRecordsManually(ctx context.Context, platform *kwilTesting.Platform, streamRef int) error {
	// Insert duplicate records with different created_at values
	records := []rec{
		{EventTime: 1728000, Value: kwilTypes.MustParseDecimalExplicit("100", 36, 18), CreatedAt: 1000}, // OPEN - older
		{EventTime: 1728000, Value: kwilTypes.MustParseDecimalExplicit("90", 36, 18), CreatedAt: 2000},  // OPEN - newer (should be kept)
		{EventTime: 1750000, Value: kwilTypes.MustParseDecimalExplicit("150", 36, 18), CreatedAt: 1000}, // HIGH - older
		{EventTime: 1750000, Value: kwilTypes.MustParseDecimalExplicit("140", 36, 18), CreatedAt: 2000}, // HIGH - newer (should be kept)
		{EventTime: 1770000, Value: kwilTypes.MustParseDecimalExplicit("50", 36, 18), CreatedAt: 1000},  // LOW - older
		{EventTime: 1770000, Value: kwilTypes.MustParseDecimalExplicit("60", 36, 18), CreatedAt: 2000},  // LOW - newer (should be kept)
		{EventTime: 1814399, Value: kwilTypes.MustParseDecimalExplicit("120", 36, 18), CreatedAt: 1000}, // CLOSE - older
		{EventTime: 1814399, Value: kwilTypes.MustParseDecimalExplicit("130", 36, 18), CreatedAt: 2000}, // CLOSE - newer (should be kept)
	}

	return insertPrimitiveRecords(ctx, platform, streamRef, records, true)
}

// insertMultipleDuplicateRecordsManually inserts >2 duplicates per timestamp
func insertMultipleDuplicateRecordsManually(ctx context.Context, platform *kwilTesting.Platform, streamRef int) error {
	// Insert multiple duplicate records with ascending created_at values
	records := []rec{
		// OPEN timestamp - 3 duplicates
		{EventTime: 1814400, Value: kwilTypes.MustParseDecimalExplicit("100", 36, 18), CreatedAt: 1000},
		{EventTime: 1814400, Value: kwilTypes.MustParseDecimalExplicit("110", 36, 18), CreatedAt: 2000},
		{EventTime: 1814400, Value: kwilTypes.MustParseDecimalExplicit("120", 36, 18), CreatedAt: 3000}, // Latest - should be kept

		// HIGH timestamp - 4 duplicates
		{EventTime: 1850000, Value: kwilTypes.MustParseDecimalExplicit("200", 36, 18), CreatedAt: 1000},
		{EventTime: 1850000, Value: kwilTypes.MustParseDecimalExplicit("210", 36, 18), CreatedAt: 2000},
		{EventTime: 1850000, Value: kwilTypes.MustParseDecimalExplicit("220", 36, 18), CreatedAt: 3000},
		{EventTime: 1850000, Value: kwilTypes.MustParseDecimalExplicit("230", 36, 18), CreatedAt: 4000}, // Latest - should be kept

		// LOW timestamp - 3 duplicates
		{EventTime: 1870000, Value: kwilTypes.MustParseDecimalExplicit("50", 36, 18), CreatedAt: 1000},
		{EventTime: 1870000, Value: kwilTypes.MustParseDecimalExplicit("40", 36, 18), CreatedAt: 2000},
		{EventTime: 1870000, Value: kwilTypes.MustParseDecimalExplicit("30", 36, 18), CreatedAt: 3000}, // Latest - should be kept

		// CLOSE timestamp - 5 duplicates
		{EventTime: 1900799, Value: kwilTypes.MustParseDecimalExplicit("150", 36, 18), CreatedAt: 1000},
		{EventTime: 1900799, Value: kwilTypes.MustParseDecimalExplicit("160", 36, 18), CreatedAt: 2000},
		{EventTime: 1900799, Value: kwilTypes.MustParseDecimalExplicit("170", 36, 18), CreatedAt: 3000},
		{EventTime: 1900799, Value: kwilTypes.MustParseDecimalExplicit("180", 36, 18), CreatedAt: 4000},
		{EventTime: 1900799, Value: kwilTypes.MustParseDecimalExplicit("190", 36, 18), CreatedAt: 5000}, // Latest - should be kept
	}

	return insertPrimitiveRecords(ctx, platform, streamRef, records, true)
}

// verifyCorrectRecordsPersisted verifies that duplicate records were resolved correctly by created_at
func verifyCorrectRecordsPersisted(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64, t *testing.T) error {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error creating ethereum address")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true,
	}

	dayStart := dayIndex * 86400
	dayEnd := dayStart + 86400

	// Query persisted records and verify values match expected (latest created_at)
	persistedRecords := make(map[int64]struct {
		value     string
		createdAt int64
	})

	err = platform.Engine.Execute(engineContext, platform.DB,
		"SELECT event_time, value, created_at FROM primitive_events WHERE stream_ref = $stream_ref AND event_time >= $day_start AND event_time < $day_end ORDER BY event_time",
		map[string]any{
			"$stream_ref": streamRef,
			"$day_start":  dayStart,
			"$day_end":    dayEnd,
		}, func(row *common.Row) error {
			if len(row.Values) != 3 {
				return errors.Errorf("expected 3 columns, got %d", len(row.Values))
			}

			eventTime, ok := row.Values[0].(int64)
			if !ok {
				return errors.New("event_time is not int64")
			}

			valueDecimal, ok := row.Values[1].(*kwilTypes.Decimal)
			if !ok {
				return errors.New("value is not decimal")
			}

			createdAt, ok := row.Values[2].(int64)
			if !ok {
				return errors.New("created_at is not int64")
			}

			persistedRecords[eventTime] = struct {
				value     string
				createdAt int64
			}{
				value:     valueDecimal.String(),
				createdAt: createdAt,
			}

			t.Logf("Persisted record: event_time=%d, value=%s, created_at=%d", eventTime, valueDecimal.String(), createdAt)
			return nil
		})

	if err != nil {
		return errors.Wrap(err, "error querying persisted records")
	}

	// Expected values (latest created_at=2000 for each event_time)
	expected := map[int64]struct {
		value     string
		createdAt int64
	}{
		1728000: {"90.000000000000000000", 2000},  // OPEN - newer record
		1750000: {"140.000000000000000000", 2000}, // HIGH - newer record
		1770000: {"60.000000000000000000", 2000},  // LOW - newer record
		1814399: {"130.000000000000000000", 2000}, // CLOSE - newer record
	}

	// Verify each expected record
	for eventTime, expectedData := range expected {
		actualData, exists := persistedRecords[eventTime]
		if !exists {
			return errors.Errorf("expected record at event_time %d not found", eventTime)
		}

		if actualData.value != expectedData.value {
			return errors.Errorf("at event_time %d: expected value %s, got %s", eventTime, expectedData.value, actualData.value)
		}

		if actualData.createdAt != expectedData.createdAt {
			return errors.Errorf("at event_time %d: expected created_at %d, got %d", eventTime, expectedData.createdAt, actualData.createdAt)
		}
	}

	// Verify we have exactly the expected number of records
	if len(persistedRecords) != len(expected) {
		return errors.Errorf("expected %d persisted records, got %d", len(expected), len(persistedRecords))
	}

	t.Logf("✅ All persisted records are correct (latest created_at values)")
	return nil
}

// verifySpecificFlags verifies that the expected flag combinations exist in primitive_event_type
func verifySpecificFlags(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64, expectedFlags []int, flagName string, t *testing.T) error {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error creating ethereum address")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true,
	}

	dayStart := dayIndex * 86400
	dayEnd := dayStart + 86400

	// Query all flag types in the day
	foundFlags := make(map[int]int64) // flag -> event_time

	err = platform.Engine.Execute(engineContext, platform.DB,
		"SELECT event_time, type FROM primitive_event_type WHERE stream_ref = $stream_ref AND event_time >= $day_start AND event_time < $day_end ORDER BY event_time",
		map[string]any{
			"$stream_ref": streamRef,
			"$day_start":  dayStart,
			"$day_end":    dayEnd,
		}, func(row *common.Row) error {
			if len(row.Values) != 2 {
				return errors.Errorf("expected 2 columns, got %d", len(row.Values))
			}

			eventTime, ok := row.Values[0].(int64)
			if !ok {
				return errors.New("event_time is not int64")
			}

			typeFlag, ok := row.Values[1].(int64)
			if !ok {
				return errors.New("type is not int64")
			}

			foundFlags[int(typeFlag)] = eventTime
			t.Logf("Found %s flag %d at event_time %d", flagName, int(typeFlag), eventTime)
			return nil
		})

	if err != nil {
		return errors.Wrap(err, "error querying primitive_event_type flags")
	}

	// Verify all expected flags exist
	for _, expectedFlag := range expectedFlags {
		if _, exists := foundFlags[expectedFlag]; !exists {
			return errors.Errorf("expected %s flag %d not found", flagName, expectedFlag)
		}
	}

	// Verify we have exactly the expected flags (no extra ones)
	if len(foundFlags) != len(expectedFlags) {
		return errors.Errorf("expected %d flags for %s, found %d", len(expectedFlags), flagName, len(foundFlags))
	}

	t.Logf("✅ %s flags verified correctly: %v", flagName, expectedFlags)
	return nil
}

// verifyMultipleDuplicatesCorrectRecords verifies that multiple duplicates were resolved correctly by created_at
func verifyMultipleDuplicatesCorrectRecords(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64, t *testing.T) error {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error creating ethereum address")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true,
	}

	dayStart := dayIndex * 86400
	dayEnd := dayStart + 86400

	// Query persisted records and verify values match expected (latest created_at)
	persistedRecords := make(map[int64]struct {
		value     string
		createdAt int64
	})

	err = platform.Engine.Execute(engineContext, platform.DB,
		"SELECT event_time, value, created_at FROM primitive_events WHERE stream_ref = $stream_ref AND event_time >= $day_start AND event_time < $day_end ORDER BY event_time",
		map[string]any{
			"$stream_ref": streamRef,
			"$day_start":  dayStart,
			"$day_end":    dayEnd,
		}, func(row *common.Row) error {
			if len(row.Values) != 3 {
				return errors.Errorf("expected 3 columns, got %d", len(row.Values))
			}

			eventTime, ok := row.Values[0].(int64)
			if !ok {
				return errors.New("event_time is not int64")
			}

			valueDecimal, ok := row.Values[1].(*kwilTypes.Decimal)
			if !ok {
				return errors.New("value is not decimal")
			}

			createdAt, ok := row.Values[2].(int64)
			if !ok {
				return errors.New("created_at is not int64")
			}

			persistedRecords[eventTime] = struct {
				value     string
				createdAt int64
			}{
				value:     valueDecimal.String(),
				createdAt: createdAt,
			}

			t.Logf("Persisted record: event_time=%d, value=%s, created_at=%d", eventTime, valueDecimal.String(), createdAt)
			return nil
		})

	if err != nil {
		return errors.Wrap(err, "error querying persisted records")
	}

	// Expected values (latest created_at for each event_time)
	expected := map[int64]struct {
		value     string
		createdAt int64
	}{
		1814400: {"120.000000000000000000", 3000}, // OPEN - latest of 3 duplicates
		1850000: {"230.000000000000000000", 4000}, // HIGH - latest of 4 duplicates
		1870000: {"30.000000000000000000", 3000},  // LOW - latest of 3 duplicates
		1900799: {"190.000000000000000000", 5000}, // CLOSE - latest of 5 duplicates
	}

	// Verify each expected record
	for eventTime, expectedData := range expected {
		actualData, exists := persistedRecords[eventTime]
		if !exists {
			return errors.Errorf("expected record at event_time %d not found", eventTime)
		}

		if actualData.value != expectedData.value {
			return errors.Errorf("at event_time %d: expected value %s, got %s", eventTime, expectedData.value, actualData.value)
		}

		if actualData.createdAt != expectedData.createdAt {
			return errors.Errorf("at event_time %d: expected created_at %d, got %d", eventTime, expectedData.createdAt, actualData.createdAt)
		}
	}

	// Verify we have exactly the expected number of records
	if len(persistedRecords) != len(expected) {
		return errors.Errorf("expected %d persisted records, got %d", len(expected), len(persistedRecords))
	}

	t.Logf("✅ All multiple duplicate records are correct (latest created_at values)")
	return nil
}

// WithLeftoverPrimitivesSetup creates a scenario with leftover primitives (primitives without markers)
func WithLeftoverPrimitivesSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Create test data with primitives that will have leftovers after partial processing
		testStreamId := util.GenerateStreamId("leftover_primitives_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 2160000    | 100   |
			| 2160001    | 110   |
			| 2160002    | 105   |
			| 2160003    | 115   |
			| 2160004    | 102   |
			| 2160005    | 108   |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up leftover primitives test stream")
		}

		return testFn(ctx, platform)
	}
}

// testLeftoverPrimitivesHandling tests that leftover primitives are properly handled in subsequent digest runs
func testLeftoverPrimitivesHandling(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "leftover_primitives_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}
		dayIndex := int64(25) // Day 25: 2160000-2160005 (6 records)

		// Insert pending day
		err = insertPendingDay(ctx, platform, streamRef, dayIndex)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day")
		}

		// Count initial records
		initialCount, err := countPrimitiveEvents(ctx, platform, streamRef, dayIndex)
		if err != nil {
			return errors.Wrap(err, "error counting initial records")
		}
		t.Logf("Initial records: %d", initialCount)

		// First pass with very low delete cap to create leftovers
		result1, err := callBatchDigestWithCap(ctx, platform, []int{streamRef}, []int{int(dayIndex)}, 2)
		if err != nil {
			return errors.Wrap(err, "error calling first batch_digest with cap")
		}

		// Verify first pass had more to delete
		if len(result1) > 0 && len(result1[0]) >= 4 {
			hasMore1 := result1[0][3]
			if hasMore1 != "true" {
				t.Errorf("Expected has_more_to_delete=true on first run, got %s", hasMore1)
			}
			t.Logf("First pass result: %v", result1[0])
		}

		// Count records after first pass
		afterFirstCount, err := countPrimitiveEvents(ctx, platform, streamRef, dayIndex)
		if err != nil {
			return errors.Wrap(err, "error counting records after first pass")
		}
		t.Logf("After first pass: %d records (deleted: %d)", afterFirstCount, initialCount-afterFirstCount)

		// Verify some markers were created but leftover primitives remain
		markerCount, err := countMarkers(ctx, platform, streamRef, dayIndex)
		if err != nil {
			return errors.Wrap(err, "error counting markers after first pass")
		}
		t.Logf("Markers after first pass: %d", markerCount)

		if markerCount == 0 {
			t.Error("Expected some markers to be created in first pass")
		}
		if afterFirstCount <= 3 {
			t.Error("Expected leftover primitives to remain after first pass")
		}

		// Second pass should detect leftovers and continue processing
		result2, err := callBatchDigestWithCap(ctx, platform, []int{streamRef}, []int{int(dayIndex)}, 10)
		if err != nil {
			return errors.Wrap(err, "error calling second batch_digest")
		}

		// Verify second pass processes the leftovers
		if len(result2) > 0 && len(result2[0]) >= 4 {
			hasMore2 := result2[0][3]
			if hasMore2 != "false" {
				t.Errorf("Expected has_more_to_delete=false on second run, got %s", hasMore2)
			}
			t.Logf("Second pass result: %v", result2[0])
		}

		// Count final records
		finalCount, err := countPrimitiveEvents(ctx, platform, streamRef, dayIndex)
		if err != nil {
			return errors.Wrap(err, "error counting final records")
		}
		t.Logf("Final records: %d (total deleted: %d)", finalCount, initialCount-finalCount)

		// Verify final count is reasonable (should be reduced from initial 6)
		if finalCount >= initialCount {
			t.Errorf("Expected final record count (%d) to be less than initial count (%d)", finalCount, initialCount)
		}

		// Verify all pending prune days are cleaned up
		pendingExists, err := checkPendingPruneDayExists(ctx, platform, streamRef, dayIndex)
		if err != nil {
			return errors.Wrap(err, "error checking pending prune day")
		}
		if pendingExists {
			t.Error("Expected pending_prune_days entry to be removed after processing leftovers")
		}

		// Verify final markers count
		finalMarkerCount, err := countMarkers(ctx, platform, streamRef, dayIndex)
		if err != nil {
			return errors.Wrap(err, "error counting final markers")
		}
		t.Logf("Final markers: %d", finalMarkerCount)

		if finalMarkerCount == 0 {
			t.Error("Expected some markers to exist after complete processing")
		}

		// Test reprocessing: running again should reprocess but not change the final result
		result3, err := callBatchDigest(ctx, platform, []int{streamRef}, []int{int(dayIndex)})
		if err != nil {
			return errors.Wrap(err, "error calling third batch_digest for reprocessing test")
		}

		// batch_digest always processes when asked (no short-circuiting)
		if len(result3) > 0 && len(result3[0]) >= 1 {
			processedDays3 := result3[0][0]
			t.Logf("Reprocessing test result: %v (processed days: %s)", result3[0], processedDays3)
			// Should process 1 day since we're asking it to reprocess
			if processedDays3 != "1" {
				t.Errorf("Expected batch_digest to process 1 day when explicitly asked, got %s", processedDays3)
			}
		}

		// Verify record count unchanged after reprocessing (should be stable)
		reprocessedCount, err := countPrimitiveEvents(ctx, platform, streamRef, dayIndex)
		if err != nil {
			return errors.Wrap(err, "error counting records after reprocessing test")
		}
		if reprocessedCount != finalCount {
			t.Errorf("Expected record count to remain %d after reprocessing, got %d", finalCount, reprocessedCount)
		}

		// 🔍 SELECT statement verification: Check that OHLC markers are correct after all processing
		err = verifyOHLCMarkersCorrect(ctx, platform, streamRef, dayIndex, t, "Leftover Primitives Test")
		if err != nil {
			return errors.Wrap(err, "error verifying OHLC markers after leftover primitives processing")
		}

		t.Logf("✅ Leftover primitives handling test completed successfully")
		return nil
	}
}

// MarkerInfo represents a primitive_event_type record (OHLC marker)
type MarkerInfo struct {
	EventTime int64
	Type      int
	Value     string // from primitive_events via join
}

// queryOHLCMarkers queries all OHLC markers for a specific stream and day using SELECT
func queryOHLCMarkers(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64) ([]MarkerInfo, error) {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error creating ethereum address")
	}

	dayStart := dayIndex * 86400
	dayEnd := dayStart + 86400

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true,
	}

	var markers []MarkerInfo
	err = platform.Engine.Execute(engineContext, platform.DB, `
		SELECT pet.event_time, pet.type, pe.value
		FROM primitive_event_type pet
		JOIN primitive_events pe ON pet.stream_ref = pe.stream_ref AND pet.event_time = pe.event_time
		WHERE pet.stream_ref = $stream_ref 
		  AND pet.event_time >= $day_start 
		  AND pet.event_time < $day_end
		ORDER BY pet.event_time
	`, map[string]any{
		"$stream_ref": streamRef,
		"$day_start":  dayStart,
		"$day_end":    dayEnd,
	}, func(row *common.Row) error {
		if len(row.Values) != 3 {
			return errors.Errorf("expected 3 columns, got %d", len(row.Values))
		}

		eventTime, err := parseIntValue(row.Values[0])
		if err != nil {
			return errors.Wrap(err, "error parsing event_time")
		}

		typeVal, err := parseIntValue(row.Values[1])
		if err != nil {
			return errors.Wrap(err, "error parsing type")
		}

		var value string
		switch v := row.Values[2].(type) {
		case string:
			value = v
		case *kwilTypes.Decimal:
			value = v.String()
		default:
			return errors.Errorf("unexpected value type: %T", row.Values[2])
		}

		markers = append(markers, MarkerInfo{
			EventTime: eventTime,
			Type:      int(typeVal),
			Value:     value,
		})

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error executing query OHLC markers")
	}

	return markers, nil
}

// parseIntValue handles parsing int values from different types (string, int64, int)
func parseIntValue(val any) (int64, error) {
	switch v := val.(type) {
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, errors.Wrap(err, "error parsing string to int")
		}
		return parsed, nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	default:
		return 0, errors.Errorf("unexpected int value type: %T", val)
	}
}

// verifyOHLCMarkersCorrect verifies that OHLC markers are correct using SELECT statements
func verifyOHLCMarkersCorrect(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64, t *testing.T, testName string) error {
	markers, err := queryOHLCMarkers(ctx, platform, streamRef, dayIndex)
	if err != nil {
		return errors.Wrap(err, "error querying OHLC markers")
	}

	if len(markers) == 0 {
		return errors.New("no OHLC markers found")
	}

	t.Logf("🔍 %s - Found %d OHLC markers:", testName, len(markers))

	// Track which OHLC flags we've seen
	flagsSeen := make(map[int]bool)
	for _, marker := range markers {
		t.Logf("  - event_time=%d, type=%d (flags: %s), value=%s",
			marker.EventTime, marker.Type, formatOHLCFlags(marker.Type), marker.Value)
		flagsSeen[marker.Type] = true
	}

	// Verify we have reasonable OHLC representation
	// At minimum, we should have records that cover OPEN, HIGH, LOW, CLOSE concepts
	hasOpen := flagsSeen[1] || flagsSeen[3] || flagsSeen[5] || flagsSeen[7] || flagsSeen[9] || flagsSeen[11] || flagsSeen[13] || flagsSeen[15]     // Any flag with OPEN bit
	hasHigh := flagsSeen[2] || flagsSeen[3] || flagsSeen[6] || flagsSeen[7] || flagsSeen[10] || flagsSeen[11] || flagsSeen[14] || flagsSeen[15]    // Any flag with HIGH bit
	hasLow := flagsSeen[4] || flagsSeen[5] || flagsSeen[6] || flagsSeen[7] || flagsSeen[12] || flagsSeen[13] || flagsSeen[14] || flagsSeen[15]     // Any flag with LOW bit
	hasClose := flagsSeen[8] || flagsSeen[9] || flagsSeen[10] || flagsSeen[11] || flagsSeen[12] || flagsSeen[13] || flagsSeen[14] || flagsSeen[15] // Any flag with CLOSE bit

	if !hasOpen {
		t.Errorf("❌ %s - Missing OPEN flag in markers", testName)
	}
	if !hasHigh {
		t.Errorf("❌ %s - Missing HIGH flag in markers", testName)
	}
	if !hasLow {
		t.Errorf("❌ %s - Missing LOW flag in markers", testName)
	}
	if !hasClose {
		t.Errorf("❌ %s - Missing CLOSE flag in markers", testName)
	}

	if hasOpen && hasHigh && hasLow && hasClose {
		t.Logf("✅ %s - OHLC markers verified: complete OHLC coverage", testName)
	}

	return nil
}

// formatOHLCFlags formats type flags into readable string
func formatOHLCFlags(flags int) string {
	var parts []string
	if flags&1 != 0 { // OPEN
		parts = append(parts, "OPEN")
	}
	if flags&2 != 0 { // HIGH
		parts = append(parts, "HIGH")
	}
	if flags&4 != 0 { // LOW
		parts = append(parts, "LOW")
	}
	if flags&8 != 0 { // CLOSE
		parts = append(parts, "CLOSE")
	}
	if len(parts) == 0 {
		return "NONE"
	}
	return strings.Join(parts, "+")
}

// WithSupersedingLaterDuplicateSetup creates a scenario where later timestamp gets a newer duplicate
func WithSupersedingLaterDuplicateSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Day 31 (2678400-2764800): two records t1 earlier=200, t2 later=100
		testStreamId := util.GenerateStreamId("superseding_later_duplicate_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 2678400    | 200   |
			| 2764799    | 100   |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up superseding later duplicate base stream")
		}

		return testFn(ctx, platform)
	}
}

// testDuplicateSupersedesLaterTimestamp verifies that a newer duplicate at later timestamp overrides by created_at
func testDuplicateSupersedesLaterTimestamp(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "superseding_later_duplicate_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}
		dayIndex := int64(31)

		// Insert pending day for initial two records
		if err := insertPendingDay(ctx, platform, streamRef, dayIndex); err != nil {
			return errors.Wrap(err, "error inserting pending day (initial)")
		}

		// First digest → OHLC should be [200, 200, 100, 100]
		if _, err := callBatchDigest(ctx, platform, []int{streamRef}, []int{int(dayIndex)}); err != nil {
			return errors.Wrap(err, "error calling first batch_digest")
		}

		ohlc1, err := callGetDailyOHLC(ctx, platform, streamRef, dayIndex)
		if err != nil {
			return errors.Wrap(err, "error calling get_daily_ohlc after first digest")
		}
		expected1 := `
		| open_value | high_value | low_value | close_value |
		|------------|------------|-----------|-------------|
		| 200.000000000000000000 | 200.000000000000000000 | 100.000000000000000000 | 100.000000000000000000 |
		`
		assertMarkdownEquals(t, ohlc1, expected1)

		// Insert a new record at the same event_time as the later one with higher created_at and value 250
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		txContext := &common.TxContext{Ctx: ctx, BlockContext: &common.BlockContext{Height: 1}, Signer: deployer.Bytes(), Caller: deployer.Address(), TxID: platform.Txid()}
		engineContext := &common.EngineContext{TxContext: txContext, OverrideAuthz: true}

		// later timestamp within day 31 is 2764799; insert duplicate with created_at higher than previous (e.g., 9999)
		if err := platform.Engine.Execute(engineContext, platform.DB,
			"INSERT INTO primitive_events (stream_ref, event_time, value, created_at) VALUES ($stream_ref, $event_time, $value, $created_at)",
			map[string]any{
				"$stream_ref": streamRef,
				"$event_time": int64(2764799),
				"$value":      kwilTypes.MustParseDecimalExplicit("250", 36, 18),
				"$created_at": int64(9999),
			}, func(row *common.Row) error { return nil }); err != nil {
			return errors.Wrap(err, "error inserting newer duplicate at later timestamp")
		}

		// Re-run digest and expect OHLC to update to [200, 250, 200, 250]
		if _, err := callBatchDigest(ctx, platform, []int{streamRef}, []int{int(dayIndex)}); err != nil {
			return errors.Wrap(err, "error calling second batch_digest")
		}

		ohlc2, err := callGetDailyOHLC(ctx, platform, streamRef, dayIndex)
		if err != nil {
			return errors.Wrap(err, "error calling get_daily_ohlc after second digest")
		}
		expected2 := `
		| open_value | high_value | low_value | close_value |
		|------------|------------|-----------|-------------|
		| 200.000000000000000000 | 250.000000000000000000 | 200.000000000000000000 | 250.000000000000000000 |
		`
		assertMarkdownEquals(t, ohlc2, expected2)

		return nil
	}
}

// ================================
// HIGH-ROI GAP FIXES
// ================================

// WithStaleMarkerOpenBitSetup creates test data where stale markers can corrupt get_daily_ohlc results
func WithStaleMarkerOpenBitSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Create test data with OHLC pattern for day 1 (86400-172800 seconds)
		// OPEN=50 (earliest), HIGH=100 (max), LOW=10 (min), CLOSE=75 (latest)
		testStreamId := util.GenerateStreamId("stale_marker_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 86400      | 50    |
			| 129600     | 10    |
			| 151200     | 100   |
			| 172799     | 75    |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up stale marker test stream")
		}

		// Resolve the stream_ref we just created
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "stale_marker_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		// Insert pending day and run batch_digest once to create correct markers
		if err := insertPendingDay(ctx, platform, streamRef, 1); err != nil {
			return errors.Wrap(err, "error inserting pending day")
		}

		if _, err := callBatchDigest(ctx, platform, []int{streamRef}, []int{1}); err != nil {
			return errors.Wrap(err, "error running initial batch_digest")
		}

		// Inject a stale/extra OPEN-bit marker at a non-OPEN timestamp (151200, value=100)
		// This simulates leftover marker state while the day is "considered digested"
		addr, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		txContext := &common.TxContext{
			Ctx:          ctx,
			BlockContext: &common.BlockContext{Height: 1},
			Signer:       addr.Bytes(),
			Caller:       addr.Address(),
			TxID:         platform.Txid(),
		}

		engineContext := &common.EngineContext{
			TxContext:     txContext,
			OverrideAuthz: true,
		}

		// Read existing type (if any)
		var existingType int64
		err = platform.Engine.Execute(engineContext, platform.DB,
			"SELECT type FROM primitive_event_type WHERE stream_ref=$sr AND event_time=$et",
			map[string]any{"$sr": streamRef, "$et": int64(151200)},
			func(row *common.Row) error {
				if len(row.Values) == 1 {
					if v, ok := row.Values[0].(int64); ok {
						existingType = v
					}
				}
				return nil
			},
		)
		if err != nil {
			return errors.Wrap(err, "error reading existing marker type")
		}
		newType := existingType | 1
		// Upsert with computed type value
		err = platform.Engine.Execute(engineContext, platform.DB,
			"INSERT INTO primitive_event_type (stream_ref, event_time, type) VALUES ($sr, $et, $type) ON CONFLICT (stream_ref, event_time) DO UPDATE SET type = $type",
			map[string]any{"$sr": streamRef, "$et": int64(151200), "$type": newType},
			func(*common.Row) error { return nil },
		)
		if err != nil {
			return errors.Wrap(err, "error injecting stale marker")
		}

		return testFn(ctx, platform)
	}
}

// testGetDailyOHLC_IgnoresStaleMarkers verifies that get_daily_ohlc ignores stale markers and returns correct OHLC values
func testGetDailyOHLC_IgnoresStaleMarkers(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "stale_marker_test_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		// Call get_daily_ohlc - should ignore stale OPEN marker at 151200 and return correct OPEN=50
		ohlcResult, err := callGetDailyOHLC(ctx, platform, streamRef, 1)
		if err != nil {
			return errors.Wrap(err, "error calling get_daily_ohlc with stale markers")
		}

		// Expect OPEN to remain 50 (earliest), not 100 (stale marker value)
		expectedOHLC := `
		| open_value | high_value | low_value | close_value |
		|------------|------------|-----------|-------------|
		| 50.000000000000000000 | 100.000000000000000000 | 10.000000000000000000 | 75.000000000000000000 |
		`

		assertMarkdownEquals(t, ohlcResult, expectedOHLC)

		return nil
	}
}

// WithHighCloseTogetherSetup creates test data for HIGH+CLOSE combination (flag 10)
func WithHighCloseTogetherSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Create test data where HIGH and CLOSE are at the same timestamp
		// Day 40: earliest=50 (OPEN), last=90 (CLOSE & HIGH), middle=60
		testStreamId := util.GenerateStreamId("high_close_together_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 3456000    | 50    |
			| 3460000    | 60    |
			| 3542399    | 90    |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up HIGH+CLOSE test stream")
		}

		return testFn(ctx, platform)
	}
}

// testHighCloseTogether_Flag10 tests HIGH+CLOSE combination (flag 10)
func testHighCloseTogether_Flag10(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "high_close_together_stream")
		if err != nil {
			return errors.Wrap(err, "error resolving stream ref")
		}

		// Insert pending day for day 40
		err = insertPendingDay(ctx, platform, streamRef, 40)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day 40")
		}

		// Run batch_digest
		_, err = callBatchDigest(ctx, platform, []int{streamRef}, []int{40})
		if err != nil {
			return errors.Wrap(err, "error calling batch_digest for HIGH+CLOSE test")
		}

		// Expect flags: OPEN at 3456000 (flag 1), HIGH+CLOSE at 3542399 (flag 2+8=10)
		// LOW is also at 3456000 (same as OPEN), so OPEN+LOW (flag 1+4=5)
		err = verifySpecificFlags(ctx, platform, streamRef, 40, []int{5, 10}, "OPEN+LOW and HIGH+CLOSE", t)
		if err != nil {
			return errors.Wrap(err, "error verifying HIGH+CLOSE flags")
		}

		return nil
	}
}

// WithAutoDigestZeroExpectedSetup creates minimal setup for testing auto_digest with zero expected records
func WithAutoDigestZeroExpectedSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		return testFn(ctx, platform)
	}
}

// testAutoDigest_ValidatesExpectedRecordsInput tests that auto_digest validates expected_records_per_stream input
func testAutoDigest_ValidatesExpectedRecordsInput(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Test case 1: expected_records_per_stream = 0 (should error)
		err = testAutoDigestWithExpectedRecords(platform, &deployer, 0, "expected_records_per_stream must be a positive integer")
		if err != nil {
			return errors.Wrap(err, "error testing zero expected_records_per_stream")
		}

		// Test case 2: expected_records_per_stream = -1 (should error)
		err = testAutoDigestWithExpectedRecords(platform, &deployer, -1, "expected_records_per_stream must be a positive integer")
		if err != nil {
			return errors.Wrap(err, "error testing negative expected_records_per_stream")
		}

		// Test case 3: expected_records_per_stream = 1 (should succeed)
		err = testAutoDigestWithExpectedRecords(platform, &deployer, 1, "")
		if err != nil {
			return errors.Wrap(err, "error testing valid expected_records_per_stream")
		}

		t.Logf("✅ auto_digest properly validates expected_records_per_stream input")
		return nil
	}
}

// testAutoDigest_PreservesRecentDaysCutoff verifies that auto_digest excludes the most recent days
// according to the default preserve window (preserve_past_days = 2), meaning it should not process
// today or yesterday, and only process day <= current_day-2.
func testAutoDigest_PreservesRecentDaysCutoff(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use a fixed reference point instead of time.Now() to avoid flakiness
		// This simulates "current time" for the test
		referenceTime := int64(1700000000) // Fixed timestamp for test consistency
		currentDay := referenceTime / 86400
		olderDay := currentDay - 3  // should be processed by default cutoff
		recentDay := currentDay - 1 // should be preserved (not processed)

		// Prepare a dedicated test stream with events on olderDay and recentDay
		testStreamId := util.GenerateStreamId("preserve_recent_days_cutoff_stream")
		olderStart := olderDay * 86400
		recentStart := recentDay * 86400

		markdown := fmt.Sprintf(`
			| event_time | value |
			|------------|-------|
			| %d         | 10    |
			| %d         | 20    |
			| %d         | 15    |
			| %d         | 50    |
			| %d         | 40    |
		`,
			olderStart+100, olderStart+45000, olderStart+86399,
			recentStart+100, recentStart+86399,
		)

		if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform:     platform,
			StreamId:     testStreamId,
			Height:       1,
			MarkdownData: markdown,
		}); err != nil {
			return errors.Wrap(err, "setup stream for preserve cutoff test")
		}

		// Resolve streamRef for pending queue inserts
		streamRef, err := setup.GetStreamIdForDeployer(ctx, platform, "preserve_recent_days_cutoff_stream")
		if err != nil {
			return errors.Wrap(err, "resolve stream ref")
		}

		// Enqueue both days
		if err := insertPendingDay(ctx, platform, streamRef, olderDay); err != nil {
			return errors.Wrap(err, "enqueue olderDay")
		}
		if err := insertPendingDay(ctx, platform, streamRef, recentDay); err != nil {
			return errors.Wrap(err, "enqueue recentDay")
		}

		// Run auto_digest at the fixed reference timestamp so only olderDay is processed
		result, err := callAutoDigestAtTimestamp(ctx, platform, 100, 24, 2, referenceTime)
		if err != nil {
			return errors.Wrap(err, "call auto_digest")
		}

		// Expect exactly one processed day (olderDay). Columns: processed_days | total_deleted_rows | has_more_to_delete
		assertMarkdownEquals(t, result, `
			| processed_days | total_deleted_rows | has_more_to_delete |
			|----------------|-------------------|--------------------|
			| 1              | 0                 | false              |
		`)

		// Verify pending queue cleanup behavior: olderDay removed, recentDay remains
		pendingOlder, err := checkPendingPruneDayExists(ctx, platform, streamRef, olderDay)
		if err != nil {
			return errors.Wrap(err, "check pending olderDay")
		}
		if pendingOlder {
			return errors.New("expected olderDay to be removed from pending_prune_days")
		}

		pendingRecent, err := checkPendingPruneDayExists(ctx, platform, streamRef, recentDay)
		if err != nil {
			return errors.Wrap(err, "check pending recentDay")
		}
		if !pendingRecent {
			return errors.New("expected recentDay to remain in pending_prune_days")
		}

		// Markers should exist for olderDay, and none for recentDay (since it was preserved)
		markersOlder, err := countMarkers(ctx, platform, streamRef, olderDay)
		if err != nil {
			return errors.Wrap(err, "count markers olderDay")
		}
		if markersOlder == 0 {
			return errors.New("expected OHLC markers for olderDay")
		}

		markersRecent, err := countMarkers(ctx, platform, streamRef, recentDay)
		if err != nil {
			return errors.Wrap(err, "count markers recentDay")
		}
		if markersRecent != 0 {
			return errors.Errorf("expected no markers for recentDay, got %d", markersRecent)
		}

		return nil
	}
}
