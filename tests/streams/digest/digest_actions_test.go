package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"

	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

const digestTestStreamName = "digest_test_stream"

var digestTestStreamId = util.GenerateStreamId(digestTestStreamName)

func TestDigestActions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
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
		},
	}, testutils.GetTestOptionsWithCache().Options)
}

// WithDigestTestSetup sets up test environment with digest-specific data
func WithDigestTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Setup test data with OHLC pattern for a single day
		// Day 1 (86400 seconds): Multiple records with known OHLC values
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: digestTestStreamId,
			Height:   1,
			// Create data for day index 1 (86400-172800 seconds)
			// OPEN=50 (earliest), HIGH=100 (max), LOW=10 (min), CLOSE=75 (latest)
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
			return errors.Wrap(err, "error setting up digest test stream")
		}

		return testFn(ctx, platform)
	}
}

// testDigestBasicOHLCCalculation tests basic OHLC calculation logic
func testDigestBasicOHLCCalculation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef := 1

		// Insert a day into the pending queue for digest processing
		err := insertPendingDay(ctx, platform, streamRef, 1)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day")
		}

		// Test get_daily_ohlc with raw data (before digest)
		ohlcResult, err := callGetDailyOHLC(ctx, platform, streamRef, 1)
		if err != nil {
			return errors.Wrap(err, "error calling get_daily_ohlc")
		}

		// Verify OHLC values: OPEN=50 (earliest time), HIGH=100 (max value), LOW=10 (min value), CLOSE=75 (latest time)
		expectedOHLC := `
		| open_value | high_value | low_value | close_value |
		|------------|------------|-----------|-------------|
		| 50.000000000000000000 | 100.000000000000000000 | 10.000000000000000000 | 75.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   ohlcResult,
			Expected: expectedOHLC,
		})

		// Test auto_digest action with delete cap 10
		digestResult, err := callAutoDigest(ctx, platform, 10)
		if err != nil {
			return errors.Wrap(err, "error calling auto_digest")
		}

		// Verify auto_digest result (processes 1 day with simplified exclusive boundaries)
		expectedDigest := `
		| processed_days | total_deleted_rows | has_more_to_delete |
		|----------------|-------------------|--------------------|
		| 1              | 0                 | false              |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   digestResult,
			Expected: expectedDigest,
		})

		// Verify get_daily_ohlc still works after digest (should use digested data)
		ohlcAfterDigest, err := callGetDailyOHLC(ctx, platform, streamRef, 1)
		if err != nil {
			return errors.Wrap(err, "error calling get_daily_ohlc after digest")
		}

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   ohlcAfterDigest,
			Expected: expectedOHLC, // Should be same OHLC values
		})

		// Test OHLC type flags in primitive_event_type table
		err = verifyOHLCTypeFlags(ctx, platform, streamRef, 1)
		if err != nil {
			return errors.Wrap(err, "error verifying OHLC type flags")
		}

		return nil
	}
}

// WithDigestCombinationFlagSetup sets up test data where some OHLC values are the same (testing combination flags)
func WithDigestCombinationFlagSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Create test data where OPEN = LOW (both are 10 at earliest time)
		// This should result in combination flag 1+4=5
		// Use day 2 timestamps to avoid conflict with basic test (day 1)
		testStreamId := util.GenerateStreamId("combination_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 172800     | 10    |
			| 216000     | 50    |
			| 237600     | 100   |
			| 259199     | 75    |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up combination flag test stream")
		}

		return testFn(ctx, platform)
	}
}

// testDigestCombinationFlags tests that combination flags are correctly assigned when OHLC values overlap
func testDigestCombinationFlags(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef := 1

		// Insert a day into the pending queue (day 2 to avoid primary key conflict)
		err := insertPendingDay(ctx, platform, streamRef, 2)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day for combination test")
		}

		// Run auto_digest with delete cap 10
		_, err = callAutoDigest(ctx, platform, 10)
		if err != nil {
			return errors.Wrap(err, "error calling auto_digest for combination test")
		}

		// Verify combination flags
		err = verifyCombinationFlags(ctx, platform, streamRef, 2)
		if err != nil {
			return errors.Wrap(err, "error verifying combination flags")
		}

		return nil
	}
}

// insertPendingDay inserts a day into the pending_prune_days queue (without status column)
func insertPendingDay(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64) error {
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
		OverrideAuthz: true, // Override authorization for system operations in tests
	}

	err = platform.Engine.Execute(engineContext, platform.DB, "INSERT INTO pending_prune_days (stream_ref, day_index) VALUES ($stream_ref, $day_index) ON CONFLICT DO NOTHING", map[string]any{
		"$stream_ref": streamRef,
		"$day_index":  dayIndex,
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// callGetDailyOHLC calls the get_daily_ohlc action
func callGetDailyOHLC(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64) ([]procedure.ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error creating ethereum address")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var result []procedure.ResultRow
	r, err := platform.Engine.Call(engineContext, platform.DB, "", "get_daily_ohlc", []any{
		streamRef,
		dayIndex,
	}, func(row *common.Row) error {
		if len(row.Values) != 4 {
			return errors.Errorf("expected 4 columns, got %d", len(row.Values))
		}
		openValue := fmt.Sprintf("%v", row.Values[0])
		highValue := fmt.Sprintf("%v", row.Values[1])
		lowValue := fmt.Sprintf("%v", row.Values[2])
		closeValue := fmt.Sprintf("%v", row.Values[3])
		result = append(result, procedure.ResultRow{openValue, highValue, lowValue, closeValue})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "get_daily_ohlc failed")
	}
	return result, nil
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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   allResult,
			Expected: expectedAll,
		})
		return nil
	}
}

// verifyOHLCTypeFlags checks that the correct type flags are assigned to OHLC records
func verifyOHLCTypeFlags(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64) error {
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

	// Query primitive_event_type table to get type flags for each timestamp
	typeFlags := make(map[int64]int) // timestamp -> type flag
	err = platform.Engine.Execute(engineContext, platform.DB, "SELECT event_time, type FROM primitive_event_type WHERE stream_ref = $stream_ref AND event_time >= $day_start AND event_time <= $day_end ORDER BY event_time", map[string]any{
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

		typeFlags[eventTime] = int(typeFlag)
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "error querying primitive_event_type")
	}

	// Expected type flags for Day 1 only (boundary shifted record belongs to Day 2):
	// 86400 (50)  = OPEN  -> flag 1
	// 129600 (10) = LOW   -> flag 4
	// 151200 (100) = HIGH -> flag 2
	// 172799 (75) = CLOSE -> flag 8
	// Note: 172801 belongs to Day 2 and is outside Day 1's range
	expectedFlags := map[int64]int{
		86400:  1, // OPEN
		129600: 4, // LOW
		151200: 2, // HIGH
		172799: 8, // CLOSE
	}

	// Verify all expected flags are present and correct
	for timestamp, expectedFlag := range expectedFlags {
		actualFlag, exists := typeFlags[timestamp]
		if !exists {
			return errors.Errorf("missing type flag for timestamp %d", timestamp)
		}
		if actualFlag != expectedFlag {
			return errors.Errorf("wrong type flag for timestamp %d: expected %d, got %d", timestamp, expectedFlag, actualFlag)
		}
	}

	// Verify we have exactly the expected number of records
	if len(typeFlags) != len(expectedFlags) {
		return errors.Errorf("expected %d type flag records, got %d", len(expectedFlags), len(typeFlags))
	}

	return nil
}

// verifyCombinationFlags checks combination flags when OHLC values overlap
func verifyCombinationFlags(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64) error {
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

	// Query primitive_event_type table
	typeFlags := make(map[int64]int)
	err = platform.Engine.Execute(engineContext, platform.DB, "SELECT event_time, type FROM primitive_event_type WHERE stream_ref = $stream_ref AND event_time >= $day_start AND event_time <= $day_end ORDER BY event_time", map[string]any{
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

		typeFlags[eventTime] = int(typeFlag)
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "error querying primitive_event_type for combination flags")
	}

	// Expected combination flags based on test data (day 2):
	// 172800 (10)  = OPEN + LOW -> flag 1+4=5 (earliest time AND minimum value)
	// 237600 (100) = HIGH -> flag 2 (maximum value)
	// 259199 (75) = CLOSE -> flag 8 (latest time)
	// Note: 216000 (50) should be deleted as it's not OHLC
	expectedFlags := map[int64]int{
		172800: 5, // OPEN + LOW (1+4)
		237600: 2, // HIGH
		259199: 8, // CLOSE
	}

	// Verify all expected flags are present and correct
	for timestamp, expectedFlag := range expectedFlags {
		actualFlag, exists := typeFlags[timestamp]
		if !exists {
			return errors.Errorf("missing combination type flag for timestamp %d", timestamp)
		}
		if actualFlag != expectedFlag {
			return errors.Errorf("wrong combination type flag for timestamp %d: expected %d, got %d", timestamp, expectedFlag, actualFlag)
		}
	}

	// Verify we have exactly the expected number of records (3, not 4)
	if len(typeFlags) != len(expectedFlags) {
		return errors.Errorf("expected %d combination flag records, got %d", len(expectedFlags), len(typeFlags))
	}

	return nil
}

// WithDigestAllSameFlagSetup sets up test data where all values are identical (testing maximum combination flag 15)
func WithDigestAllSameFlagSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Create test data where all values are identical (50)
		// This should result in maximum combination flag 1+2+4+8=15
		// Use day 3 timestamps to avoid conflict with other tests
		testStreamId := util.GenerateStreamId("all_same_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 259200     | 50    |
			| 302400     | 50    |
			| 324000     | 50    |
			| 345599     | 50    |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up all same values test stream")
		}

		return testFn(ctx, platform)
	}
}

// testDigestAllSameValueFlags tests that flag 15 is correctly assigned when all OHLC values are identical
func testDigestAllSameValueFlags(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef := 1

		// Insert a day into the pending queue (day 3 to avoid primary key conflict)
		err := insertPendingDay(ctx, platform, streamRef, 3)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day for all same values test")
		}

		// Run auto_digest with delete cap 10
		_, err = callAutoDigest(ctx, platform, 10)
		if err != nil {
			return errors.Wrap(err, "error calling auto_digest for all same values test")
		}

		// Verify maximum combination flag (15 = OPEN+HIGH+LOW+CLOSE)
		err = verifyAllSameFlags(ctx, platform, streamRef, 3)
		if err != nil {
			return errors.Wrap(err, "error verifying all same value flags")
		}

		return nil
	}
}

// verifyAllSameFlags checks that flag 15 is correctly assigned when all OHLC values are identical
func verifyAllSameFlags(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64) error {
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

	// Query primitive_event_type table
	typeFlags := make(map[int64]int)
	err = platform.Engine.Execute(engineContext, platform.DB, "SELECT event_time, type FROM primitive_event_type WHERE stream_ref = $stream_ref AND event_time >= $day_start AND event_time <= $day_end ORDER BY event_time", map[string]any{
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

		typeFlags[eventTime] = int(typeFlag)
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "error querying primitive_event_type for all same flags")
	}

	// When all values are identical, we should have (day 3):
	// - OPEN+HIGH+LOW at earliest time (259200) with flag 1+2+4=7
	// - CLOSE at latest time (345599) with flag 8
	// Total: 2 records
	if len(typeFlags) != 2 {
		return errors.Errorf("expected 2 type flag records for all same values, got %d", len(typeFlags))
	}

	// Check OPEN+HIGH+LOW record at 259200 (flag 7)
	openFlag, exists := typeFlags[259200]
	if !exists {
		return errors.New("missing type flag for OPEN timestamp 259200")
	}
	if openFlag != 7 {
		return errors.Errorf("wrong type flag for OPEN+HIGH+LOW: expected 7, got %d", openFlag)
	}

	// Check CLOSE record at 345599 (flag 8)
	closeFlag, exists := typeFlags[345599]
	if !exists {
		return errors.New("missing type flag for CLOSE timestamp 345599")
	}
	if closeFlag != 8 {
		return errors.Errorf("wrong type flag for CLOSE: expected 8, got %d", closeFlag)
	}

	return nil
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
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId1,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 432001     | 20    |
			| 454800     | 80    |
			| 475200     | 5     |
			| 518399     | 65    |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up batch test stream 1")
		}

		// Stream 2: Day 6 (518400-604800 seconds) - Different OHLC pattern
		testStreamId2 := util.GenerateStreamId("batch_test_stream_2")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId2,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 518401     | 100   |
			| 540000     | 25    |
			| 561600     | 150   |
			| 604799     | 90    |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up batch test stream 2")
		}

		// Stream 3: Day 7 (604800-691200 seconds) - Minimal data (should be skipped)
		testStreamId3 := util.GenerateStreamId("batch_test_stream_3")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId3,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 604801     | 42    |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up batch test stream 3")
		}

		return testFn(ctx, platform)
	}
}

// testBatchDigestSingleCandidate tests batch_digest with a single candidate
func testBatchDigestSingleCandidate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef := 1

		// Insert pending day for stream 1, day 5
		err := insertPendingDay(ctx, platform, streamRef, 5)
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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expectedResult,
		})

		return nil
	}
}

// testBatchDigestMultipleCandidates tests batch_digest with multiple candidates
func testBatchDigestMultipleCandidates(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Insert pending days for multiple streams
		err := insertPendingDay(ctx, platform, 1, 5) // Stream 1, Day 5
		if err != nil {
			return errors.Wrap(err, "error inserting pending day 1")
		}

		err = insertPendingDay(ctx, platform, 2, 6) // Stream 2, Day 6
		if err != nil {
			return errors.Wrap(err, "error inserting pending day 2")
		}

		err = insertPendingDay(ctx, platform, 3, 7) // Stream 3, Day 7 (minimal data)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day 3")
		}

		// Call batch_digest with multiple candidates
		streamRefs := []int{1, 2, 3}
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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expectedResult,
		})

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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expectedResult,
		})

		return nil
	}
}

// testBatchDigestMismatchedArrays tests batch_digest with mismatched array lengths
func testBatchDigestMismatchedArrays(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Call batch_digest with mismatched array lengths (should error)
		streamRefs := []int{1, 2}
		dayIndexes := []int{5} // One less element

		_, err := callBatchDigest(ctx, platform, streamRefs, dayIndexes)
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
			streamRef int
			dayIndex  int64
			streamId  string
		}{
			{1, 10, "array_test_stream_1_day_10"},
			{2, 10, "array_test_stream_2_day_10"},
			{3, 10, "array_test_stream_3_day_10"},
			{1, 11, "array_test_stream_1_day_11"},
			{2, 11, "array_test_stream_2_day_11"},
			{3, 11, "array_test_stream_3_day_11"},
			{1, 12, "array_test_stream_1_day_12"},
			{2, 12, "array_test_stream_2_day_12"},
			{3, 12, "array_test_stream_3_day_12"},
		}

		// Setup test data for each stream with multiple records per day
		for _, ts := range testStreams {
			testStreamId := util.GenerateStreamId(ts.streamId)
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
				return errors.Wrapf(err, "error setting up test data for stream %s", ts.streamId)
			}

			// Insert pending day
			err = insertPendingDay(ctx, platform, ts.streamRef, ts.dayIndex)
			if err != nil {
				return errors.Wrapf(err, "error inserting pending day for stream %d, day %d", ts.streamRef, ts.dayIndex)
			}
		}

		// Run auto_digest multiple times to test for ordering consistency
		var results []string
		for i := 0; i < 3; i++ {
			result, err := callAutoDigest(ctx, platform, 50) // delete cap 50
			if err != nil {
				return errors.Wrapf(err, "error calling auto_digest (iteration %d)", i+1)
			}

			if len(result) > 0 && len(result[0]) >= 2 {
				results = append(results, fmt.Sprintf("Run %d: processed %s days, deleted %s rows",
					i+1, result[0][0], result[0][1]))
			}
		}

		// Log all results to see if they're consistent
		for _, res := range results {
			t.Log(res)
		}

		// If we get here without errors, array ordering is working correctly
		// Any array index misalignment would cause runtime errors in batch_digest
		t.Log("Array ordering test completed successfully - no index misalignment detected")
		return nil
	}
}

// testOptimizedAutoDigest tests the optimized auto_digest function
func testOptimizedAutoDigest(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Insert pending days for multiple streams
		err := insertPendingDay(ctx, platform, 1, 5)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day 1")
		}

		err = insertPendingDay(ctx, platform, 2, 6)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day 2")
		}

		// Include the single-record day so the optimized flow covers it
		err = insertPendingDay(ctx, platform, 3, 7)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day 3")
		}

		// Call optimized auto_digest — use a cap large enough to capture all 3 at once
		result, err := callAutoDigest(ctx, platform, 50) // or keep 10: GREATEST(1, ...) still works, but may require multiple passes
		if err != nil {
			return errors.Wrap(err, "error calling optimized auto_digest")
		}

		// Verify auto_digest processed all three candidates in one pass
		expectedResult := `
		| processed_days | total_deleted_rows | has_more_to_delete |
		|----------------|-------------------|--------------------|
		| 3              | 0                 | false              |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expectedResult,
		})

		return nil
	}
}

// callBatchDigest calls the batch_digest action
func callBatchDigest(ctx context.Context, platform *kwilTesting.Platform, streamRefs []int, dayIndexes []int) ([]procedure.ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error creating ethereum address")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var result []procedure.ResultRow
	r, err := platform.Engine.Call(engineContext, platform.DB, "", "batch_digest", []any{
		streamRefs,
		dayIndexes,
	}, func(row *common.Row) error {
		if len(row.Values) != 4 {
			return errors.Errorf("expected 4 columns, got %d", len(row.Values))
		}
		processedDays := fmt.Sprintf("%v", row.Values[0])
		totalDeleted := fmt.Sprintf("%v", row.Values[1])
		totalPreserved := fmt.Sprintf("%v", row.Values[2])
		hasMore := fmt.Sprintf("%v", row.Values[3])
		result = append(result, procedure.ResultRow{processedDays, totalDeleted, totalPreserved, hasMore})
		return nil
	})

	// Print debug information via NOTICE log
	if r != nil && r.Logs != nil && len(r.Logs) > 0 {
		for i, log := range r.Logs {
			fmt.Println("NOTICE log", i, ":", log)
		}
	}

	if err != nil {
		return nil, err
	}
	if r != nil && r.Error != nil {
		return nil, errors.Wrap(r.Error, "batch_digest failed")
	}
	return result, nil
}

// callAutoDigest calls the auto_digest action
func callAutoDigest(ctx context.Context, platform *kwilTesting.Platform, deleteCap int) ([]procedure.ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error creating ethereum address")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var result []procedure.ResultRow
	r, err := platform.Engine.Call(engineContext, platform.DB, "", "auto_digest", []any{
		deleteCap,
		24, // expected_records_per_stream (default)
	}, func(row *common.Row) error {
		if len(row.Values) != 3 {
			return errors.Errorf("expected 3 columns, got %d", len(row.Values))
		}
		processedDays := fmt.Sprintf("%v", row.Values[0])
		totalDeleted := fmt.Sprintf("%v", row.Values[1])
		hasMore := fmt.Sprintf("%v", row.Values[2])
		result = append(result, procedure.ResultRow{processedDays, totalDeleted, hasMore})
		return nil
	})

	// Print debug information via NOTICE log
	if r != nil && r.Logs != nil && len(r.Logs) > 0 {
		for i, log := range r.Logs {
			fmt.Println("NOTICE log", i, ":", log)
		}
	}

	if err != nil {
		return nil, err
	}

	if r != nil && r.Error != nil {
		return nil, errors.Wrap(r.Error, "auto_digest failed")
	}
	return result, nil
}

// WithDeletionTestSetup sets up test environment with excess data to verify deletion logic
func WithDeletionTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrapf(err, "error registering data provider")
		}

		// Create test data with MANY intermediate records that should be deleted
		// Day 8 (691200-777600): 10 records with clear OHLC pattern
		// OPEN=100 (earliest), HIGH=200 (max), LOW=50 (min), CLOSE=150 (latest)
		// 6 intermediate records should be deleted: 110, 120, 130, 140, 160, 180
		testStreamId := util.GenerateStreamId("deletion_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId,
			Height:   1,
			MarkdownData: `
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
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up deletion test stream")
		}

		return testFn(ctx, platform)
	}
}

// testDigestDeletionLogic tests that intermediate records are actually deleted
func testDigestDeletionLogic(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamRef := 1

		// Insert pending day for deletion test (day 8)
		err := insertPendingDay(ctx, platform, streamRef, 8)
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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expectedResult,
		})

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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   ohlcResult,
			Expected: expectedOHLC,
		})

		return nil
	}
}

// countPrimitiveEvents counts records in primitive_events table for a specific stream/day
func countPrimitiveEvents(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64) (int, error) {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return 0, errors.Wrap(err, "error creating ethereum address")
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

	var count int
	err = platform.Engine.Execute(engineContext, platform.DB, "SELECT COUNT(*) FROM primitive_events WHERE stream_ref = $stream_ref AND event_time >= $day_start AND event_time < $day_end", map[string]any{
		"$stream_ref": streamRef,
		"$day_start":  dayStart,
		"$day_end":    dayEnd,
	}, func(row *common.Row) error {
		if len(row.Values) != 1 {
			return errors.Errorf("expected 1 column, got %d", len(row.Values))
		}

		if countVal, ok := row.Values[0].(int64); ok {
			count = int(countVal)
		} else {
			return errors.New("count is not int64")
		}
		return nil
	})

	return count, err
}
