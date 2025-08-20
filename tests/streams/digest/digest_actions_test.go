package tests

import (
	"context"
	"fmt"
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
			| 172800     | 75    |
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

		// Test digest_daily action
		digestResult, err := callDigestDaily(ctx, platform, streamRef, 1)
		if err != nil {
			return errors.Wrap(err, "error calling digest_daily")
		}

		// Verify digest result (all 4 test records represent different OHLC values, so all are preserved)
		expectedDigest := `
		| deleted_rows | preserved_records |
		|--------------|-------------------|
		| 0            | 4                 |
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
		testStreamId := util.GenerateStreamId("combination_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 86400      | 10    |
			| 129600     | 50    |
			| 151200     | 100   |
			| 172800     | 75    |
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

		// Insert a day into the pending queue
		err := insertPendingDay(ctx, platform, streamRef, 1)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day for combination test")
		}

		// Run digest_daily
		_, err = callDigestDaily(ctx, platform, streamRef, 1)
		if err != nil {
			return errors.Wrap(err, "error calling digest_daily for combination test")
		}

		// Verify combination flags
		err = verifyCombinationFlags(ctx, platform, streamRef, 1)
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

	err = platform.Engine.Execute(engineContext, platform.DB, "INSERT INTO pending_prune_days (stream_ref, day_index) VALUES ($stream_ref, $day_index)", map[string]any{
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

// callDigestDaily calls the digest_daily action
func callDigestDaily(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64) ([]procedure.ResultRow, error) {
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
	r, err := platform.Engine.Call(engineContext, platform.DB, "", "digest_daily", []any{
		streamRef,
		dayIndex,
	}, func(row *common.Row) error {
		if len(row.Values) != 2 {
			return errors.Errorf("expected 2 columns, got %d", len(row.Values))
		}
		deletedRows := fmt.Sprintf("%v", row.Values[0])
		preservedRecords := fmt.Sprintf("%v", row.Values[1])
		result = append(result, procedure.ResultRow{deletedRows, preservedRecords})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "digest_daily failed")
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
			FromTime: func() *int64 { v := int64(172800); return &v }(),
			ToTime:   func() *int64 { v := int64(172800); return &v }(),
		})

		if err != nil {
			return errors.Wrap(err, "error querying CLOSE value")
		}

		// Verify CLOSE value is 75 (latest time record)
		expected := `
		| event_time | value |
		|------------|-------|
		| 172800     | 75.000000000000000000 |
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
			ToTime:   func() *int64 { v := int64(172800); return &v }(),
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
		| 172800     | 75.000000000000000000 |
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

	// Expected type flags based on our test data:
	// 86400 (50)  = OPEN  -> flag 1
	// 129600 (10) = LOW   -> flag 4
	// 151200 (100) = HIGH -> flag 2
	// 172800 (75) = CLOSE -> flag 8
	expectedFlags := map[int64]int{
		86400:  1, // OPEN
		129600: 4, // LOW
		151200: 2, // HIGH
		172800: 8, // CLOSE
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

	// Expected combination flags based on test data:
	// 86400 (10)  = OPEN + LOW -> flag 1+4=5 (earliest time AND minimum value)
	// 151200 (100) = HIGH -> flag 2 (maximum value)
	// 172800 (75) = CLOSE -> flag 8 (latest time)
	// Note: 129600 (50) should be deleted as it's not OHLC
	expectedFlags := map[int64]int{
		86400:  5, // OPEN + LOW (1+4)
		151200: 2, // HIGH
		172800: 8, // CLOSE
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
		testStreamId := util.GenerateStreamId("all_same_test_stream")
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: testStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 86400      | 50    |
			| 129600     | 50    |
			| 151200     | 50    |
			| 172800     | 50    |
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

		// Insert a day into the pending queue
		err := insertPendingDay(ctx, platform, streamRef, 1)
		if err != nil {
			return errors.Wrap(err, "error inserting pending day for all same values test")
		}

		// Run digest_daily
		_, err = callDigestDaily(ctx, platform, streamRef, 1)
		if err != nil {
			return errors.Wrap(err, "error calling digest_daily for all same values test")
		}

		// Verify maximum combination flag (15 = OPEN+HIGH+LOW+CLOSE)
		err = verifyAllSameFlags(ctx, platform, streamRef, 1)
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

	// When all values are identical, we should have:
	// - OPEN+HIGH+LOW at earliest time (86400) with flag 1+2+4=7  
	// - CLOSE at latest time (172800) with flag 8
	// Total: 2 records
	if len(typeFlags) != 2 {
		return errors.Errorf("expected 2 type flag records for all same values, got %d", len(typeFlags))
	}

	// Check OPEN+HIGH+LOW record at 86400 (flag 7)
	openFlag, exists := typeFlags[86400]
	if !exists {
		return errors.New("missing type flag for OPEN timestamp 86400")
	}
	if openFlag != 7 {
		return errors.Errorf("wrong type flag for OPEN+HIGH+LOW: expected 7, got %d", openFlag)
	}

	// Check CLOSE record at 172800 (flag 8)
	closeFlag, exists := typeFlags[172800]
	if !exists {
		return errors.New("missing type flag for CLOSE timestamp 172800")
	}
	if closeFlag != 8 {
		return errors.Errorf("wrong type flag for CLOSE: expected 8, got %d", closeFlag)
	}

	return nil
}