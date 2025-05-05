/*
QUERY TEST SUITE

This test file covers the query-related behaviors defined in streams_behaviors.md:

- [QUERY01] Authorized users can query records over a specified date range (testQUERY01_InsertAndGetRecord)
- [QUERY02] Authorized users can query index value (testQUERY02_GetIndex)
- [QUERY03] Authorized users can query percentage changes (testQUERY03_GetIndexChange)
- [QUERY05] Authorized users can query earliest available record (testQUERY05_GetFirstRecord)
- [QUERY06] If no data for queried date, return closest past data (testQUERY06_GetRecordWithFutureDate)
- [QUERY07] Only one data point per date returned (testDuplicateDate, testQUERY07_AdditionalInsertWillFetchLatestRecord)
- If from_time and to_time are omitted from get_record, return only the latest available record (test_GetLatestRecord)
- If from_time and to_time are omitted from get_index, return only the latest available index (test_GetLatestIndex)
- If from_time and to_time are omitted from get_index_change, return only the latest available index change (test_GetLatestIndexChange)
- If to_time is omitted from get_record, return records from from_time to the latest (test_GetRecordFromSpecificTime)
- If from_time is omitted from get_record, return records from the earliest to to_time (test_GetRecordUntilSpecificTime)
- If to_time is omitted from get_index, return index from from_time to the latest (test_GetIndexFromSpecificTime)
- If from_time is omitted from get_index, return index from the earliest to to_time (test_GetIndexUntilSpecificTime)
- If to_time is omitted from get_index_change, return change from from_time to the latest (test_GetIndexChangeFromSpecificTime)
- If from_time is omitted from get_index_change, return change from the earliest to to_time (test_GetIndexChangeUntilSpecificTime)
*/

package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pkg/errors"

	kwilTesting "github.com/kwilteam/kwil-db/testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

const primitiveStreamName = "primitive_stream_query_test"
const composedStreamName = "composed_stream_query_test"
const primitiveChildStreamName = "primitive_child_stream_query_test"

var primitiveStreamId = util.GenerateStreamId(primitiveStreamName)
var composedStreamId = util.GenerateStreamId(composedStreamName)
var primitiveChildStreamId = util.GenerateStreamId(primitiveChildStreamName)

func TestQueryStream(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "query_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithQueryTestSetup(testQUERY01_InsertAndGetRecord(t)),
			WithQueryTestSetup(testQUERY06_GetRecordWithFutureDate(t)),
			WithQueryTestSetup(testQUERY02_GetIndex(t)),
			WithQueryTestSetup(testQUERY03_GetIndexChange(t)),
			WithQueryTestSetup(testQUERY05_GetFirstRecord(t)),
			WithQueryTestSetup(testQUERY07_DuplicateDate(t)),
			WithQueryTestSetup(testQUERY01_GetRecordWithBaseDate(t)),
			WithQueryTestSetup(testQUERY07_AdditionalInsertWillFetchLatestRecord(t)),
			WithComposedQueryTestSetup(testAGGR03_ComposedStreamWithWeights(t)),
			WithQueryTestSetup(testBatchInsertAndQueryRecord(t)),
			WithQueryTestSetup(testListStreams(t)),
			WithQueryTestSetup(test_GetLatestRecord(t)),
			WithQueryTestSetup(test_GetLatestIndex(t)),
			WithQueryTestSetup(test_GetLatestIndexChange(t)),
			WithQueryTestSetup(test_GetRecordFromSpecificTime(t)),
			WithQueryTestSetup(test_GetRecordUntilSpecificTime(t)),
			WithQueryTestSetup(test_GetIndexFromSpecificTime(t)),
			WithQueryTestSetup(test_GetIndexUntilSpecificTime(t)),
			WithQueryTestSetup(test_GetIndexChangeFromSpecificTime(t)),
			WithQueryTestSetup(test_GetIndexChangeUntilSpecificTime(t)),
		},
	}, testutils.GetTestOptions())
}

// TestConfig holds the configuration for testing streams
type TestConfig struct {
	WritableStreamId util.StreamId
	ReadableStreamId util.StreamId
	Name             string
}

// WithQueryTestSetup is a helper function that sets up the test environment with a deployer and signer
func WithQueryTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Setup initial data for primitive stream
		err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: primitiveStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 1          | 1     |
			| 2          | 2     |
			| 3          | 4     |
			| 4          | 5     |
			| 5          | 3     |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up primitive stream")
		}

		// Setup a composed stream with a single child (the primitive stream)
		// This should behave exactly like the primitive stream
		// it already sets up the primitive stream too
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			Height:   1,
			MarkdownData: fmt.Sprintf(`
			| event_time | %s |
			|------------|------------------|
			| 1          | 1                |
			| 2          | 2                |
			| 3          | 4                |
			| 4          | 5                |
			| 5          | 3                |
			`,
				primitiveChildStreamName,
			),
		})
		if err != nil {
			return errors.Wrap(err, "error setting up single child composed stream")
		}

		// Run the actual test function
		return testFn(ctx, platform)
	}
}

// runTestForAllStreamTypes runs a test function against all stream types
func runTestForAllStreamTypes(t *testing.T, testName string, testFn func(ctx context.Context, platform *kwilTesting.Platform, testConfig TestConfig) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// We don't need the deployer variable for test execution in this context
		// Just iterate through the stream types and run the test

		// Test configurations for each stream type
		testConfigs := []TestConfig{
			{
				WritableStreamId: primitiveStreamId,
				ReadableStreamId: primitiveStreamId,
				Name:             "Primitive Stream",
			},
			{
				WritableStreamId: primitiveChildStreamId,
				ReadableStreamId: composedStreamId,
				Name:             "Composed Stream",
			},
		}

		// Track failures across all streams explicitly
		var errors []error
		var failedStreams []string

		// Run tests for each stream type
		for _, config := range testConfigs {
			err := testFn(ctx, platform, config)
			if err != nil {
				// Store the error and mark which stream failed
				errors = append(errors, err)
				failedStreams = append(failedStreams, fmt.Sprintf("%s (StreamId: %s)", config.Name, config.ReadableStreamId.String()))

				// Log the error to make it visible in the test output
				t.Errorf("%s test failed for %s (StreamId: %s): %v",
					testName,
					config.Name,
					config.ReadableStreamId.String(),
					err)
			}
		}

		// If any stream failed, return an error to fail the parent test
		if len(errors) > 0 {
			failedStreamsStr := fmt.Sprintf("Failed streams: %s", failedStreams)
			return fmt.Errorf("%d of %d streams failed test %s: %s",
				len(errors), len(testConfigs), testName, failedStreamsStr)
		}

		return nil
	}
}

// Helper functions for testing
// streamTestingHandler wraps testing.T to include stream information in error messages
type streamTestingHandler struct {
	*testing.T
	StreamName string
	StreamId   string
}

// errorCapturingT is a wrapper around testing.T that captures errors instead of failing the test
type errorCapturingT struct {
	*testing.T
	StreamName    string
	StreamId      string
	ErrorOccurred bool
	ErrorMessage  string
}

// Errorf captures the error message rather than failing the test immediately
func (e *errorCapturingT) Errorf(format string, args ...interface{}) {
	e.ErrorOccurred = true
	e.ErrorMessage = fmt.Sprintf(format, args...)
	// Also log the error to the main test
	e.T.Logf("[%s (StreamId: %s)] %s", e.StreamName, e.StreamId, e.ErrorMessage)
}

// Fatalf captures the error message rather than failing the test immediately
func (e *errorCapturingT) Fatalf(format string, args ...interface{}) {
	e.ErrorOccurred = true
	e.ErrorMessage = fmt.Sprintf(format, args...)
	// Also log the error to the main test
	e.T.Logf("[%s (StreamId: %s)] %s", e.StreamName, e.StreamId, e.ErrorMessage)
}

// [QUERY01] Authorized users (owner and whitelisted wallets) can query records over a specified date range.
func testQUERY01_InsertAndGetRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "QUERY01_InsertAndGetRecord", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s", config.Name)
		}

		fromTime := int64(1)
		toTime := int64(5)

		// Get records
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
		})

		if err != nil {
			return errors.Wrapf(err, "error getting records from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 1.000000000000000000 |
		| 2          | 2.000000000000000000 |
		| 3          | 4.000000000000000000 |
		| 4          | 5.000000000000000000 |
		| 5          | 3.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// [QUERY06] If a point in time is queried, but there's no available data for that point, the closest available data in the past is returned.
func testQUERY06_GetRecordWithFutureDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "QUERY06_GetRecordWithFutureDate", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s", config.Name)
		}

		// Get records with a future date
		fromTime := int64(6)
		toTime := int64(6)

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
		})

		if err != nil {
			return errors.Wrapf(err, "error getting records with future date from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 5          | 3.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// [QUERY02] Authorized users (owner and whitelisted wallets) can query index value which is a normalized index computed from the raw data over specified date range.
func testQUERY02_GetIndex(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "QUERY02_GetIndex", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		fromTime := int64(1)
		toTime := int64(5)

		// Get index
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
		})

		if err != nil {
			return errors.Wrapf(err, "error getting index from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 100.000000000000000000 |
		| 2          | 200.000000000000000000 |
		| 3          | 400.000000000000000000 |
		| 4          | 500.000000000000000000 |
		| 5          | 300.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// [QUERY03] Authorized users (owner and whitelisted wallets) can query percentage changes of an index over specified date range.
func testQUERY03_GetIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "QUERY03_GetIndexChange", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		fromTime := int64(1)
		toTime := int64(5)
		interval := int(1)
		// Get index change
		result, err := procedure.GetIndexChange(ctx, procedure.GetIndexChangeInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			Interval: &interval,
		})

		if err != nil {
			return errors.Wrapf(err, "error getting index change from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 2          | 100.000000000000000000 |
		| 3          | 100.000000000000000000 |
		| 4          | 25.000000000000000000 |
		| 5          | -40.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// [QUERY05] Authorized users can query earliest available record for a stream.
func testQUERY05_GetFirstRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "QUERY05_GetFirstRecord", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		// Get first record
		result, err := procedure.GetFirstRecord(ctx, procedure.GetFirstRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
		})

		if err != nil {
			return errors.Wrapf(err, "error getting first record from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 1.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// [QUERY07] Only one data point per date is returned from query (the latest inserted one)
func testQUERY07_DuplicateDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "QUERY07_DuplicateDate", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		fromTime := int64(1)
		toTime := int64(5)

		// Insert a record with a duplicate date - this has to be done to both streams to keep them in sync
		writableStreamLocator := types.StreamLocator{
			StreamId:     config.WritableStreamId,
			DataProvider: deployer,
		}

		err = setup.ExecuteInsertRecord(ctx, platform, writableStreamLocator, setup.InsertRecordInput{
			EventTime: 3,
			Value:     10,
		}, 3)

		if err != nil {
			return errors.Wrapf(err, "error inserting record into %s (StreamId: %s)",
				config.Name, config.WritableStreamId.String())
		}

		readableStreamLocator := types.StreamLocator{
			StreamId:     config.ReadableStreamId,
			DataProvider: deployer,
		}

		// Get records
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: readableStreamLocator,
			FromTime:      &fromTime,
			ToTime:        &toTime,
		})

		if err != nil {
			return errors.Wrapf(err, "error getting records from %s (StreamId: %s) after insert",
				config.Name, config.ReadableStreamId.String())
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 1.000000000000000000 |
		| 2          | 2.000000000000000000 |
		| 3          | 10.000000000000000000 |
		| 4          | 5.000000000000000000 |
		| 5          | 3.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// [QUERY01] Authorized users can query records over a specified date range.
func testQUERY01_GetRecordWithBaseDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "QUERY01_GetRecordWithBaseDate", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		fromTime := int64(1)
		toTime := int64(5)
		baseTime := int64(3)

		// Get records with base_time
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			BaseTime: &baseTime,
			Height:   0,
		})

		if err != nil {
			return errors.Wrapf(err, "error getting index with base time from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 25.000000000000000000 |
		| 2          | 50.000000000000000000 |
		| 3          | 100.000000000000000000 |
		| 4          | 125.000000000000000000 |
		| 5          | 75.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// [QUERY07] Only one data point per date is returned from query (the latest inserted one)
// This test verifies that when multiple records are inserted for the same date, only the latest one is returned.
func testQUERY07_AdditionalInsertWillFetchLatestRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "QUERY07_AdditionalInsertWillFetchLatestRecord", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		fromTime := int64(1)
		toTime := int64(5)

		// Insert a record with a duplicate date
		writableStreamLocator := types.StreamLocator{
			StreamId:     config.WritableStreamId,
			DataProvider: deployer,
		}

		err = setup.ExecuteInsertRecord(ctx, platform, writableStreamLocator, setup.InsertRecordInput{
			EventTime: 3,
			Value:     20,
		}, 3)

		if err != nil {
			return errors.Wrapf(err, "error inserting record into %s (StreamId: %s)",
				config.Name, config.WritableStreamId.String())
		}

		readableStreamLocator := types.StreamLocator{
			StreamId:     config.ReadableStreamId,
			DataProvider: deployer,
		}

		// Get records
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: readableStreamLocator,
			FromTime:      &fromTime,
			ToTime:        &toTime,
		})

		if err != nil {
			return errors.Wrapf(err, "error getting records from %s (StreamId: %s) after insert",
				config.Name, config.ReadableStreamId.String())
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 1.000000000000000000 |
		| 2          | 2.000000000000000000 |
		| 3          | 20.000000000000000000 |
		| 4          | 5.000000000000000000 |
		| 5          | 3.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// If from_time and to_time are omitted, return only the latest available record.
func test_GetLatestRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "GetLatestRecord", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		// Get records without specifying from_time or to_time
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			// FromTime and ToTime are omitted
		})

		if err != nil {
			return errors.Wrapf(err, "error getting latest record from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		// The setup inserts records with event_time 1, 2, 3, 4, 5. The latest should be 5.
		expected := `
		| event_time | value |
		|------------|-------|
		| 5          | 3.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// If from_time and to_time are omitted, return only the latest available index.
func test_GetLatestIndex(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "GetLatestIndex", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		// Get index without specifying from_time or to_time
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			// FromTime and ToTime are omitted
		})

		if err != nil {
			return errors.Wrapf(err, "error getting latest index from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		// The setup inserts records up to event_time 5 (value 3). First record is time 1 (value 1).
		// Latest index should be (3/1) * 100 = 300 at time 5.
		expected := `
		| event_time | value |
		|------------|-------|
		| 5          | 300.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// If from_time and to_time are omitted, return only the latest available index change.
func test_GetLatestIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "GetLatestIndexChange", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		interval := int(1) // Default interval for change calculation

		// Get index change without specifying from_time or to_time
		result, err := procedure.GetIndexChange(ctx, procedure.GetIndexChangeInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			Interval: &interval,
			// FromTime and ToTime are omitted
		})

		if err != nil {
			return errors.Wrapf(err, "error getting latest index change from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		// Latest records are time 4 (value 5) and time 5 (value 3).
		// Index at 4: (5/1)*100 = 500. Index at 5: (3/1)*100 = 300.
		// Change: ((300 - 500) / 500) * 100 = -40.
		expected := `
		| event_time | value |
		|------------|-------|
		| 5          | -40.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// If to_time is omitted, return records from from_time to the latest.
func test_GetRecordFromSpecificTime(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "GetRecordFromSpecificTime", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s", config.Name)
		}

		fromTime := int64(3) // Start from time 3

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			// ToTime is omitted
		})

		if err != nil {
			return errors.Wrapf(err, "error getting records with from_time only from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		// Should return records from time 3, 4, 5
		expected := `
		| event_time | value |
		|------------|-------|
		| 3          | 4.000000000000000000 |
		| 4          | 5.000000000000000000 |
		| 5          | 3.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// If from_time is omitted, return records from the earliest to to_time.
func test_GetRecordUntilSpecificTime(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "GetRecordUntilSpecificTime", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s", config.Name)
		}

		toTime := int64(3) // End at time 3

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			// FromTime is omitted
			ToTime: &toTime,
		})

		if err != nil {
			return errors.Wrapf(err, "error getting records with to_time only from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		// Should return records from time 1, 2, 3
		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 1.000000000000000000 |
		| 2          | 2.000000000000000000 |
		| 3          | 4.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// If to_time is omitted, return index from from_time to the latest.
func test_GetIndexFromSpecificTime(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "GetIndexFromSpecificTime", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s", config.Name)
		}

		fromTime := int64(3) // Start from time 3

		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			// ToTime is omitted
		})

		if err != nil {
			return errors.Wrapf(err, "error getting index with from_time only from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		// Base is time 1 (value 1). Index values: 3->400, 4->500, 5->300
		expected := `
		| event_time | value |
		|------------|-------|
		| 3          | 400.000000000000000000 |
		| 4          | 500.000000000000000000 |
		| 5          | 300.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// If from_time is omitted, return index from the earliest to to_time.
func test_GetIndexUntilSpecificTime(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "GetIndexUntilSpecificTime", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s", config.Name)
		}

		toTime := int64(3) // End at time 3

		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			// FromTime is omitted
			ToTime: &toTime,
		})

		if err != nil {
			return errors.Wrapf(err, "error getting index with to_time only from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		// Base is time 1 (value 1). Index values: 1->100, 2->200, 3->400
		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 100.000000000000000000 |
		| 2          | 200.000000000000000000 |
		| 3          | 400.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// If to_time is omitted, return index change from from_time to the latest.
func test_GetIndexChangeFromSpecificTime(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "GetIndexChangeFromSpecificTime", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s", config.Name)
		}

		fromTime := int64(2) // Start calculation from time 2 (change between 1 and 2)
		interval := int(1)

		result, err := procedure.GetIndexChange(ctx, procedure.GetIndexChangeInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			Interval: &interval,
			// ToTime is omitted
		})

		if err != nil {
			return errors.Wrapf(err, "error getting index change with from_time only from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		// Changes: 2 vs 1 -> 100%, 3 vs 2 -> 100%, 4 vs 3 -> 25%, 5 vs 4 -> -40%
		expected := `
		| event_time | value |
		|------------|-------|
		| 2          | 100.000000000000000000 |
		| 3          | 100.000000000000000000 |
		| 4          | 25.000000000000000000 |
		| 5          | -40.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// If from_time is omitted, return index change from the earliest to to_time.
func test_GetIndexChangeUntilSpecificTime(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return runTestForAllStreamTypes(t, "GetIndexChangeUntilSpecificTime", func(ctx context.Context, platform *kwilTesting.Platform, config TestConfig) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrapf(err, "error creating ethereum address for %s", config.Name)
		}

		toTime := int64(4) // End calculation at time 4 (change between 3 and 4)
		interval := int(1)

		result, err := procedure.GetIndexChange(ctx, procedure.GetIndexChangeInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     config.ReadableStreamId,
				DataProvider: deployer,
			},
			// FromTime is omitted
			ToTime:   &toTime,
			Interval: &interval,
		})

		if err != nil {
			return errors.Wrapf(err, "error getting index change with to_time only from %s (StreamId: %s)",
				config.Name, config.ReadableStreamId.String())
		}

		// Changes: 2 vs 1 -> 100%, 3 vs 2 -> 100%, 4 vs 3 -> 25%
		expected := `
		| event_time | value |
		|------------|-------|
		| 2          | 100.000000000000000000 |
		| 3          | 100.000000000000000000 |
		| 4          | 25.000000000000000000 |
		`

		return validateTableResult(t, result, expected, config)
	})
}

// WithComposedQueryTestSetup is a helper function that sets up the test environment with a deployer and signer
func WithComposedQueryTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Setup initial data
		err := setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | stream 1 | stream 2 | stream 3 |
			| ---------- | -------- | -------- | -------- |
			| 1          | 1        | 2        |          |
			| 2          |          |          |          |
			| 3          | 3        | 4        | 5        |
			`,
			Weights: nil,
			Height:  1,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		// Run the actual test function
		return testFn(ctx, platform)
	}
}

// [AGGR03] Taxonomies define the mapping of child streams, including a period of validity for each weight. (start_date otherwise not set)
func testAGGR03_ComposedStreamWithWeights(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Describe taxonomies
		result, err := procedure.DescribeTaxonomies(ctx, procedure.DescribeTaxonomiesInput{
			Platform:      platform,
			StreamId:      composedStreamId.String(),
			DataProvider:  deployer.Address(),
			LatestVersion: true,
		})
		if err != nil {
			return errors.Wrapf(err, "error getting taxonomies for Composed Stream (StreamId: %s)",
				composedStreamId.String())
		}

		parentStreamId := composedStreamId.String()
		childStream1 := util.GenerateStreamId("stream 1")
		childStream1Id := childStream1.String()
		childStream2 := util.GenerateStreamId("stream 2")
		childStream2Id := childStream2.String()
		childStream3 := util.GenerateStreamId("stream 3")
		childStream3Id := childStream3.String()

		// In the describe taxonomies result, the order of the child streams is ordered by created_at
		// Since the child streams are created in the same block, the order is not deterministic
		// That's why in the expected result, stream 3 is placed before stream 2 to match the actual result
		expected := fmt.Sprintf(`
		| data_provider | stream_id | child_data_provider | child_stream_id | weight | created_at | version | start_date |
		|---------------|-----------|--------------------|-----------------|--------|------------|---------|------------|
		| 0x0000000000000000000000000000000000000000 | %s | 0x0000000000000000000000000000000000000000 | %s | 1.000000000000000000 | 0 | 1 | 0 |
		| 0x0000000000000000000000000000000000000000 | %s | 0x0000000000000000000000000000000000000000 | %s | 1.000000000000000000 | 0 | 1 | 0 |
		| 0x0000000000000000000000000000000000000000 | %s | 0x0000000000000000000000000000000000000000 | %s | 1.000000000000000000 | 0 | 1 | 0 |
		`,
			parentStreamId, childStream1Id,
			parentStreamId, childStream3Id,
			parentStreamId, childStream2Id,
		)

		composedConfig := TestConfig{
			ReadableStreamId: composedStreamId,
			Name:             "Composed Stream",
		}

		if err := validateTableResult(t, result, expected, composedConfig); err != nil {
			return errors.Wrapf(err, "error validating composed stream taxonomy (StreamId: %s)",
				composedStreamId.String())
		}

		return nil
	}
}

// FormatResultRows formats the result rows into a markdown-like table body for logging.
func FormatResultRows(rows []procedure.ResultRow) string {
	if len(rows) == 0 {
		return "<empty result set>"
	}
	var builder strings.Builder
	for _, row := range rows {
		builder.WriteString("| ")
		builder.WriteString(strings.Join(row, " | "))
		builder.WriteString(" |\n")
	}
	return builder.String()
}

// validateTableResult checks if the table result matches the expected format and returns an error if it doesn't.
// If the validation fails, the error message will include the actual result.
func validateTableResult(t *testing.T, result []procedure.ResultRow, expectedMarkdown string, config TestConfig) error {
	// Create a subtest with a descriptive name to capture the results
	success := true
	var capturedError error
	var actualResultStr string

	// Run the test inside a subtest so we can capture failures
	testName := fmt.Sprintf("Validate %s (StreamId: %s)", config.Name, config.ReadableStreamId.String())
	t.Run(testName, func(t *testing.T) {
		// Create a helper that will mark the test as failed but not cause an immediate exit
		oldT := t
		defer func() { // This defer runs when the subtest t.Run finishes
			// If there was a failure, capture it
			if oldT.Failed() && success { // Check oldT.Failed() as 't' might not reflect the failure yet
				success = false
				actualResultStr = FormatResultRows(result)
				capturedError = fmt.Errorf("table validation failed for %s (StreamId: %s)\nExpected:\n%s\nActual:\n%s",
					config.Name, config.ReadableStreamId.String(), expectedMarkdown, actualResultStr)
			}
		}()

		// Use the standard assertion function with our testing.T
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expectedMarkdown,
		})
	})

	// If the test failed, return the error
	if !success {
		return capturedError
	}

	return nil
}

func testBatchInsertAndQueryRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		streamLocator := types.StreamLocator{
			StreamId:     primitiveStreamId,
			DataProvider: deployer,
		}

		// Prepare a batch of records to insert
		batchData := []setup.InsertRecordInput{
			{EventTime: 10, Value: 100},
			{EventTime: 11, Value: 110},
			{EventTime: 12, Value: 120},
		}

		primitiveStream := setup.PrimitiveStreamWithData{
			PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
				StreamLocator: streamLocator,
			},
			Data: batchData,
		}

		// Insert the batch using the new batch insertion action
		err = setup.InsertPrimitiveDataBatch(ctx, setup.InsertPrimitiveDataInput{
			Platform:        platform,
			PrimitiveStream: primitiveStream,
			Height:          2,
		})
		if err != nil {
			return errors.Wrap(err, "error in batch insertion")
		}

		// Query the inserted records and check the result
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      func() *int64 { v := int64(10); return &v }(),
			ToTime:        func() *int64 { v := int64(12); return &v }(),
		})
		if err != nil {
			return errors.Wrap(err, "error querying batch inserted records")
		}

		expected := `
        | event_time | value |
        |------------|-------|
        | 10         | 100.000000000000000000 |
        | 11         | 110.000000000000000000 |
        | 12         | 120.000000000000000000 |
        `
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})
		return nil
	}
}

func testListStreams(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dataProviderStr := fmt.Sprintf("0x%x", platform.Deployer)
		result, err := procedure.ListStreams(ctx, procedure.ListStreamsInput{
			Platform:     platform,
			Height:       0,
			DataProvider: dataProviderStr,
			Limit:        10,
			Offset:       0,
			OrderBy:      "created_at DESC",
		})
		if err != nil {
			return errors.Wrap(err, "error listing streams")
		}

		expected := fmt.Sprintf(`
		| data_provider | stream_id | stream_type | created_at |
		|---------------|-----------|-------------|------------|
		| %s | %s | primitive   | 1 |
		| %s | %s | composed    | 1 |
		| %s | %s | primitive   | 1 |
		`,
			dataProviderStr, primitiveChildStreamId.String(),
			dataProviderStr, composedStreamId.String(),
			dataProviderStr, primitiveStreamId.String(),
		)

		// Validate the result
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})
		return nil
	}
}
