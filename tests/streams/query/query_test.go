/*
QUERY TEST SUITE

This test file covers the query-related behaviors defined in streams_behaviors.md:

- [QUERY01] Authorized users can query records over a specified date range (testQUERY01_InsertAndGetRecord)
- [QUERY02] Authorized users can query index value (testQUERY02_GetIndex)
- [QUERY03] Authorized users can query percentage changes (testQUERY03_GetIndexChange)
- [QUERY05] Authorized users can query earliest available record (testQUERY05_GetFirstRecord)
- [QUERY06] If no data for queried date, return closest past data (testQUERY06_GetRecordWithFutureDate)
- [QUERY07] Only one data point per date returned (testDuplicateDate, testQUERY07_AdditionalInsertWillFetchLatestRecord)
*/

package tests

import (
	"context"
	"fmt"
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

// validateTableResult checks if the table result matches the expected format and returns an error if it doesn't
func validateTableResult(t *testing.T, result []procedure.ResultRow, expected string, config TestConfig) error {
	// Create a subtest with a descriptive name to capture the results
	success := true
	var capturedError error

	// Run the test inside a subtest so we can capture failures
	testName := fmt.Sprintf("Validate %s (StreamId: %s)", config.Name, config.ReadableStreamId.String())
	t.Run(testName, func(t *testing.T) {
		// Create a helper that will mark the test as failed but not cause an immediate exit
		oldT := t
		defer func() {
			// If there was a failure, capture it
			if oldT.Failed() && success {
				success = false
				capturedError = fmt.Errorf("table validation failed for %s (StreamId: %s)",
					config.Name, config.ReadableStreamId.String())
			}
		}()

		// Use the standard assertion function with our testing.T
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
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
