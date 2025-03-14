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

var primitiveStreamId = util.GenerateStreamId(primitiveStreamName)
var composedStreamId = util.GenerateStreamId(composedStreamName)

func TestQueryStream(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "query_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithQueryTestSetup(testQUERY01_InsertAndGetRecord(t)),
			WithQueryTestSetup(testQUERY06_GetRecordWithFutureDate(t)),
			WithQueryTestSetup(testQUERY02_GetIndex(t)),
			// TODO: get index change is not implemented yet
			// WithQueryTestSetup(testQUERY03_GetIndexChange(t)),
			WithQueryTestSetup(testQUERY05_GetFirstRecord(t)),
			WithQueryTestSetup(testQUERY07_DuplicateDate(t)),
			WithQueryTestSetup(testQUERY01_GetRecordWithBaseDate(t)),
			WithQueryTestSetup(testQUERY07_AdditionalInsertWillFetchLatestRecord(t)),
			WithComposedQueryTestSetup(testAGGR03_ComposedStreamWithWeights(t)),
		},
	}, testutils.GetTestOptions())
}

// WithQueryTestSetup is a helper function that sets up the test environment with a deployer and signer
func WithQueryTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")

		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Setup initial data
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

		// Run the actual test function
		return testFn(ctx, platform)
	}
}

// [QUERY01] Authorized users (owner and whitelisted wallets) can query records over a specified date range.
func testQUERY01_InsertAndGetRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get records
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 1,
			ToTime:   5,
		})

		if err != nil {
			return errors.Wrap(err, "error getting records")
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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

// [QUERY06] If a point in time is queried, but there's no available data for that point, the closest available data in the past is returned.
func testQUERY06_GetRecordWithFutureDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get records with a future date
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 6, // Future date
			ToTime:   6,
		})

		if err != nil {
			return errors.Wrap(err, "error getting records")
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 5          | 3.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

// [QUERY02] Authorized users (owner and whitelisted wallets) can query index value which is a normalized index computed from the raw data over specified date range.
func testQUERY02_GetIndex(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get index
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 1,
			ToTime:   5,
		})

		if err != nil {
			return errors.Wrap(err, "error getting index")
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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

// [QUERY03] Authorized users (owner and whitelisted wallets) can query percentage changes of an index over specified date range.
func testQUERY03_GetIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get index change
		result, err := procedure.GetIndexChange(ctx, procedure.GetIndexChangeInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 1,
			ToTime:   5,
		})

		if err != nil {
			return errors.Wrap(err, "error getting index change")
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 0.000000000000000000 |
		| 2          | 100.000000000000000000 |
		| 3          | 100.000000000000000000 |
		| 4          | 25.000000000000000000 |
		| 5          | -40.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

// [QUERY05] Authorized users can query earliest available record for a stream.
func testQUERY05_GetFirstRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get first record
		result, err := procedure.GetFirstRecord(ctx, procedure.GetFirstRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
		})

		if err != nil {
			return errors.Wrap(err, "error getting first record")
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 1.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

// [QUERY07] Only one data point per date is returned from query (the latest inserted one)
func testQUERY07_DuplicateDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Insert a record with a duplicate date
		streamLocator := types.StreamLocator{
			StreamId:     primitiveStreamId,
			DataProvider: deployer,
		}

		err = setup.ExecuteInsertRecord(ctx, platform, streamLocator, setup.InsertRecordInput{
			EventTime: 3,
			Value:     10,
		}, 3)

		if err != nil {
			return errors.Wrap(err, "error inserting record")
		}

		// Get records
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      1,
			ToTime:        5,
		})

		if err != nil {
			return errors.Wrap(err, "error getting records")
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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

// [QUERY01] Authorized users can query records over a specified date range.
func testQUERY01_GetRecordWithBaseDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Define the base_time
		baseTime := int64(3)

		// Get records with base_time
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 1,
			ToTime:   5,
			BaseTime: baseTime,
			Height:   0,
		})

		if err != nil {
			return errors.Wrap(err, "error getting index with base time")
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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

// [QUERY07] Only one data point per date is returned from query (the latest inserted one)
// This test verifies that when multiple records are inserted for the same date, only the latest one is returned.
func testQUERY07_AdditionalInsertWillFetchLatestRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Insert a record with a duplicate date
		streamLocator := types.StreamLocator{
			StreamId:     primitiveStreamId,
			DataProvider: deployer,
		}

		err = setup.ExecuteInsertRecord(ctx, platform, streamLocator, setup.InsertRecordInput{
			EventTime: 3,
			Value:     20,
		}, 3)

		if err != nil {
			return errors.Wrap(err, "error inserting record")
		}

		// Get records
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      1,
			ToTime:        5,
		})

		if err != nil {
			return errors.Wrap(err, "error getting records")
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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
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
			return errors.Wrap(err, "error getting records")
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

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})
		return nil
	}
}
