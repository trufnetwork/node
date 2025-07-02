package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"

	"github.com/pkg/errors"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"

	"github.com/stretchr/testify/assert"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

var (
	primitiveStreamNames    = []string{"p1", "p2", "p3"}
	complexComposedDeployer = util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
)

func TestComplexComposed(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "complex_composed_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithTestSetup(testComplexComposedRecord(t)),
			WithTestSetup(testComplexComposedIndex(t)),
			WithTestSetup(testComplexComposedLatestValue(t)),
			WithTestSetup(testComplexComposedEmptyDate(t)),
			WithTestSetup(testComplexComposedIndexChange(t)),
			WithTestSetup(testComplexComposedFirstRecord(t)),
			WithTestSetup(testComplexComposedOutOfRange(t)),
			WithTestSetup(testComplexComposedIndexLatestValueConsistency(t)),
			WithTestSetup(testComposedRecordNoDuplicates(t)),
		},
	}, testutils.GetTestOptions())
}

func WithTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set the platform signer
		platform = procedure.WithSigner(platform, complexComposedDeployer.Bytes())

		// Deploy the contracts here
		err := setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			Height:   1,
			MarkdownData: fmt.Sprintf(`
				| event_time | %s   | %s   | %s   |
				| ---------- | ---- | ---- | ---- |
				| 1          |      |      | 3    |
				| 2          | 4    | 5    | 6    |
				| 3          |      |      | 9    |
				| 4          | 10   |      |      |
				| 5          | 13   |      | 15   |
				| 6          |      | 17   | 18   |
				| 7          | 19   | 20   |      |
				| 8          |      | 23   |      |
				| 9          | 25   |      |      |
				| 10         |      |      | 30   |
				| 11         |      | 32   |      |
				| 12         |      |      |      |
				| 13         |      |      | 39   |
				`,
				primitiveStreamNames[0],
				primitiveStreamNames[1],
				primitiveStreamNames[2],
			),
			Weights: []string{"1", "2", "3"},
		})
		if err != nil {
			return errors.Wrap(err, "error deploying contracts")
		}

		// Run the actual test function
		return testFn(ctx, platform)
	}
}

func testComplexComposedRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: complexComposedDeployer,
		}

		dateFrom := int64(1)
		dateTo := int64(13)

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedRecord")
		}

		expected := `
		| event_time | value  |
		| ---------- | ------ |
		| 1          | 3.000000000000000000  |
		| 2          | 5.333333333333333333  |
		| 3          | 6.833333333333333333  |
		| 4          | 7.833333333333333333  |
		| 5          | 11.333333333333333333 |
		| 6          | 16.833333333333333333 |
		| 7          | 18.833333333333333333 |
		| 8          | 19.833333333333333333 |
		| 9          | 20.833333333333333333 |
		| 10         | 26.833333333333333333 |
		| 11         | 29.833333333333333333 |
		| 13         | 34.333333333333333333 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

func testComplexComposedIndex(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: complexComposedDeployer,
		}

		dateFrom := int64(1)
		dateTo := int64(13)

		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedIndex")
		}

		expected := `
		| event_time | value  |
		| ---------- | ------ |
		| 1          | 100.000000000000000000 |
		| 2          | 150.000000000000000000 |
		| 3          | 200.000000000000000000 |
		| 4          | 225.000000000000000000 |
		| 5          | 337.500000000000000000 |
		| 6          | 467.500000000000000000 |
		| 7          | 512.500000000000000000 |
		| 8          | 532.500000000000000000 |
		| 9          | 557.500000000000000000 |
		| 10         | 757.500000000000000000 |
		| 11         | 817.500000000000000000 |
		| 13         | 967.500000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

func testComplexComposedLatestValue(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: complexComposedDeployer,
		}

		dateFrom := int64(13)
		dateTo := int64(13)

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedLatestValue")
		}

		expected := `
		| event_time | value  |
		| ---------- | ------ |
		| 13         | 34.333333333333333333 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

func testComplexComposedEmptyDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: complexComposedDeployer,
		}

		dateFrom := int64(12)
		dateTo := int64(12)

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedEmptyDate")
		}

		expected := `
		| event_time | value  |
		| ---------- | ------ |
		| 11         | 29.833333333333333333 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

func testComplexComposedIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: complexComposedDeployer,
		}

		dateFrom := int64(2)
		dateTo := int64(13)
		interval := 1

		result, err := procedure.GetIndexChange(ctx, procedure.GetIndexChangeInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Interval:      &interval,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedIndexChange")
		}

		// Expected values should be calculated based on the index changes
		expected := `
		| event_time | value  |
		| ---------- | ------ |
		| 2          | 50.000000000000000000 |
		| 3          | 33.333333333333333333 |
		| 4          | 12.500000000000000000 |
		| 5          | 50.000000000000000000 |
		| 6          | 38.518518518518518519 |
		| 7          |  9.625668449197860963 |
		| 8          | 3.902439024390243902  |
		| 9          | 4.694835680751173709  |
		| 10         | 35.874439461883408072 |
		| 11         | 7.920792079207920792  |
		| 13         | 18.348623853211009174 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

// testComplexComposedFirstRecord tests that the first record is returned correctly
// it tests on some situations:
// - no after date is provided
// - an after date is provided having partial data on it (some children having data, others not)
// - an after date after the last record is provided
func testComplexComposedFirstRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: complexComposedDeployer,
		}

		// no after date is provided
		result, err := procedure.GetFirstRecord(ctx, procedure.GetFirstRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			AfterTime:     nil,
			Height:        0,
		})
		assert.NoError(t, err, "Expected no error for valid date")

		expected := `
		| event_time | value  |
		| ---------- | ------ |
		| 1          | 3.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		// an after date is provided having partial data on it (some children having data, others not)
		afterDate := int64(5)
		result, err = procedure.GetFirstRecord(ctx, procedure.GetFirstRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			AfterTime:     &afterDate,
			Height:        0,
		})
		assert.NoError(t, err, "Expected no error for valid date")

		expected = `
		| event_time | value  |
		| ---------- | ------ |
		| 5          | 11.333333333333333333 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		// date after the last record is provided
		afterDate = int64(14)
		result, err = procedure.GetFirstRecord(ctx, procedure.GetFirstRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			AfterTime:     &afterDate,
			Height:        0,
		})
		assert.NoError(t, err, "Expected no error for valid date")

		expected = `
		| event_time | value  |
		| ---------- | ------ |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

// testComplexComposedIndexLatestValueConsistency tests that the latest value is consistent
// it's a regression test for https://github.com/trufnetwork/node/issues/938
func testComplexComposedIndexLatestValueConsistency(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId, // Assuming composedStreamId is accessible
			DataProvider: complexComposedDeployer,
		}
		latestEventTime := int64(13) // Max event_time in seed data

		// Scenario 1: Call get_index with from=nil, to=nil (SQL Path A - special latest)
		resultLatestOnly, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      nil,
			ToTime:        nil,
			// BaseTime, FrozenAt, Height are implicitly nil/0 for this test's purpose
		})
		if !assert.NoError(t, err, "GetIndex (latest only) should not return an error") {
			return errors.Wrap(err, "error in GetIndex (latest only)")
		}

		if !assert.Equal(t, 1, len(resultLatestOnly), "Expected 1 row for latest only path") {
			return errors.New("assertion failed: resultLatestOnly row count")
		}
		if !assert.Equal(t, fmt.Sprintf("%d", latestEventTime), resultLatestOnly[0][0], "Expected event_time to be latest for latest only path") {
			return errors.New("assertion failed: resultLatestOnly event_time")
		}
		valueFromLatestOnlyPath := resultLatestOnly[0][1]

		// Scenario 2: Call get_index with from=latestEventTime, to=latestEventTime (SQL Path B - ranged)
		resultLatestRanged, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &latestEventTime,
			ToTime:        &latestEventTime,
			// BaseTime, FrozenAt, Height are implicitly nil/0 for this test's purpose
		})
		if !assert.NoError(t, err, "GetIndex (latest ranged) should not return an error") {
			return errors.Wrap(err, "error in GetIndex (latest ranged)")
		}

		if !assert.Equal(t, 1, len(resultLatestRanged), "Expected 1 row for latest ranged path") {
			return errors.New("assertion failed: resultLatestRanged row count")
		}
		if !assert.Equal(t, fmt.Sprintf("%d", latestEventTime), resultLatestRanged[0][0], "Expected event_time to be latest for latest ranged path") {
			return errors.New("assertion failed: resultLatestRanged event_time")
		}
		valueFromLatestRangedPath := resultLatestRanged[0][1]

		// Verify that the ranged path gives the known correct value for event_time 13 from testComplexComposedIndex
		expectedValueFromRangedPath := "967.500000000000000000" // This is the value for event_time 13 in testComplexComposedIndex
		assert.Equal(t, expectedValueFromRangedPath, valueFromLatestRangedPath,
			"Value from 'ranged' path for event_time %d (%s) does not match expected value (%s) from full range test",
			latestEventTime, valueFromLatestRangedPath, expectedValueFromRangedPath)

		assert.Equal(t, valueFromLatestOnlyPath, valueFromLatestRangedPath,
			"Values for latest event_time (%d) should differ. 'Latest only' path gave '%s', 'ranged' path gave '%s'. This demonstrates the inconsistency.",
			latestEventTime, valueFromLatestOnlyPath, valueFromLatestRangedPath)

		return nil
	}
}

func testComplexComposedOutOfRange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: complexComposedDeployer,
		}

		dateFrom := int64(0) // Before first record
		dateTo := int64(14)  // After last record

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedOutOfRange")
		}

		// expect the correct number of rows (one of them is empty)
		assert.Equal(t, 12, len(result), "Expected 13 rows")

		// We expect the first and last dates to be within our data range
		firstDate := result[0][0]
		lastDate := result[len(result)-1][0]

		assert.Equal(t, "1", firstDate, "First date should be the earliest available date")
		assert.Equal(t, "13", lastDate, "Last date should be the latest available date")

		return nil
	}
}

// testComposedRecordNoDuplicates ensures that GetRecord and GetIndex do not return duplicate entries for the same event_time and value
func testComposedRecordNoDuplicates(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create unique stream IDs and deployer for this specific test
		localComposedStreamId := util.GenerateStreamId("local_composed_dedup")
		localPrimitiveStreamId1 := util.GenerateStreamId("local_primitive_dedup1")
		localPrimitiveStreamId2 := util.GenerateStreamId("local_primitive_dedup2")
		localPrimitiveStreamId3 := util.GenerateStreamId("local_primitive_dedup3")
		localDeployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000DED")

		platform = procedure.WithSigner(platform, localDeployer.Bytes())

		// Setup a composed stream with three primitive children.
		// Data is staggered and designed to test LOCF and the final DISTINCT on the composed result.
		err := setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: localComposedStreamId,
			Height:   1,
			MarkdownData: fmt.Sprintf(`
				| event_time | %s   | %s   | %s   |
				| ---------- | ---- | ---- | ---- |
				| 10         | 100  | 200  | 300  |
				| 11         | 101  | 200  | 300  |
				| 12         | 101  | 201  | 300  |
				| 13         | 101  | 201  | 301  |
				| 15         | 150  | 250  | 350  |
				| 16         | 151  | 250  | 350  |
				| 17         | 151  | 251  | 350  |
				| 18         | 151  | 251  | 351  |
			`, localPrimitiveStreamId1.String(), localPrimitiveStreamId2.String(), localPrimitiveStreamId3.String()),
			Weights: []string{"1", "1", "1"}, // Equal weights for simplicity in calculation
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream for dedup test")
		}

		composedStreamLocator := types.StreamLocator{
			StreamId:     localComposedStreamId,
			DataProvider: localDeployer,
		}

		// Query a range that includes these events.
		dateFrom := int64(9) // Start before the first record to ensure anchor/LOCF is considered
		dateTo := int64(19)  // End after the last record

		// Test GetRecord
		recordResult, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error in GetRecord for dedup test")
		}

		// Expected GetRecord results (weighted average with LOCF for missing points, weights are 1)
		// 10: (100+200+300)/3 = 200
		// 11: (101+200+300)/3 = 601/3 = 200.333333333333333333
		// 12: (101+201+300)/3 = 602/3 = 200.666666666666666667
		// 13: (101+201+301)/3 = 603/3 = 201
		// 15: (150+250+350)/3 = 250
		// 16: (151+250+350)/3 = 751/3 = 250.333333333333333333
		// 17: (151+251+350)/3 = 752/3 = 250.666666666666666667
		// 18: (151+251+351)/3 = 753/3 = 251
		expectedRecord := `
		| event_time | value                  |
		| ---------- | ---------------------- |
		| 10         | 200.000000000000000000 |
		| 11         | 200.333333333333333333 |
		| 12         | 200.666666666666666667 |
		| 13         | 201.000000000000000000 |
		| 15         | 250.000000000000000000 |
		| 16         | 250.333333333333333333 |
		| 17         | 250.666666666666666667 |
		| 18         | 251.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   recordResult,
			Expected: expectedRecord,
		})

		// Also test GetIndex to ensure consistency
		// Base value for index is the composed value at time 10: 200
		indexResult, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
			BaseTime:      func() *int64 { bt := int64(10); return &bt }(), // Explicitly set base_time
		})
		if err != nil {
			return errors.Wrap(err, "error in GetIndex for dedup test")
		}

		// Correct Expected GetIndex values: (Sum of (primitive_value / primitive_base_value) * 100) / 3
		// Time 10: P1=100(100%), P2=200(100%), P3=300(100%). Avg = (100+100+100)/3 = 100
		// Time 11: P1=101(101%), P2=200(100%), P3=300(100%). Avg = (101+100+100)/3 = 301/3 = 100.333333333333333333
		// Time 12: P1=101(101%), P2=201(100.5%), P3=300(100%). Avg = (101+100.5+100)/3 = 301.5/3 = 100.5
		// Time 13: P1=101(101%), P2=201(100.5%), P3=301(100.333..%). Avg = (101+100.5+100.333333333333333333)/3 = 301.833333333333333333/3 = 100.611111111111111111
		// Time 15: P1=150(150%), P2=250(125%), P3=350(116.666..%). Avg = (150+125+116.666666666666666667)/3 = 391.666666666666666667/3 = 130.555555555555555555
		// Time 16: P1=151(151%), P2=250(125%), P3=350(116.666..%). Avg = (151+125+116.666666666666666667)/3 = 392.666666666666666667/3 = 130.888888888888888889
		// Time 17: P1=151(151%), P2=251(125.5%), P3=350(116.666..%). Avg = (151+125.5+116.666666666666666667)/3 = 393.166666666666666667/3 = 131.055555555555555555
		// Time 18: P1=151(151%), P2=251(125.5%), P3=351(117%). Avg = (151+125.5+117)/3 = 393.5/3 = 131.166666666666666666
		expectedIndex := `
		| event_time | value                  |
		| ---------- | ---------------------- |
		| 10         | 100.000000000000000000 |
		| 11         | 100.333333333333333333 |
		| 12         | 100.500000000000000000 |
		| 13         | 100.611111111111111111 |
		| 15         | 130.555555555555555555 |
		| 16         | 130.888888888888888889 |
		| 17         | 131.055555555555555555 |
		| 18         | 131.166666666666666666 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   indexResult,
			Expected: expectedIndex,
		})

		return nil
	}
}
