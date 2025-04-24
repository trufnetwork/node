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

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/stretchr/testify/assert"
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
