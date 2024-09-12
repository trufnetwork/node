package tests

import (
	"context"
	"fmt"
	"testing"

	testutils "github.com/truflation/tsn-db/internal/contracts/tests/utils"

	"github.com/pkg/errors"
	"github.com/truflation/tsn-sdk/core/util"

	"github.com/kwilteam/kwil-db/core/utils"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/stretchr/testify/assert"
)

var (
	composedStreamName   = "complex_composed_a"
	composedStreamId     = util.GenerateStreamId(composedStreamName)
	primitiveStreamNames = []string{"p1", "p2", "p3"}
)

func TestComplexComposed(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "complex_composed_test",
		FunctionTests: []kwilTesting.TestFunc{
			WithTestSetup(testComplexComposedRecord(t)),
			WithTestSetup(testComplexComposedIndex(t)),
			WithTestSetup(testComplexComposedLatestValue(t)),
			WithTestSetup(testComplexComposedEmptyDate(t)),
			WithTestSetup(testComplexComposedIndexChange(t)),
			WithTestSetup(testComplexComposedOutOfRange(t)),
			WithTestSetup(testComplexComposedInvalidDate(t)),
		},
	})
}

func WithTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// platform.Deployer can't be the dummy value that is used by default
		deployerAddress := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		platform.Deployer = deployerAddress.Bytes()

		// Deploy the contracts here
		err := testutils.SetupComposedFromMarkdown(ctx, testutils.MarkdownComposedSetupInput{
			Platform:           platform,
			ComposedStreamName: composedStreamName,
			Height:             1,
			MarkdownData: fmt.Sprintf(`
				| date       | %s   | %s   | %s   |
				| ---------- | ---- | ---- | ---- |
				| 2021-01-01 |      |      | 3    |
				| 2021-01-02 | 4    | 5    | 6    |
				| 2021-01-03 |      |      | 9    |
				| 2021-01-04 | 10   |      |      |
				| 2021-01-05 | 13   |      | 15   |
				| 2021-01-06 |      | 17   | 18   |
				| 2021-01-07 | 19   | 20   |      |
				| 2021-01-08 |      | 23   |      |
				| 2021-01-09 | 25   |      |      |
				| 2021-01-10 |      |      | 30   |
				| 2021-01-11 |      | 32   |      |
				   | 2021-01-12 |      |      |      |
				| 2021-01-13 |      |      | 39   |
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
		composedDBID := utils.GenerateDBID(composedStreamId.String(), platform.Deployer)

		result, err := testutils.GetRecord(ctx, testutils.GetRecordOrIndexInput{
			Platform: platform,
			DBID:     composedDBID,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-13",
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedRecord")
		}

		expected := `
		| date       | value  |
		| ---------- | ------ |
		| 2021-01-01 | 3.000  |
		| 2021-01-02 | 5.333  |
		| 2021-01-03 | 6.833  |
		| 2021-01-04 | 7.833  |
		| 2021-01-05 | 11.333 |
		| 2021-01-06 | 16.833 |
		| 2021-01-07 | 18.833 |
		| 2021-01-08 | 19.833 |
		| 2021-01-09 | 20.833 |
		| 2021-01-10 | 26.833 |
		| 2021-01-11 | 29.833 |
		| 2021-01-13 | 34.333 |
		`

		testutils.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testComplexComposedIndex(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		composedDBID := utils.GenerateDBID(composedStreamId.String(), platform.Deployer)

		result, err := testutils.GetIndex(ctx, testutils.GetRecordOrIndexInput{
			Platform: platform,
			DBID:     composedDBID,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-13",
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedIndex")
		}

		expected := `
		| date       | value  |
		| ---------- | ------ |
		| 2021-01-01 | 100.000 |
		| 2021-01-02 | 150.000 |
		| 2021-01-03 | 200.000 |
		| 2021-01-04 | 225.000 |
		| 2021-01-05 | 337.500 |
		| 2021-01-06 | 467.500 |
		| 2021-01-07 | 512.500 |
		| 2021-01-08 | 532.500 |
		| 2021-01-09 | 557.500 |
		| 2021-01-10 | 757.500 |
		| 2021-01-11 | 817.500 |
		| 2021-01-13 | 967.500 |
		`

		testutils.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testComplexComposedLatestValue(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		composedDBID := utils.GenerateDBID(composedStreamId.String(), platform.Deployer)

		result, err := testutils.GetRecord(ctx, testutils.GetRecordOrIndexInput{
			Platform: platform,
			DBID:     composedDBID,
			DateFrom: "2021-01-13",
			DateTo:   "2021-01-13",
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedLatestValue")
		}

		expected := `
		| date       | value  |
		| ---------- | ------ |
		| 2021-01-13 | 34.333 |
		`

		testutils.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testComplexComposedEmptyDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		composedDBID := utils.GenerateDBID(composedStreamId.String(), platform.Deployer)

		result, err := testutils.GetRecord(ctx, testutils.GetRecordOrIndexInput{
			Platform: platform,
			DBID:     composedDBID,
			DateFrom: "2021-01-12",
			DateTo:   "2021-01-12",
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedEmptyDate")
		}

		expected := `
		| date       | value  |
		| ---------- | ------ |
		| 2021-01-11 | 29.833 |
		`

		testutils.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testComplexComposedIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		composedDBID := utils.GenerateDBID(composedStreamId.String(), platform.Deployer)

		result, err := testutils.GetIndexChange(ctx, testutils.GetIndexChangeInput{
			Platform: platform,
			DBID:     composedDBID,
			DateFrom: "2021-01-02",
			DateTo:   "2021-01-13",
			Interval: 1,
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedIndexChange")
		}

		// Expected values should be calculated based on the index changes
		expected := `
		| date       | value  |
		| ---------- | ------ |
		| 2021-01-02 | 50.000 |
		| 2021-01-03 | 33.333 |
		| 2021-01-04 | 12.500 |
		| 2021-01-05 | 50.000 |
		| 2021-01-06 | 38.519 |
		| 2021-01-07 | 9.626  |
		| 2021-01-08 | 3.902  |
		| 2021-01-09 | 4.695  |
		| 2021-01-10 | 35.874 |
		| 2021-01-11 | 7.921  |
		| 2021-01-13 | 18.349 |
		`

		testutils.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testComplexComposedOutOfRange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		composedDBID := utils.GenerateDBID(composedStreamId.String(), platform.Deployer)

		result, err := testutils.GetRecord(ctx, testutils.GetRecordOrIndexInput{
			Platform: platform,
			DBID:     composedDBID,
			DateFrom: "2020-12-31",
			DateTo:   "2021-01-14",
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedOutOfRange")
		}

		// We expect the first and last dates to be within our data range
		firstDate := result[0][0]
		lastDate := result[len(result)-1][0]

		assert.Equal(t, "2021-01-01", firstDate, "First date should be the earliest available date")
		assert.Equal(t, "2021-01-13", lastDate, "Last date should be the latest available date")

		return nil
	}
}

func testComplexComposedInvalidDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		composedDBID := utils.GenerateDBID(composedStreamId.String(), platform.Deployer)

		_, err := testutils.GetRecord(ctx, testutils.GetRecordOrIndexInput{
			Platform: platform,
			DBID:     composedDBID,
			DateFrom: "invalid-date",
			DateTo:   "2021-01-13",
			Height:   0,
		})

		assert.Error(t, err, "Expected an error for invalid date format")

		return nil
	}
}
