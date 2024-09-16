package tests

import (
	"context"
	"testing"

	"github.com/truflation/tsn-db/internal/contracts/tests/utils/procedure"
	"github.com/truflation/tsn-db/internal/contracts/tests/utils/setup"
	"github.com/truflation/tsn-db/internal/contracts/tests/utils/table"

	"github.com/truflation/tsn-sdk/core/util"

	"github.com/pkg/errors"

	"github.com/kwilteam/kwil-db/core/utils"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
)

const primitiveStreamName = "primitive_stream_000000000000001"

var primitiveStreamId = util.GenerateStreamId(primitiveStreamName)

func TestPrimitiveStream(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "primitive_test",
		FunctionTests: []kwilTesting.TestFunc{
			WithPrimitiveTestSetup(testInsertAndGetRecord(t)),
			WithPrimitiveTestSetup(testGetIndex(t)),
			WithPrimitiveTestSetup(testGetIndexChange(t)),
			WithPrimitiveTestSetup(testDuplicateDate(t)),
			WithPrimitiveTestSetup(testGetRecordWithBaseDate(t)),
		},
	})
}

func WithPrimitiveTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup initial data
		err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform:            platform,
			PrimitiveStreamName: primitiveStreamName,
			Height:              1,
			MarkdownData: `
			| date       | value |
			|------------|-------|
			| 2021-01-01 | 1     |
			| 2021-01-02 | 2     |
			| 2021-01-03 | 4     |
			| 2021-01-04 | 5     |
			| 2021-01-05 | 3     |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up primitive stream")
		}

		// Run the actual test function
		return testFn(ctx, platform)
	}
}

func testInsertAndGetRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		// Get records
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			DBID:     dbid,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-05",
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records")
		}

		expected := `
		| date       | value |
		|------------|-------|
		| 2021-01-01 | 1.000 |
		| 2021-01-02 | 2.000 |
		| 2021-01-03 | 4.000 |
		| 2021-01-04 | 5.000 |
		| 2021-01-05 | 3.000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testGetIndex(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			DBID:     dbid,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-05",
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting index")
		}

		expected := `
		| date       | value  |
		|------------|--------|
		| 2021-01-01 | 100.000 |
		| 2021-01-02 | 200.000 |
		| 2021-01-03 | 400.000 |
		| 2021-01-04 | 500.000 |
		| 2021-01-05 | 300.000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testGetIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		result, err := procedure.GetIndexChange(ctx, procedure.GetIndexChangeInput{
			Platform: platform,
			DBID:     dbid,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-05",
			Interval: 1,
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting index change")
		}

		expected := `
		| date       | value  |
		|------------|--------|
		| 2021-01-02 | 100.000 |
		| 2021-01-03 | 100.000 |
		| 2021-01-04 | 25.000  |
		| 2021-01-05 | -40.000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testDuplicateDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		// insert a duplicate date
		err := setup.InsertMarkdownPrimitiveData(ctx, setup.InsertMarkdownDataInput{
			Platform:            platform,
			Height:              2, // later height
			PrimitiveStreamName: primitiveStreamName,
			MarkdownData: `
			| date       | value |
			|------------|-------|
			| 2021-01-01 | 9.000 |
			`,
		})

		if err != nil {
			return errors.Wrap(err, "error inserting duplicate date")
		}

		expected := `
		| date       | value |
		|------------|-------|
		| 2021-01-01 | 9.000 |
		| 2021-01-02 | 2.000 |
		| 2021-01-03 | 4.000 |
		| 2021-01-04 | 5.000 |
		| 2021-01-05 | 3.000 |
		`

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			DBID:     dbid,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-05",
		})

		if err != nil {
			return errors.Wrap(err, "error getting records")
		}

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testGetRecordWithBaseDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		// Define the base_date
		baseDate := "2021-01-03"

		// Get records with base_date
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			DBID:     dbid,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-05",
			BaseDate: baseDate,
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records with base_date")
		}

		expected := `
		| date       | value |
		|------------|-------|
		| 2021-01-01 | 25.000 |
		| 2021-01-02 | 50.000 |
		| 2021-01-03 | 100.000 | # this is the base date
		| 2021-01-04 | 125.000 |
		| 2021-01-05 | 75.000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}
