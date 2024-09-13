package tests

import (
	"context"
	"testing"

	"github.com/truflation/tsn-sdk/core/util"

	"github.com/pkg/errors"
	testutils "github.com/truflation/tsn-db/internal/contracts/tests/utils"

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
		},
	})
}

func WithPrimitiveTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup initial data
		err := testutils.SetupPrimitiveFromMarkdown(ctx, testutils.MarkdownPrimitiveSetupInput{
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
		result, err := testutils.GetRecord(ctx, testutils.GetRecordOrIndexInput{
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

		testutils.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testGetIndex(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		result, err := testutils.GetIndex(ctx, testutils.GetRecordOrIndexInput{
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

		testutils.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testGetIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		result, err := testutils.GetIndexChange(ctx, testutils.GetIndexChangeInput{
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

		testutils.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}
