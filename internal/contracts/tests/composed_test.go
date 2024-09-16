package tests

import (
	"context"
	"testing"
	"github.com/truflation/tsn-db/internal/contracts/tests/utils/procedure"
	"github.com/truflation/tsn-db/internal/contracts/tests/utils/setup"
	"github.com/truflation/tsn-db/internal/contracts/tests/utils/table"

	"github.com/truflation/tsn-sdk/core/util"

	"github.com/kwilteam/kwil-db/core/utils"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
)

func TestComposed(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "composed_test",
		FunctionTests: []kwilTesting.TestFunc{
			WithComposedTestSetup(testComposedLastAvailable(t)),
			WithComposedTestSetup(testComposedNoPastData(t)),
		},
	})
}

func WithComposedTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// we just need to define a valid address here, as we don't need to deploy anything
		deployerAddress := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		platform.Deployer = deployerAddress.Bytes()

		// Run the actual test function
		return testFn(ctx, platform)
	}
}

func testComposedLastAvailable(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		composedDBID := utils.GenerateDBID(composedStreamId.String(), platform.Deployer)

		// Setup data for the test
		err := setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform:           platform,
			ComposedStreamName: composedStreamName,
			Height:             1,
			MarkdownData: `
				| date       | Stream 1 | Stream 2 | Stream 3 |
				| ---------- | --------- | --------- | --------- |
				| 2024-08-29 | 1         |           | 4         |
				| 2024-08-30 |           |           |           |
				| 2024-08-31 |           | 2         | 5         |
				| 2024-09-01 |           |           | 3         |
			`,
			Weights: []string{"1", "2", "3"},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up last available test data")
		}

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			DBID:     composedDBID,
			DateFrom: "2024-08-29",
			DateTo:   "2024-09-01",
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedLastAvailable")
		}

		expected := `
		| date       | value  |
		| ---------- | ------ |
		| 2024-08-29 | 3.250  | # 1 & 4
		| 2024-08-30 | 		  |
		| 2024-08-31 | 3.333  | # 1 & 2 & 5
		| 2024-09-01 | 2.333  | # 1 & 2 & 3
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testComposedNoPastData(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		composedDBID := utils.GenerateDBID(composedStreamId.String(), platform.Deployer)

		// Setup data for the test
		err := setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform:           platform,
			ComposedStreamName: composedStreamName,
			Height:             1,
			MarkdownData: `
				| date       | Stream 1 | Stream 2 | Stream 3 |
				| ---------- | --------- | --------- | --------- |
				| 2024-08-30 | 1         |           |           |
				| 2024-08-31 |           | 2         |           |
				| 2024-09-01 |           |           | 3         |
			`,
			Weights: []string{"1", "2", "3"},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up no past data test")
		}

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			DBID:     composedDBID,
			DateFrom: "2024-08-30",
			DateTo:   "2024-09-01",
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedNoPastData")
		}

		expected := `
		| date       | value  |
		| ---------- | ------ |
		| 2024-08-30 | 1.000  | # 1
		| 2024-08-31 | 1.667  | # 1 & 2
		| 2024-09-01 | 2.333  | # 1 & 2 & 3
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}
