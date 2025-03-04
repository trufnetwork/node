package tests

import (
	"context"
	"testing"

	"github.com/kwilteam/kwil-db/core/utils"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/trufnetwork/node/internal/contracts/tests/utils/procedure"
	"github.com/trufnetwork/node/internal/contracts/tests/utils/setup"
	"github.com/trufnetwork/node/internal/contracts/tests/utils/table"
	"github.com/trufnetwork/sdk-go/core/util"
)

/*
	AGGR01: A composed stream aggregates data from multiple child streams (which may be either primitive or composed).

	bare minimum test:
		composed stream with 3 child streams (all primitives, to make it easy to insert data)
		each have the same weight
		we query and we get the correct avg value
		each has data in 3 days
*/

// TestAGGR01_BasicAggregation tests AGGR01: A composed stream aggregates data from multiple child streams (which may be either primitive or composed).
func TestAGGR01_BasicAggregation(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "aggr01_basic_aggregation_test",
		FunctionTests: []kwilTesting.TestFunc{
			testAGGR01_BasicAggregation(t),
		},
	})
}

func testAGGR01_BasicAggregation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create a composed stream with 3 child primitive streams
		composedStreamId := util.GenerateStreamId("composed_stream_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform.Deployer = deployer.Bytes()

		// Setup the composed stream with 3 primitive streams
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId.String(),
			MarkdownData: `
			| date       | primitive_1 | primitive_2 | primitive_3 |
			|------------|-------------|-------------|-------------|
			| 2021-01-01 | 10          | 20          | 30          |
			| 2021-01-02 | 15          | 25          | 35          |
			| 2021-01-03 | 20          | 30          | 40          |
			`,
			// All streams have equal weight (default is 1)
			Height: 1,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		// Generate the DBID for the composed stream
		composedDBID := utils.GenerateDBID(composedStreamId.String(), deployer.Bytes())

		// Query the composed stream to get the aggregated values
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			DBID:     composedDBID,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-03",
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records from composed stream")
		}

		// Verify the results
		// Since all streams have equal weight (1), the aggregated value should be the average
		expected := `
		| date       | value |
		|------------|-------|
		| 2021-01-01 | 20.000000000000000000 |
		| 2021-01-02 | 25.000000000000000000 |
		| 2021-01-03 | 30.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}
