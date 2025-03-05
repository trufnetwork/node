package tests

// import (
// 	"context"
// 	"testing"

// 	"github.com/kwilteam/kwil-db/core/utils"
// 	kwilTesting "github.com/kwilteam/kwil-db/testing"

// 	"github.com/pkg/errors"
// 	"github.com/trufnetwork/node/tests/streams/tests/utils/procedure"
// 	"github.com/trufnetwork/node/tests/streams/tests/utils/setup"
// 	"github.com/trufnetwork/node/tests/streams/tests/utils/table"
// 	"github.com/trufnetwork/sdk-go/core/util"
// )

// /*
// 	AGGR02: Each child stream's contribution is weighted, and these weights can vary over time.

// 	bare minimum test:
// 		composed stream with 3 child streams (all primitives, to make it easy to insert data)
// 		each have different weights
// 		we query and we get the correct weighted avg value
// 		each has data in 3 days
// */

// // TestAGGR02_WeightedContributions tests AGGR02: Each child stream's contribution is weighted, and these weights can vary over time.
// func TestAGGR02_WeightedContributions(t *testing.T) {
// 	t.Skip("Test skipped: aggregation stream tests temporarily disabled")
// 	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
// 		Name: "aggr02_weighted_contributions_test",
// 		FunctionTests: []kwilTesting.TestFunc{
// 			testAGGR02_WeightedContributions(t),
// 		},
// 	})
// }

// func testAGGR02_WeightedContributions(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		// Create a composed stream with 3 child primitive streams with different weights
// 		composedStreamId := util.GenerateStreamId("weighted_composed_stream_test")
// 		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
// 		if err != nil {
// 			return errors.Wrap(err, "error creating ethereum address")
// 		}
// 		platform.Deployer = deployer.Bytes()

// 		// Setup the composed stream with 3 primitive streams with different weights
// 		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
// 			Platform: platform,
// 			StreamId: composedStreamId.String(),
// 			MarkdownData: `
// 			| date       | primitive_1 | primitive_2 | primitive_3 |
// 			|------------|-------------|-------------|-------------|
// 			| 2021-01-01 | 10          | 20          | 30          |
// 			| 2021-01-02 | 15          | 25          | 35          |
// 			| 2021-01-03 | 20          | 30          | 40          |
// 			`,
// 			// Different weights for each primitive stream
// 			Weights: []string{"1", "2", "3"},
// 			Height:  1,
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error setting up composed stream with weighted primitives")
// 		}

// 		// Generate the DBID for the composed stream
// 		composedDBID := utils.GenerateDBID(composedStreamId.String(), deployer.Bytes())

// 		// Query the composed stream to get the aggregated values
// 		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
// 			Platform: platform,
// 			DBID:     composedDBID,
// 			DateFrom: "2021-01-01",
// 			DateTo:   "2021-01-03",
// 			Height:   1,
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error getting records from composed stream")
// 		}

// 		// Verify the results
// 		// With weights [1, 2, 3], the weighted average calculation is:
// 		// (10*1 + 20*2 + 30*3) / (1+2+3) = (10 + 40 + 90) / 6 = 140 / 6 = 23.333...
// 		// (15*1 + 25*2 + 35*3) / (1+2+3) = (15 + 50 + 105) / 6 = 170 / 6 = 28.333...
// 		// (20*1 + 30*2 + 40*3) / (1+2+3) = (20 + 60 + 120) / 6 = 200 / 6 = 33.333...
// 		expected := `
// 		| date       | value |
// 		|------------|-------|
// 		| 2021-01-01 | 23.333333333333333333 |
// 		| 2021-01-02 | 28.333333333333333333 |
// 		| 2021-01-03 | 33.333333333333333333 |
// 		`

// 		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

// 		return nil
// 	}
// }
