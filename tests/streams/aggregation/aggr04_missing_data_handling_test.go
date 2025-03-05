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
// 	AGGR04: If a child stream doesn't have data for the given date (including last available data), the composed stream will not count it's weight for that date.

// 	bare minimum test:
// 		composed stream with 2 child streams (all primitives, to make it easy to insert data)
// 		each have the same weight
// 		one of them has data for all 4 days
// 		the other starts only on the 2nd day, and on 3rd day is missing too

// 		we query and observe that the first day isn't affected by the one with missing data
// 		but we observe that the third day is uses the value from the second day on the one with missing data
// */

// // TestAGGR04_MissingDataHandling tests AGGR04: If a child stream doesn't have data for the given date (including last available data), the composed stream will not count it's weight for that date.
// func TestAGGR04_MissingDataHandling(t *testing.T) {
// 	t.Skip("Test skipped: aggregation stream tests temporarily disabled")
// 	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
// 		Name: "aggr04_missing_data_handling_test",
// 		FunctionTests: []kwilTesting.TestFunc{
// 			testAGGR04_MissingDataHandling(t),
// 		},
// 	})
// }

// func testAGGR04_MissingDataHandling(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		// Create a composed stream with 2 child primitive streams
// 		composedStreamId := util.GenerateStreamId("composed_stream_test")
// 		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
// 		if err != nil {
// 			return errors.Wrap(err, "error creating ethereum address")
// 		}
// 		platform.Deployer = deployer.Bytes()

// 		// Setup the composed stream with 2 primitive streams
// 		// One stream has data for all 4 days
// 		// The other starts only on the 2nd day, and on 3rd day is missing too (has data on 2nd and 4th day)
// 		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
// 			Platform: platform,
// 			StreamId: composedStreamId.String(),
// 			MarkdownData: `
// 			| date       | primitive_1 | primitive_2 |
// 			|------------|-------------|-------------|
// 			| 2021-01-01 | 10          |             |
// 			| 2021-01-02 | 20          | 40          |
// 			| 2021-01-03 | 30          |             |
// 			| 2021-01-04 | 40          | 80          |
// 			`,
// 			// Both streams have equal weight (default is 1)
// 			Height: 1,
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error setting up composed stream")
// 		}

// 		// Generate the DBID for the composed stream
// 		composedDBID := utils.GenerateDBID(composedStreamId.String(), deployer.Bytes())

// 		// Query the composed stream to get the aggregated values
// 		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
// 			Platform: platform,
// 			DBID:     composedDBID,
// 			DateFrom: "2021-01-01",
// 			DateTo:   "2021-01-04",
// 			Height:   1,
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error getting records from composed stream")
// 		}

// 		// Verify the results
// 		// For day 1: Only primitive_1 has data, so the value should be 10
// 		// For day 2: Both primitives have data, so the value should be (20+40)/2 = 30
// 		// For day 3: Both primitives have data, so the value should be (30+40(from past day))/2 = 35
// 		// For day 4: Both primitives have data, so the value should be (40+80)/2 = 60
// 		expected := `
// 		| date       | value |
// 		|------------|-------|
// 		| 2021-01-01 | 10.000000000000000000 |
// 		| 2021-01-02 | 30.000000000000000000 |
// 		| 2021-01-03 | 35.000000000000000000 |
// 		| 2021-01-04 | 60.000000000000000000 |
// 		`

// 		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

// 		return nil
// 	}
// }
