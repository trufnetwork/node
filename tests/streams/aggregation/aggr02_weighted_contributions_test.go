package tests

import (
	"context"
	"testing"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

/*
	AGGR02: Each child stream's contribution is weighted, and these weights can vary over time.

	bare minimum test:
		composed stream with 3 child streams (all primitives, to make it easy to insert data)
		each have different weights
		we query and we get the correct weighted avg value
		each has data in 3 days
*/

// TestAGGR02_WeightedContributions tests AGGR02: Each child stream's contribution is weighted, and these weights can vary over time.
func TestAGGR02_WeightedContributions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "aggr02_weighted_contributions_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testAGGR02_WeightedContributions(t),
		},
	}, testutils.GetTestOptions())
}

func testAGGR02_WeightedContributions(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create a composed stream with 3 child primitive streams with different weights
		composedStreamId := util.GenerateStreamId("weighted_composed_stream_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Setup the composed stream with 3 primitive streams with different weights
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | value_1 | value_2 | value_3 |
			|------------|---------|---------|---------|
			| 1          | 10      | 20      | 30      |
			| 2          | 15      | 25      | 35      |
			| 3          | 20      | 30      | 40      |
			`,
			// Different weights for each primitive stream
			Weights: []string{"1", "2", "3"},
			Height:  1,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream with weighted primitives")
		}

		fromTime := int64(1)
		toTime := int64(3)

		// Query the composed stream to get the aggregated values
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     composedStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records from composed stream")
		}

		// Verify the results
		// With weights [1, 2, 3], the weighted average calculation is:
		// (10*1 + 20*2 + 30*3) / (1+2+3) = (10 + 40 + 90) / 6 = 140 / 6 = 23.333...
		// (15*1 + 25*2 + 35*3) / (1+2+3) = (15 + 50 + 105) / 6 = 170 / 6 = 28.333...
		// (20*1 + 30*2 + 40*3) / (1+2+3) = (20 + 60 + 120) / 6 = 200 / 6 = 33.333...
		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 23.333333333333333333 |
		| 2          | 28.333333333333333333 |
		| 3          | 33.333333333333333333 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}
