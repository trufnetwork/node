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
	AGGR06: Only 1 taxonomy version can be active in a point in time.

	bare minimum test:
		composed stream with 2 child streams (all primitives, to make it easy to insert data)
		we add a new taxonomy version with a start date with only one of the child streams defined
		we add a new taxonomy version with the same start date but with the other child stream defined
		we insert a different record for both child streams, expect to return the later record
*/

// TestAGGR06_SingleActiveTaxonomy tests AGGR06: Only 1 taxonomy version can be active in a point in time.
func TestAGGR06_SingleActiveTaxonomy(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "aggr06_single_active_taxonomy_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testAGGR06_SingleActiveTaxonomy(t),
		},
	}, testutils.GetTestOptions())
}

func testAGGR06_SingleActiveTaxonomy(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create a composed stream with 2 child primitive streams
		composedStreamId := util.GenerateStreamId("composed_stream_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Setup the composed stream with 2 primitive streams
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | primitive_1 | primitive_2 |
			|------------|-------------|-------------|
			| 1          | 10          | 20          |
			`,
			Height: 1,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: deployer,
		}

		// Get the primitive stream IDs
		primitive1StreamId := util.GenerateStreamId("primitive_1")
		primitive2StreamId := util.GenerateStreamId("primitive_2")

		startTime := int64(1)
		// Add the first taxonomy version with a start date with only the first child stream defined
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{primitive1StreamId.String()},
			Weights:       []string{"1.0"},
			StartTime:     &startTime, // Same start time
			Height:        1,
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy for first primitive stream")
		}

		// Add a second taxonomy version with the same start date but with the second child stream defined
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{primitive2StreamId.String()},
			Weights:       []string{"1.0"},
			StartTime:     &startTime, // Same start time
			Height:        2,
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy for second primitive stream")
		}

		fromTime := int64(1)
		toTime := int64(1)

		// Query the composed stream to get the aggregated values
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &fromTime,
			ToTime:        &toTime,
			Height:        2,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records from composed stream")
		}

		// Verify the results - we expect to get the value from the second taxonomy (value_2)
		// since it was added later and should override the first taxonomy
		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 20.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		// TODO: Add TimeActive and OnlyActive to describe_taxonomies, otherwise we can't test it
		// // Verify the taxonomy versions by checking describe_taxonomies
		// taxonomyResult, err := procedure.DescribeTaxonomies(ctx, procedure.DescribeTaxonomiesInput{
		// 	Platform:      platform,
		// 	StreamLocator: composedStreamLocator,
		// 	// TODO: Add TimeActive and OnlyActive to describe_taxonomies, otherwise we can't test it
		// 	// TimeActive:  1,
		// 	// OnlyActive:  true,
		// })
		// if err != nil {
		// 	return errors.Wrap(err, "error describing taxonomies")
		// }

		// // We expect only the latest taxonomy version to be active
		// if len(taxonomyResult) != 1 {
		// 	return errors.Errorf("expected 1 active taxonomy, got %d", len(taxonomyResult))
		// }

		// // The active taxonomy should be for primitive_2
		// if taxonomyResult[0][0] != primitive2StreamId.String() {
		// 	return errors.Errorf("expected active taxonomy to be for primitive_2, got %s", taxonomyResult[0][0])
		// }

		return nil
	}
}
