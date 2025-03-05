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
// 	AGGR06: Only 1 taxonomy version can be active in a point in time.

// 	bare minimum test:
// 		composed stream with 2 child streams (all primitives, to make it easy to insert data)
// 		we add a new taxonomy version with a start date with only one of the child streams defined
// 		we add a new taxonomy version with the same start date but with the other child stream defined
// 		we insert a different record for both child streams, expect to return the later record
// */

// // TestAGGR06_SingleActiveTaxonomy tests AGGR06: Only 1 taxonomy version can be active in a point in time.
// func TestAGGR06_SingleActiveTaxonomy(t *testing.T) {
// 	t.Skip("Test skipped: aggregation stream tests temporarily disabled")
// 	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
// 		Name: "aggr06_single_active_taxonomy_test",
// 		FunctionTests: []kwilTesting.TestFunc{
// 			testAGGR06_SingleActiveTaxonomy(t),
// 		},
// 	})
// }

// func testAGGR06_SingleActiveTaxonomy(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		// Create a composed stream with 2 child primitive streams
// 		composedStreamId := util.GenerateStreamId("composed_stream_test")
// 		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
// 		if err != nil {
// 			return errors.Wrap(err, "error creating ethereum address")
// 		}
// 		platform.Deployer = deployer.Bytes()

// 		// Setup the composed stream with 2 primitive streams
// 		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
// 			Platform: platform,
// 			StreamId: composedStreamId.String(),
// 			MarkdownData: `
// 			| date       | primitive_1 | primitive_2 |
// 			|------------|-------------|-------------|
// 			| 2021-01-01 | 10          | 20          |
// 			`,
// 			Height: 1,
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error setting up composed stream")
// 		}

// 		// Generate the DBID for the composed stream
// 		composedDBID := utils.GenerateDBID(composedStreamId.String(), deployer.Bytes())

// 		// Get the primitive stream IDs
// 		primitive1StreamId := util.GenerateStreamId("primitive_1")
// 		primitive2StreamId := util.GenerateStreamId("primitive_2")

// 		// Add the first taxonomy version with a start date with only the first child stream defined
// 		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
// 			Platform:      platform,
// 			DBID:          composedDBID,
// 			DataProviders: []string{deployer.Address()},
// 			StreamIds:     []string{primitive1StreamId.String()},
// 			Weights:       []string{"1.0"},
// 			StartDate:     "2021-01-01", // Same start date
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error setting taxonomy for first primitive stream")
// 		}

// 		// Add a second taxonomy version with the same start date but with the second child stream defined
// 		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
// 			Platform:      platform,
// 			DBID:          composedDBID,
// 			DataProviders: []string{deployer.Address()},
// 			StreamIds:     []string{primitive2StreamId.String()},
// 			Weights:       []string{"1.0"},
// 			StartDate:     "2021-01-01", // Same start date
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error setting taxonomy for second primitive stream")
// 		}

// 		// Query the composed stream to get the aggregated values
// 		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
// 			Platform: platform,
// 			DBID:     composedDBID,
// 			DateFrom: "2021-01-01",
// 			DateTo:   "2021-01-01",
// 			Height:   1,
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error getting records from composed stream")
// 		}

// 		// Verify the results - we expect to get the value from the second taxonomy (primitive_2)
// 		// since it was added later and should override the first taxonomy
// 		expected := `
// 		| date       | value |
// 		|------------|-------|
// 		| 2021-01-01 | 20.000000000000000000 |
// 		`

// 		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

// 		// TODO: Add DateActive and OnlyActive to describe_taxonomies, otherwise we can't test it
// 		// // Verify the taxonomy versions by checking describe_taxonomies
// 		// taxonomyResult, err := procedure.DescribeTaxonomies(ctx, procedure.DescribeTaxonomiesInput{
// 		// 	Platform: platform,
// 		// 	DBID:     composedDBID,
// 		// 	// TODO: Add DateActive and OnlyActive to describe_taxonomies, otherwise we can't test it
// 		// 	// DateActive:    "2021-01-01",
// 		// 	// OnlyActive: true,
// 		// })
// 		// if err != nil {
// 		// 	return errors.Wrap(err, "error describing taxonomies")
// 		// }

// 		// // We expect only the latest taxonomy version to be active
// 		// if len(taxonomyResult) != 1 {
// 		// 	return errors.Errorf("expected 1 active taxonomy, got %d", len(taxonomyResult))
// 		// }

// 		// // The active taxonomy should be for primitive_2
// 		// if taxonomyResult[0][0] != primitive2StreamId.String() {
// 		// 	return errors.Errorf("expected active taxonomy to be for primitive_2, got %s", taxonomyResult[0][0])
// 		// }

// 		return nil
// 	}
// }
