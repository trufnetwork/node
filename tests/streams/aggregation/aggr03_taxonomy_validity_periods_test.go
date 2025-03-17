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
	AGGR03: Taxonomies define the mapping of child streams, including a period of validity for each weight. (start_date and end_date, otherwise not set)

	bare minimum test:
		composed stream
		create 2 primitive streams
		add only one as child without a start date
		add the other with a some start date
		add the first again with a later start date

		query and check that the values are correct

*/

// TestAGGR03_TaxonomyValidityPeriods tests AGGR03: Taxonomies define the mapping of child streams, including a period of validity for each weight. (start_date and end_date, otherwise not set)
func TestAGGR03_TaxonomyValidityPeriods(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "aggr03_taxonomy_validity_periods_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testAGGR03_TaxonomyValidityPeriods(t),
		},
	}, testutils.GetTestOptions())
}

func testAGGR03_TaxonomyValidityPeriods(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create a composed stream and 2 primitive streams
		composedStreamId := util.GenerateStreamId("composed_stream_test")
		primitive1StreamId := util.GenerateStreamId("primitive1")
		primitive2StreamId := util.GenerateStreamId("primitive2")

		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Setup the first primitive stream
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: primitive1StreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 1          | 10    |
			| 5          | 10    |
			| 10         | 10    |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up first primitive stream")
		}

		// Setup the second primitive stream
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: primitive2StreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 1          | 20    |
			| 5          | 20    |
			| 10         | 20    |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up second primitive stream")
		}

		// Setup the composed stream
		err = setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform,
			StreamId: composedStreamId,
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: deployer,
		}

		// 1. Add the first primitive stream without a start date
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{primitive1StreamId.String()},
			Weights:       []string{"1.0"},
			StartTime:     nil, // No start time
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy for first primitive stream")
		}

		startTime := int64(5)
		// 2. Add the second primitive stream with a start date
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{primitive2StreamId.String()},
			Weights:       []string{"1.0"},
			StartTime:     &startTime, // With start time
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy for second primitive stream")
		}

		startTime = int64(10)
		// 3. Add the first primitive stream again with a later start date
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{primitive1StreamId.String()},
			Weights:       []string{"1.0"},
			StartTime:     &startTime, // Later start time
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy for first primitive stream with later start date")
		}

		fromTime := int64(1)
		toTime := int64(10)
		// Query the composed stream to get the aggregated values
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &fromTime,
			ToTime:        &toTime,
			Height:        1,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records from composed stream")
		}

		// Verify the results
		// 1: Only primitive1 with weight 1.0 is active (value = 10)
		// 5: only primitive2 with weight 1.0 is active (value = 20)
		// 10: only primitive1 with weight 1.0 is active (value = 10)
		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 10.000000000000000000 |
		| 5          | 20.000000000000000000 |
		| 10         | 10.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}
