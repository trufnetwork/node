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
	AGGR03: Taxonomies define the mapping of child streams, including a period of validity for each weight. (start_date and end_date, otherwise not set)

	bare minimum test:
		composed stream
		create 2 primitive streams
		add only one as child without a start date
		add the other with a some start date
		add the first again with a later start date

		query and check that the values are correct

*/

// FIXME: This test is not working as expected with current contract.
// TestAGGR03_TaxonomyValidityPeriods tests AGGR03: Taxonomies define the mapping of child streams, including a period of validity for each weight. (start_date and end_date, otherwise not set)
func TestAGGR03_TaxonomyValidityPeriods(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "aggr03_taxonomy_validity_periods_test",
		FunctionTests: []kwilTesting.TestFunc{
			testAGGR03_TaxonomyValidityPeriods(t),
		},
	})
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
		platform.Deployer = deployer.Bytes()

		// Setup the first primitive stream
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: primitive1StreamId,
			Height:   1,
			MarkdownData: `
			| date       | value |
			|------------|-------|
			| 2021-01-01 | 10    |
			| 2021-01-05 | 10    |
			| 2021-01-10 | 10    |
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
			| date       | value |
			|------------|-------|
			| 2021-01-01 | 20    |
			| 2021-01-05 | 20    |
			| 2021-01-10 | 20    |
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

		// Generate the DBID for the composed stream
		composedDBID := utils.GenerateDBID(composedStreamId.String(), deployer.Bytes())

		// 1. Add the first primitive stream without a start date
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			DBID:          composedDBID,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{primitive1StreamId.String()},
			Weights:       []string{"1.0"},
			StartDate:     "", // No start date
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy for first primitive stream")
		}

		// 2. Add the second primitive stream with a start date
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			DBID:          composedDBID,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{primitive2StreamId.String()},
			Weights:       []string{"1.0"},
			StartDate:     "2021-01-05", // With start date
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy for second primitive stream")
		}

		// 3. Add the first primitive stream again with a later start date
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			DBID:          composedDBID,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{primitive1StreamId.String()},
			Weights:       []string{"1.0"},
			StartDate:     "2021-01-10", // Later start date
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy for first primitive stream with later start date")
		}

		// Query the composed stream to get the aggregated values
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			DBID:     composedDBID,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-10",
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records from composed stream")
		}

		// Verify the results
		// 2021-01-01: Only primitive1 with weight 1.0 is active (value = 10)
		// 2021-01-05: only primitive2 with weight 1.0 is active (value = 20)
		// 2021-01-10: only primitive1 with weight 1.0 is active (value = 10)
		expected := `
		| date       | value |
		|------------|-------|
		| 2021-01-01 | 10.000000000000000000 |
		| 2021-01-05 | 20.000000000000000000 |
		| 2021-01-10 | 10.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}
