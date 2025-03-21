package tests

import (
	"context"
	"strings"
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
	AGGR08: Taxonomy weight changes create event points in the composed stream.

	This test verifies that weight changes create valid event points in composed streams:
	1. When taxonomy weights change at a specific time, a data point is created at that time
	3. This data point also serves as an anchor point for gap filling in subsequent queries

	Scenario:
	- Primitive streams have data at times 1 and 10 only
	- Taxonomy weights change at time 5
	- Queries should show:
	  a) Day 1: Data using original weights
	  b) Day 5: Data point created by the weight change
	  c) Day 10: Data using new weights
	- The gap filling behavior is consistent with all other queries. It is using as the last available data point the one created by the weight change.
*/

// TestAGGR08_WeightChangeEventPoints tests that weight changes create event points in the composed stream
func TestAGGR08_WeightChangeEventPoints(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "aggr08_weight_change_event_points_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testAGGR08_WeightChangeEventPoints(t),
		},
	}, testutils.GetTestOptions())
}

func testAGGR08_WeightChangeEventPoints(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create a composed stream with 2 child primitive streams
		composedStreamId := util.GenerateStreamId("weight_events_composed")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Setup the composed stream with 2 primitive streams initially
		// Only have data at times 1 and 10
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | primitive_1 | primitive_2 |
			|------------|-------------|-------------|
			| 1          | 10          | 100         |
			| 10         | 20          | 200         |
			`,
			// Initial weights: 30% for primitive1, 70% for primitive2
			Weights: []string{"0.3", "0.7"},
			Height:  1,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream with initial weights")
		}

		primitive1Id := util.GenerateStreamId("primitive_1")
		primitive2Id := util.GenerateStreamId("primitive_2")

		day1 := int64(1)
		day5 := int64(5)
		day6 := int64(6)
		day10 := int64(10)

		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: deployer,
		}

		// Now set a new taxonomy with updated weights: 70% for primitive1, 30% for primitive2
		// Starting from day 5 (no primitive data exists at this time)
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{
				deployer.Address(),
				deployer.Address(),
			},
			StreamIds: []string{
				primitive1Id.String(),
				primitive2Id.String(),
			},
			Weights: []string{
				"0.7", // 70% weight for primitive1 (was 30%)
				"0.3", // 30% weight for primitive2 (was 70%)
			},
			StartTime: &day5, // Start from day 5 (no primitive data exists here)
			Height:    1,
		})
		if err != nil {
			return errors.Wrap(err, "error setting updated taxonomy")
		}

		// Test Section 1: Verify weight change creates data point on the day of change
		// and query the entire range to verify all event points
		// 1. Day 1: Original data with original weights (10*0.3 + 100*0.7) = 73
		// 2. Day 5: Weight change creates an event point (10*0.7 + 100*0.3) = 37
		// 3. Day 10: New data with new weights (20*0.7 + 200*0.3) = 74
		query1result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &day1,
			ToTime:        &day10,
			Height:        10,
		})
		if err != nil {
			return errors.Wrap(err, "error getting record for query 1")
		}

		// Verify query 1 result (weight change day)
		query1expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 73    |
		| 5          | 37    |
		| 10         | 74    |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   query1result,
			Expected: query1expected,
			ColumnTransformers: map[string]func(string) string{
				"value": addDecimalZeros(18),
			},
		})

		// Test Section 2: Verify that querying from a day with no data (day 6)
		// also includes the latest data point before that day (day 5)
		// This demonstrates the gap filling behavior with the weight change point
		query2result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &day6,
			ToTime:        &day10,
			Height:        10,
		})
		if err != nil {
			return errors.Wrap(err, "error getting record for query 2")
		}

		// Verify result includes day 5 data point (which is before the query range)
		// This shows the system retrieves the last known data point for gap filling
		day6expected := `
		| event_time | value |
		|------------|-------|
		| 5          | 37    |
		| 10         | 74    |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   query2result,
			Expected: day6expected,
			ColumnTransformers: map[string]func(string) string{
				"value": addDecimalZeros(18),
			},
		})

		return nil
	}
}

func addDecimalZeros(count int) func(string) string {
	return func(value string) string {
		return value + "." + strings.Repeat("0", count)
	}
}
