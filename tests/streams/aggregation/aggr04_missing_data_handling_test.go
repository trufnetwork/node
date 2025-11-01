package tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_cache"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

/*
	AGGR04: If a child stream doesn't have data for the given date (including last available data), the composed stream will not count it's weight for that date.

	bare minimum test:
		composed stream with 2 child streams (all primitives, to make it easy to insert data)
		each have the same weight
		one of them has data for all 4 days
		the other starts only on the 2nd day, and on 3rd day is missing too

		we query and observe that the first day isn't affected by the one with missing data
		but we observe that the third day is uses the value from the second day on the one with missing data
*/

// TestAGGR04_MissingDataHandling tests AGGR04: If a child stream doesn't have data for the given date (including last available data), the composed stream will not count it's weight for that date.
func TestAGGR04_MissingDataHandling(t *testing.T) {
	cacheConfig := testutils.SimpleCache("0x0000000000000000000000000000000000000123", "*")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "aggr04_missing_data_handling_test",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			wrapTestWithCacheModes(t, "AGGR04_MissingDataHandling", testAGGR04_MissingDataHandling),
		},
	}, testutils.GetTestOptionsWithCache(cacheConfig))
}

func testAGGR04_MissingDataHandling(t *testing.T, useCache bool) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create a composed stream with 2 child primitive streams
		composedStreamId := util.GenerateStreamId("composed_stream_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())
		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// Setup the composed stream with 2 primitive streams
		// One stream has data for all 4 days
		// The other starts only on the 2nd day, and on 3rd day is missing too (has data on 2nd and 4th day)
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | value_1 | value_2 |
			|------------|---------|---------|
			| 1          | 10      |         |
			| 2          | 20      | 40      |
			| 3          | 30      |         |
			| 4          | 40      | 80      |
			`,
			// Both streams have equal weight (default is 1)
			Height: 1,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		// Set up cache (only when useCache is true)
		if useCache {
			_, err := tn_cache.GetTestHelper().RefreshAllStreamsSync(ctx)
			if err != nil {
				return errors.Wrap(err, "error refreshing cache")
			}
		}

		fromTime := int64(1)
		toTime := int64(4)

		// Query the composed stream to get the aggregated values
		result, err := procedure.GetRecordWithLogs(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     composedStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			Height:   1,
			UseCache: &useCache,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records from composed stream")
		}
		if useCache {
			assert.True(t, result.CacheHit, "Expected cache hit")
		}

		// Verify the results
		// For day 1: Only primitive_1 has data, so the value should be 10
		// For day 2: Both primitives have data, so the value should be (20+40)/2 = 30
		// For day 3: Both primitives have data, so the value should be (30+40(from past day))/2 = 35
		// For day 4: Both primitives have data, so the value should be (40+80)/2 = 60
		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 10.000000000000000000 |
		| 2          | 30.000000000000000000 |
		| 3          | 35.000000000000000000 |
		| 4          | 60.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result.Rows,
			Expected: expected,
		})

		return nil
	}
}
