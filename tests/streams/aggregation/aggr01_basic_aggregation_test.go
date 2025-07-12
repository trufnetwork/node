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
	AGGR01: A composed stream aggregates data from multiple child streams (which may be either primitive or composed).

	bare minimum test:
		composed stream with 3 child streams (all primitives, to make it easy to insert data)
		each have the same weight
		we query and we get the correct avg value
		each has data in 3 days
*/

// TestAGGR01_BasicAggregation tests AGGR01: A composed stream aggregates data from multiple child streams (which may be either primitive or composed).
func TestAGGR01_BasicAggregation(t *testing.T) {
	// Cache all streams from this deployer
	cacheConfig := testutils.TestCache("0x0000000000000000000000000000000000000123", "*")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "aggr01_basic_aggregation_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			wrapTestWithCacheModes(t, "AGGR01_BasicAggregation", testAGGR01_BasicAggregation),
		},
	}, testutils.GetTestOptionsWithCache(cacheConfig))
}

func testAGGR01_BasicAggregation(t *testing.T, useCache bool) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create a composed stream with 3 child primitive streams
		composedStreamId := util.GenerateStreamId("composed_stream_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Setup the composed stream with 3 primitive streams
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
			// All streams have equal weight (default is 1)
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
		toTime := int64(3)

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
		// here we should have a helper that, if useCache was used, we also check if we had a hit
		// ...
		if err != nil {
			return errors.Wrap(err, "error getting records from composed stream")
		}
		if useCache {
			assert.True(t, result.CacheHit, "Expected cache hit")
		}

		// Verify the results
		// Since all streams have equal weight (1), the aggregated value should be the average
		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 20.000000000000000000 |
		| 2          | 25.000000000000000000 |
		| 3          | 30.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result.Rows,
			Expected: expected,
		})

		return nil
	}
}
