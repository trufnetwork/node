package tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"
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
	AGGR09: Missing data handling uses last known value, and composed streams emit on child stream events.

	This test ensures two main behaviors:
	1. If a child primitive stream is missing data for a specific event time, the aggregation
	   uses the last known value for that stream.
	2. A composed stream generates an event point whenever any of its child streams emit a new value.

	Test Setup:
	- A composed stream with two child primitive streams.
	- Both child streams have equal weight (1.0).
	- At event time 1, both streams emit the value 10.
	- At event time 2, only the second stream emits a value (10), while the first has missing data.

	Expected Outcome:
	- The composed stream will have data points at both time 1 and time 2, because at least one child stream emitted a value at each of those times.
	- The aggregation for time 2 will use the last known value of the first stream (which is 10, from time 1).
	- The final aggregated result for both time 1 and time 2 should be 10.
*/

// TestAGGR09_DuplicateValues tests AGGR09: Duplicate values from multiple child streams are both counted in aggregation.
func TestAGGR09_DuplicateValues(t *testing.T) {
	cacheConfig := testutils.TestCache("0x0000000000000000000000000000000000000123", "*")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "aggr09_duplicate_values_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			wrapTestWithCacheModes(t, "AGGR09_DuplicateValues", testAGGR09_DuplicateValues),
		},
	}, testutils.GetTestOptionsWithCache(cacheConfig))
}

func testAGGR09_DuplicateValues(t *testing.T, useCache bool) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create a composed stream with 2 child primitive streams
		composedStreamId := util.GenerateStreamId("composed_stream_duplicate_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Setup the composed stream with 2 primitive streams
		// The two streams emit the same value at times 1 and 2.
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | value_1 | value_2 |
			|------------|---------|---------|

			| 1          | 10      | 10      |
			| 2          |         | 10      |
			`,
			// All streams have equal weight (default is 1)
			Height: 1,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		// Set up cache (only when useCache is true)
		if useCache {
			recordsCached, err := tn_cache.GetTestHelper().RefreshStreamCacheSync(ctx, deployer.Address(), composedStreamId.String())
			if err != nil {
				return errors.Wrap(err, "error refreshing cache")
			}
			if recordsCached == 0 {
				return errors.New("no records cached")
			}
		}

		fromTime := int64(1)
		toTime := int64(2)

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

		// Verify the results
		// Time 1: (10*1 + 10*1) / (1+1) = 10
		// Time 2: Uses last known value for value_1 (10 from time 1). So, (10*1 + 10*1) / (1+1) = 10
		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 10.000000000000000000 |
		| 2          | 10.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result.Rows,
			Expected: expected,
		})

		return nil
	}
}
