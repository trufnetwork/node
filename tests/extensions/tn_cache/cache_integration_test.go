package tn_cache_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

var composedStreamId = util.GenerateStreamId("cache_test_composed")

// TestCacheIntegration tests core cache functionality including:
// - Cache schema initialization
// - Data integrity with composed streams and aggregation
// - Cache refresh mechanism
// - Cached data query correctness
func TestCacheIntegration(t *testing.T) {
	// Create cache configuration for testing
	// This sets up a cache that won't auto-refresh (scheduled for Feb 31st)
	cacheConfig := testutils.TestCache("0x0000000000000000000000000000000000000123", composedStreamId.String())

	// Run the test with cache enabled using the new wrapper
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "cache_integration_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testCacheBasicFunctionality(t, cacheConfig),
		},
	}, testutils.GetTestOptionsWithCache(cacheConfig))
}

func testCacheBasicFunctionality(t *testing.T, cacheConfig *testutils.CacheOptions) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Cache is already set up by the wrapper, but we need the helper for RefreshCache
		helper := testutils.SetupCacheTest(ctx, platform, cacheConfig)
		defer helper.Cleanup()

		// Create a composed stream with child streams
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Setup composed stream with test data
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | value_1 | value_2 | value_3 |
			|------------|---------|---------|---------|
			| 1          | 100     | 200     | 300     |
			| 2          | 150     | 250     | 350     |
			| 3          | 200     | 300     | 400     |
			`,
			Height: 1,
		})
		require.NoError(t, err, "Setup composed stream failed")


		// Test that cache schema tables exist by trying to query them
		// This validates the extension is loaded and initialized
		var tableExists bool
		_, err = platform.DB.Execute(ctx,
			`SELECT 1 FROM ext_tn_cache.cached_streams LIMIT 0`)
		// If no error, the table exists
		tableExists = (err == nil)
		assert.True(t, tableExists, "ext_tn_cache.cached_streams table should exist")

		// The cache configuration was already provided via test options
		// The extension should have already inserted the stream into cached_streams
		// Let's verify it's there
		result, err := platform.DB.Execute(ctx,
			`SELECT COUNT(*) FROM ext_tn_cache.cached_streams
			 WHERE data_provider = $1 AND stream_id = $2`,
			deployer.Address(),
			composedStreamId.String(),
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Should have one row")
		streamCount := result.Rows[0][0].(int64)
		assert.Equal(t, int64(1), streamCount, "Stream should be configured in cache")

		// Query original data from TN
		fromTime := int64(1)
		toTime := int64(3)

		originalData, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     composedStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			Height:   1,
		})
		require.NoError(t, err)
		require.Len(t, originalData, 3, "Should have 3 original records")

		// No need to commit since we're using the same DB connection now

		// Refresh the cache to populate it with our test data
		recordsCached, err := helper.RefreshCache(ctx, deployer.Address(), composedStreamId.String())
		require.NoError(t, err, "should be able to refresh cache")
		assert.Equal(t, 3, recordsCached, "should cache 3 records")

		// Query cached data
		result, err = platform.DB.Execute(ctx,
			`SELECT event_time, value FROM ext_tn_cache.cached_events 
			 WHERE data_provider = $1 AND stream_id = $2 
			 AND event_time >= $3 AND event_time <= $4
			 ORDER BY event_time`,
			deployer.Address(),
			composedStreamId.String(),
			fromTime,
			toTime,
		)
		require.NoError(t, err)
		cachedEvents := result.Rows

		// Verify cached data matches original
		require.Len(t, cachedEvents, 3, "Should have 3 cached records")

		// Expected aggregated values (average of 3 child streams)
		expectedValues := []string{
			"200.000000000000000000", // (100+200+300)/3
			"250.000000000000000000", // (150+250+350)/3
			"300.000000000000000000", // (200+300+400)/3
		}

		for i, cached := range cachedEvents {
			assert.Equal(t, originalData[i][0], strconv.Itoa(int(cached[0].(int64))), "Event time should match")

			// Cached values from DB come as *types.Decimal
			cachedValue := fmt.Sprintf("%v", cached[1])
			assert.Equal(t, expectedValues[i], cachedValue, "Cached value should match expected aggregated value")
		}

		// Verify stream configuration exists
		result, err = platform.DB.Execute(ctx,
			`SELECT COUNT(*) FROM ext_tn_cache.cached_streams 
			 WHERE data_provider = $1 AND stream_id = $2`,
			deployer.Address(),
			composedStreamId.String(),
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Should have one row")
		assert.Equal(t, int64(1), result.Rows[0][0].(int64), "Should have one cached stream configuration")

		return nil
	}
}
