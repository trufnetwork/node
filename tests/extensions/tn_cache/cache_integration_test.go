package tn_cache_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

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

		useCache := true
		originalData, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
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
		require.NoError(t, err)
		require.Len(t, originalData, 3, "Should have 3 original records")

		// No need to commit since we're using the same DB connection now

		// Refresh the cache to populate it with our test data
		recordsCached, err := helper.RefreshAllStreamsSync(ctx)
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

		// Test get_index cache functionality
		baseTime := int64(1)

		// Query original index data from TN
		originalIndexData, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     composedStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			BaseTime: &baseTime,
			Height:   1,
		})
		require.NoError(t, err)
		require.Len(t, originalIndexData, 3, "Should have 3 original index records")

		// Query cached index data - cache should be used automatically
		cachedIndexData, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     composedStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			BaseTime: &baseTime,
			Height:   1,
		})
		require.NoError(t, err)
		require.Len(t, cachedIndexData, 3, "Should have 3 cached index records")

		// Verify cached index data matches original index data
		// Index is calculated as weighted average of individual stream indices
		// Stream 1: base=100, values=[100,150,200] -> indices=[100,150,200]
		// Stream 2: base=200, values=[200,250,300] -> indices=[100,125,150]
		// Stream 3: base=300, values=[300,350,400] -> indices=[100,116.67,133.33]
		// Weighted avg (1:2:3): [100, 130.56, 161.11]
		expectedIndexValues := []string{
			"100.000000000000000000", // All streams start at 100% of their base
			"130.555555555555555556", // Weighted average of individual indices
			"161.111111111111111111", // Weighted average of individual indices
		}

		for i, cached := range cachedIndexData {
			assert.Equal(t, originalIndexData[i][0], cached[0], "Index event time should match")
			assert.Equal(t, expectedIndexValues[i], cached[1], "Cached index value should match expected indexed value")
		}

		return nil
	}
}

// TestCacheIncludeChildrenForNestedComposed tests cache functionality with include_children option
// for composed streams that have other composed streams as children
func TestCacheIncludeChildrenForNestedComposed(t *testing.T) {
	parentComposedId := util.GenerateStreamId("cache_test_parent_composed")
	deployer := "0x0000000000000000000000000000000000000123"

	// Create cache configuration with include_children enabled for parent
	cacheConfig := testutils.NewCacheOptions().
		WithEnabled().
		WithMaxBlockAge(-1*time.Second). // Disable sync checking for tests
		WithComposedStream(deployer, parentComposedId.String(), "0 0 0 31 2 *", true)

	// Run the test with cache enabled
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "cache_include_children_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testCacheIncludeChildren(t, cacheConfig),
		},
	}, testutils.GetTestOptionsWithCache(cacheConfig))
}

func testCacheIncludeChildren(t *testing.T, cacheConfig *testutils.CacheOptions) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		helper := testutils.SetupCacheTest(ctx, platform, cacheConfig)
		defer helper.Cleanup()

		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Setup hierarchical stream structure:
		// Parent Composed -> Child Composed 1 & 2 -> Primitives 1-4
		childComposed1Id := util.GenerateStreamId("cache_test_child_composed_1")
		childComposed2Id := util.GenerateStreamId("cache_test_child_composed_2")
		parentComposedId := util.GenerateStreamId("cache_test_parent_composed")

		// Setup child composed 1 with primitives using markdown
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: childComposed1Id,
			MarkdownData: `
			| event_time | primitive_1 | primitive_2 |
			|------------|-------------|-------------|
			| 1          | 10          | 20          |
			| 2          | 15          | 25          |
			| 3          | 20          | 30          |
			`,
			Weights: []string{"0.5", "0.5"},
			Height:  1,
		})
		require.NoError(t, err, "Setup child composed 1 failed")

		// Setup child composed 2 with primitives using markdown
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: childComposed2Id,
			MarkdownData: `
			| event_time | primitive_3 | primitive_4 |
			|------------|-------------|-------------|
			| 1          | 30          | 40          |
			| 2          | 35          | 45          |
			| 3          | 40          | 50          |
			`,
			Weights: []string{"0.5", "0.5"},
			Height:  1,
		})
		require.NoError(t, err, "Setup child composed 2 failed")

		// Setup parent composed stream
		err = setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform,
			StreamId: parentComposedId,
			Height:   1,
		})
		require.NoError(t, err, "Setup parent composed stream failed")

		// Set parent taxonomy to include both child composed streams
		startTime := int64(0)
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: types.StreamLocator{StreamId: parentComposedId, DataProvider: deployer},
			DataProviders: []string{deployer.Address(), deployer.Address()},
			StreamIds:     []string{childComposed1Id.String(), childComposed2Id.String()},
			Weights:       []string{"0.5", "0.5"},
			StartTime:     &startTime,
			Height:        1,
		})
		require.NoError(t, err, "Set taxonomy for parent composed failed")

		// Verify parent stream exists in cache config
		verifyCacheStreamExists(t, ctx, platform, deployer.Address(), parentComposedId.String())

		// Refresh cache - should cache parent and auto-resolve children
		recordsCached, err := helper.RefreshAllStreamsSync(ctx)
		require.NoError(t, err)
		assert.Equal(t, 9, recordsCached, "should cache 9 records (3 parent + 3 child1 + 3 child2)")

		// Verify child streams were automatically added to cache
		verifyChildStreamsInCache(t, ctx, platform, deployer.Address(), childComposed1Id.String(), childComposed2Id.String())

		// Refresh cache for child streams to populate their data
		_, err = helper.RefreshAllStreamsSync(ctx)
		require.NoError(t, err, "Failed to refresh child composed 1 cache")

		_, err = helper.RefreshAllStreamsSync(ctx)
		require.NoError(t, err, "Failed to refresh child composed 2 cache")

		// Verify all composed streams have cached data (primitives should not)
		verifyCachedEventCounts(t, ctx, platform, deployer.Address(), map[string]int64{
			parentComposedId.String(): 3,
			childComposed1Id.String(): 3,
			childComposed2Id.String(): 3,
		})

		// Test data correctness with cache
		verifyParentDataFromCache(t, ctx, platform, deployer, parentComposedId)

		return nil
	}
}

// Helper functions for better readability

func verifyCacheStreamExists(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, dataProvider, streamId string) {
	result, err := platform.DB.Execute(ctx,
		`SELECT COUNT(*) FROM ext_tn_cache.cached_streams
		 WHERE data_provider = $1 AND stream_id = $2`,
		dataProvider,
		streamId,
	)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "Should have one row")
	count := result.Rows[0][0].(int64)
	assert.Equal(t, int64(1), count, "Stream should exist in cache config")
}

func verifyChildStreamsInCache(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, dataProvider, childId1, childId2 string) {
	result, err := platform.DB.Execute(ctx,
		`SELECT stream_id FROM ext_tn_cache.cached_streams
		 WHERE data_provider = $1 AND stream_id IN ($2, $3)
		 ORDER BY stream_id`,
		dataProvider,
		childId1,
		childId2,
	)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2, "Should have both child streams in cache config after include_children resolution")
}

func verifyCachedEventCounts(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, dataProvider string, expectedCounts map[string]int64) {
	for streamId, expectedCount := range expectedCounts {
		result, err := platform.DB.Execute(ctx,
			`SELECT COUNT(*) FROM ext_tn_cache.cached_events 
			 WHERE data_provider = $1 AND stream_id = $2`,
			dataProvider,
			streamId,
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		count := result.Rows[0][0].(int64)
		assert.Equal(t, expectedCount, count, "Stream %s should have %d cached events", streamId, expectedCount)
	}
}

func verifyParentDataFromCache(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, deployer util.EthereumAddress, parentComposedId util.StreamId) {
	fromTime := int64(1)
	toTime := int64(3)
	useCache := true

	parentData, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
		Platform: platform,
		StreamLocator: types.StreamLocator{
			StreamId:     parentComposedId,
			DataProvider: deployer,
		},
		FromTime: &fromTime,
		ToTime:   &toTime,
		Height:   1,
		UseCache: &useCache,
	})
	require.NoError(t, err)
	require.Len(t, parentData, 3, "Should get 3 parent records from cache")

	// Expected: average of averages
	// Child1: (10+20)/2=15, (15+25)/2=20, (20+30)/2=25
	// Child2: (30+40)/2=35, (35+45)/2=40, (40+50)/2=45
	// Parent: (15+35)/2=25, (20+40)/2=30, (25+45)/2=35
	expectedValues := []string{"25.000000000000000000", "30.000000000000000000", "35.000000000000000000"}

	for i, record := range parentData {
		assert.Equal(t, strconv.Itoa(i+1), record[0], "Event time should match")
		assert.Equal(t, expectedValues[i], record[1], "Parent value should be average of children")
	}
}
