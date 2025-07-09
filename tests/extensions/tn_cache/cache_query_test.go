package tn_cache_test

import (
	"context"
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

// TestCacheQueryOptimization tests cache integration with get_record and get_index:
// - Cache hits improve query performance
// - Cache misses fall back to original logic
// - Frozen queries bypass cache for data integrity
// - Base time queries bypass cache
// - Graceful degradation when cache is disabled
func TestCacheQueryOptimization(t *testing.T) {
	// TODO: Enable when kwilTesting framework supports extensions
	t.Skip("Skipping: kwilTesting framework doesn't support loading extensions")

	deployer := "0x0000000000000000000000000000000000000123"
	streamId := util.GenerateStreamId("cache_query_test")
	cacheConfig := testutils.TestCache(deployer, streamId.String())

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "cache_query_optimization_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testGetRecordCacheHit(t, deployer, streamId, cacheConfig),
			testGetRecordCacheMiss(t, deployer, streamId, cacheConfig),
			testFrozenQueryBypass(t, deployer, streamId, cacheConfig),
			testBaseTimeQueryBypass(t, deployer, streamId, cacheConfig),
			testGetIndexWithCache(t, deployer, streamId, cacheConfig),
		},
	}, testutils.GetTestOptionsWithCache(cacheConfig))
}

func testGetRecordCacheHit(t *testing.T, deployer string, streamId util.StreamId, cacheConfig *testutils.CacheOptions) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Cache is already set up by the wrapper, but we need the helper for RefreshCache
		helper := testutils.SetupCacheTest(ctx, platform, cacheConfig)
		defer helper.Cleanup()

		deployerAddr, err := util.NewEthereumAddressFromString(deployer)
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployerAddr.Bytes())

		// Setup test data
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: streamId,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 1000       | 100   |
			| 2000       | 200   |
			| 3000       | 300   |
			`,
			Height: 1,
		})
		require.NoError(t, err)

		// Refresh cache
		recordsCached, err := helper.RefreshCache(ctx, deployer, streamId.String())
		require.NoError(t, err)
		assert.Equal(t, 3, recordsCached)

		// Measure cache hit performance
		fromTime := int64(1000)
		toTime := int64(3000)
		
		start := time.Now()
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     streamId,
				DataProvider: deployerAddr,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			Height:   1,
		})
		cacheHitDuration := time.Since(start)
		
		require.NoError(t, err)
		require.Len(t, result, 3, "Should return 3 cached records")

		// Clear cache to test miss performance
		_, err = platform.DB.Execute(ctx,
			`DELETE FROM ext_tn_cache.cached_events 
			 WHERE data_provider = $1 AND stream_id = $2`,
			deployer, streamId.String())
		require.NoError(t, err)

		// Measure cache miss performance
		start = time.Now()
		result2, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     streamId,
				DataProvider: deployerAddr,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			Height:   1,
		})
		cacheMissDuration := time.Since(start)
		
		require.NoError(t, err)
		require.Len(t, result2, 3, "Should return 3 records from source")

		// Cache hit should be faster than cache miss
		assert.Less(t, cacheHitDuration, cacheMissDuration, 
			"Cache hit should be faster than cache miss")

		return nil
	}
}

func testGetRecordCacheMiss(t *testing.T, deployer string, streamId util.StreamId, cacheConfig *testutils.CacheOptions) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Cache is already set up by the wrapper, but we need the helper for RefreshCache
		helper := testutils.SetupCacheTest(ctx, platform, cacheConfig)
		defer helper.Cleanup()

		deployerAddr, err := util.NewEthereumAddressFromString(deployer)
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployerAddr.Bytes())

		// Query without any cached data
		fromTime := int64(5000)
		toTime := int64(6000)
		
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     streamId,
				DataProvider: deployerAddr,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			Height:   1,
		})
		
		require.NoError(t, err)
		assert.Empty(t, result, "Should return empty result for non-existent time range")

		// Verify cache table is empty for this stream
		cacheResult, err := platform.DB.Execute(ctx,
			`SELECT COUNT(*) FROM ext_tn_cache.cached_events 
			 WHERE data_provider = $1 AND stream_id = $2`,
			deployer, streamId.String())
		require.NoError(t, err)
		assert.Equal(t, int64(0), cacheResult.Rows[0][0], "Cache should be empty")

		return nil
	}
}

func testFrozenQueryBypass(t *testing.T, deployer string, streamId util.StreamId, cacheConfig *testutils.CacheOptions) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Cache is already set up by the wrapper, but we need the helper for RefreshCache
		helper := testutils.SetupCacheTest(ctx, platform, cacheConfig)
		defer helper.Cleanup()

		deployerAddr, err := util.NewEthereumAddressFromString(deployer)
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployerAddr.Bytes())

		// Setup and cache data
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: streamId,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 1000       | 100   |
			| 2000       | 200   |
			| 3000       | 300   |
			`,
			Height: 1,
		})
		require.NoError(t, err)

		_, err = helper.RefreshCache(ctx, deployer, streamId.String())
		require.NoError(t, err)

		// Test frozen query bypasses cache
		fromTime := int64(1000)
		toTime := int64(3000)
		frozenAt := int64(2500)
		
		result, logs := GetRecordWithLogs(ctx, platform, procedure.GetRecordInput{
			StreamLocator: types.StreamLocator{
				StreamId:     streamId,
				DataProvider: deployerAddr,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			FrozenAt: &frozenAt,
			Height:   1,
		})
		
		require.NotNil(t, result)
		
		// Verify no cache-related logs for frozen query
		for _, log := range logs {
			assert.NotContains(t, log, "cache_hit", "Frozen queries should not log cache activity")
		}

		return nil
	}
}

func testBaseTimeQueryBypass(t *testing.T, deployer string, streamId util.StreamId, cacheConfig *testutils.CacheOptions) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Similar to frozen query test but with base_time parameter
		// TODO: Implement when get_index supports base_time parameter
		return nil
	}
}

func testGetIndexWithCache(t *testing.T, deployer string, streamId util.StreamId, cacheConfig *testutils.CacheOptions) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Cache is already set up by the wrapper, but we need the helper for RefreshCache
		helper := testutils.SetupCacheTest(ctx, platform, cacheConfig)
		defer helper.Cleanup()

		deployerAddr, err := util.NewEthereumAddressFromString(deployer)
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployerAddr.Bytes())

		// Setup composed stream for index calculation
		composedId := util.GenerateStreamId("cache_index_composed")
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedId,
			MarkdownData: `
			| event_time | value_1 | value_2 | value_3 |
			|------------|---------|---------|---------|
			| 1000       | 100     | 200     | 300     |
			| 2000       | 110     | 220     | 330     |
			| 3000       | 121     | 242     | 363     |
			`,
			Height: 1,
		})
		require.NoError(t, err)

		// TODO: Call get_index when available
		// The test would verify:
		// 1. Index calculation uses cached data
		// 2. Performance improvement from caching
		// 3. Correct index values with precision

		return nil
	}
}

// TestCachePerformanceBenchmark benchmarks cache vs non-cache performance
func TestCachePerformanceBenchmark(t *testing.T) {
	// TODO: Enable when extension support is available
	t.Skip("Skipping: Benchmark requires extension support")

	// This would benchmark:
	// - Query latency with/without cache
	// - Cache refresh time for various data sizes
	// - Concurrent access patterns
	// - Memory usage with large datasets
}