// Package tn_cache_test contains integration tests for the get_index action cache functionality
//
// TODO: IMPORTANT - These tests are currently skipped because:
// The kwilTesting framework doesn't support loading extensions during test execution.
// The tn_cache extension needs to be properly initialized to create schemas and register precompile functions.
//
// To enable these tests, one of the following approaches is required:
// 1. **Extend kwilTesting Framework**: Modify kwilTesting.Platform to support extension loading
// 2. **Custom Test Harness**: Create a new test harness that runs a full Kwil node with extensions
// 3. **Mock Extension Functions**: Create mock implementations of tn_cache extension functions
// 4. **Integration Test Environment**: Set up end-to-end tests in a real node environment
//
// Test Coverage:
// - Cache hit scenario: Validates cached data is retrieved and index calculated correctly
// - Cache miss scenario: Ensures fallback to original logic works correctly
// - Frozen query bypass: Tests that historical queries bypass cache for data integrity
// - Base time query bypass: Tests that custom base time queries bypass cache
// - Cache disabled fallback: Verifies graceful degradation when extension is disabled
// - Index calculation correctness: Verifies index values are calculated properly from cached data
//
// Each test includes detailed TODOs for validation points that should be checked
// when the tests can actually run with extension support.
package tn_cache_test

import (
	"context"
	"testing"

	"github.com/pkg/errors"
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

// TestGetIndexCacheIntegration tests the cache integration in get_index action
// TODO: Enable this test once the kwilTesting framework supports loading extensions
// Currently skipped because:
// 1. The kwilTesting framework doesn't initialize extensions during test setup
// 2. The tn_cache extension needs to be loaded to create the ext_tn_cache schema
// 3. Extension precompile functions (is_enabled, has_cached_data, get_cached_data) are not available
//
// To enable this test, one of the following approaches is needed:
// - Modify kwilTesting.Platform to support extension loading during initialization
// - Create a custom test harness that runs a full node with extensions enabled
// - Mock the extension functions for testing purposes
func TestGetIndexCacheIntegration(t *testing.T) {
	t.Skip("Skipping test: kwilTesting framework doesn't support loading extensions")

	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "get_index_cache_integration",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testIndexCacheHitScenario(t),
			testIndexCacheMissScenario(t),
			testIndexFrozenQueryBypassesCache(t),
			testIndexBaseTimeQueryBypassesCache(t),
			testIndexCacheDisabledFallback(t),
			testIndexCalculationFromCache(t),
		},
	}, testutils.GetTestOptions())
}

// testIndexCacheHitScenario tests that cache hits return cached data and calculate index correctly
// TODO: This test validates that:
// 1. tn_cache.is_enabled() returns true when extension is active
// 2. tn_cache.has_cached_data() correctly identifies available cached data
// 3. tn_cache.get_cached_data() returns the correct cached events
// 4. Cache hit NOTICE is logged properly for get_index
// 5. Index calculations are performed correctly on cached data
// 6. Base value is retrieved correctly from cached data
// 7. Zero division handling works when base value is zero
func testIndexCacheHitScenario(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup test data - create a composed stream with 3 child streams
		composedStreamId := util.GenerateStreamId("index_cache_hit_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())

		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// Create composed stream with test data
		// Values are chosen to create predictable index calculations
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | value_1 | value_2 | value_3 |
			|------------|---------|---------|---------|
			| 1000       | 100     | 200     | 300     |
			| 2000       | 150     | 250     | 350     |
			| 3000       | 200     | 300     | 400     |
			| 4000       | 300     | 400     | 500     |
			`,
			Height: 1,
		})
		require.NoError(t, err)

		// Simulate cache being populated by the tn_cache scheduler
		// Raw composed values: 200, 250, 300, 400 (averages of child streams)
		err = populateCache(ctx, platform, deployer.Address(), composedStreamId.String(), []CacheEvent{
			{EventTime: 1000, Value: "200.000000000000000000"}, // (100+200+300)/3 = 200
			{EventTime: 2000, Value: "250.000000000000000000"}, // (150+250+350)/3 = 250
			{EventTime: 3000, Value: "300.000000000000000000"}, // (200+300+400)/3 = 300
			{EventTime: 4000, Value: "400.000000000000000000"}, // (300+400+500)/3 = 400
		})
		require.NoError(t, err)

		// Test scenario 1: get_index without base_time (should use cache, default base_time=0)
		fromTime := int64(2000)
		toTime := int64(4000)
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
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
		require.Len(t, result, 3, "Should return 3 cached records")

		// Expected index values with base=200 (first cached value):
		// Event 2000: (250/200) * 100 = 125.0
		// Event 3000: (300/200) * 100 = 150.0
		// Event 4000: (400/200) * 100 = 200.0
		expectedIndexValues := []string{
			"125.000000000000000000", // 250/200 * 100
			"150.000000000000000000", // 300/200 * 100
			"200.000000000000000000", // 400/200 * 100
		}

		for i, record := range result {
			expectedTime := int64(2000 + i*1000)
			assert.Equal(t, expectedTime, record[0], "Event time should match")
			assert.Equal(t, expectedIndexValues[i], record[1], "Cached index value should match")
		}

		// Test scenario 2: get_index with explicit base_time (should bypass cache)
		baseTime := int64(1000)
		result2, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
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
		require.Len(t, result2, 3, "Should return 3 records from original logic")

		// With explicit base_time=1000, base value should be 200, so results should be:
		// Event 2000: (250/200) * 100 = 125.0 (same as cache scenario)
		// Event 3000: (300/200) * 100 = 150.0
		// Event 4000: (400/200) * 100 = 200.0
		for i, record := range result2 {
			expectedTime := int64(2000 + i*1000)
			assert.Equal(t, expectedTime, record[0], "Event time should match")
			assert.Equal(t, expectedIndexValues[i], record[1], "Original index value should match")
		}

		// TODO: Verify that cache hit NOTICE was logged for first query
		// TODO: Verify that cache miss NOTICE was logged for second query (due to base_time)
		// TODO: Verify that expensive composed stream logic was not executed for cached query
		// TODO: Compare execution times to confirm cache is faster

		return nil
	}
}

// testIndexCacheMissScenario tests that cache misses fall back to original logic
// TODO: This test validates that:
// 1. tn_cache.is_enabled() returns true but tn_cache.has_cached_data() returns false
// 2. Cache miss NOTICE is logged properly for get_index
// 3. Original get_index_composed logic is executed correctly
// 4. Index calculations are identical to non-cached execution
// 5. Performance is slower than cache hit (takes time to compute)
func testIndexCacheMissScenario(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup test data - composed stream without any cached data
		composedStreamId := util.GenerateStreamId("index_cache_miss_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000124")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// Create composed stream with test data
		// This will trigger expensive computation since no cache exists
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | value_1 | value_2 | value_3 |
			|------------|---------|---------|---------|
			| 1000       | 100     | 200     | 300     |
			| 2000       | 150     | 250     | 350     |
			| 3000       | 200     | 300     | 400     |
			`,
			Height: 1,
		})
		require.NoError(t, err)

		// Intentionally don't populate cache - this should result in cache miss
		// TODO: Verify that ext_tn_cache.cached_events table is empty for this stream

		// Query using get_index - should miss cache and fallback to original logic
		// TODO: Add timing assertion to verify this is slower than cache hit
		fromTime := int64(1000)
		toTime := int64(3000)
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
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
		require.Len(t, result, 3, "Should return 3 records from original logic")

		// Verify the data matches expected index calculations from composed stream
		// Raw values: 200, 250, 300; base=200 (first value with default base_time=0)
		// Index values: 100.0, 125.0, 150.0
		expectedIndexValues := []string{
			"100.000000000000000000", // 200/200 * 100 = 100
			"125.000000000000000000", // 250/200 * 100 = 125
			"150.000000000000000000", // 300/200 * 100 = 150
		}

		for i, record := range result {
			expectedTime := int64(1000 + i*1000)
			assert.Equal(t, expectedTime, record[0], "Event time should match")
			assert.Equal(t, expectedIndexValues[i], record[1], "Fallback index value should match original calculation")
		}

		// TODO: Verify that cache miss NOTICE was logged
		// TODO: Verify that get_index_composed was actually executed
		// TODO: Compare execution time with cache hit scenario
		// TODO: Verify index calculation logic matches exactly

		return nil
	}
}

// testIndexFrozenQueryBypassesCache tests that frozen queries bypass the cache
// TODO: This test validates that:
// 1. Cache bypass logic works correctly when $frozen_at IS NOT NULL
// 2. tn_cache.is_enabled() is never called when frozen_at is provided
// 3. Original get_index_composed logic is used for historical consistency
// 4. Cached values are completely ignored even if they exist
// 5. Results are deterministic based on frozen timestamp, not current cache state
func testIndexFrozenQueryBypassesCache(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup test data - this tests a critical feature for historical data integrity
		composedStreamId := util.GenerateStreamId("index_frozen_bypass_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000125")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// Create composed stream with test data
		// This data represents the historical state that should be preserved
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | value_1 | value_2 | value_3 |
			|------------|---------|---------|---------|
			| 1000       | 100     | 200     | 300     |
			| 2000       | 150     | 250     | 350     |
			| 3000       | 200     | 300     | 400     |
			`,
			Height: 1,
		})
		require.NoError(t, err)

		// Populate cache with intentionally wrong values to ensure frozen queries don't use them
		// This simulates a scenario where cache has been updated but we want historical data
		err = populateCache(ctx, platform, deployer.Address(), composedStreamId.String(), []CacheEvent{
			{EventTime: 1000, Value: "999.000000000000000000"}, // Deliberately wrong cached value
			{EventTime: 2000, Value: "888.000000000000000000"}, // Deliberately wrong cached value
			{EventTime: 3000, Value: "777.000000000000000000"}, // Deliberately wrong cached value
		})
		require.NoError(t, err)

		// Query with frozen_at parameter - should completely bypass cache
		// frozen_at=1500 means "show me data as it existed at timestamp 1500"
		fromTime := int64(1000)
		toTime := int64(3000)
		frozenAt := int64(1500) // This should only return data up to timestamp 1500
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     composedStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			FrozenAt: &frozenAt,
			Height:   1,
		})
		require.NoError(t, err)
		require.Len(t, result, 1, "Should return 1 record from frozen query (only events <= 1500)")

		// Verify the data matches original calculation, completely ignoring cached values
		// Raw value: 200 (average at time 1000); Index: 100.0 (200/200 * 100)
		assert.Equal(t, int64(1000), result[0][0], "Event time should match")
		assert.Equal(t, "100.000000000000000000", result[0][1], "Should use original index logic, not cache")

		// TODO: Verify that cache-related extension functions were never called
		// TODO: Verify that no cache-related NOTICE messages were logged
		// TODO: Test with different frozen_at values to ensure time filtering works
		// TODO: Test that frozen queries work correctly with base_time parameters

		return nil
	}
}

// testIndexBaseTimeQueryBypassesCache tests that base_time queries bypass the cache
// TODO: This test validates that:
// 1. Cache bypass logic works correctly when $base_time IS NOT NULL
// 2. tn_cache extension functions are never called when base_time is provided
// 3. Original get_index_composed logic is used for custom base calculations
// 4. Cached values are completely ignored when base_time is specified
// 5. Index calculations use the correct base value from the original data
func testIndexBaseTimeQueryBypassesCache(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup test data - this tests cache bypass for custom base time calculations
		composedStreamId := util.GenerateStreamId("index_base_time_bypass_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000126")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// Create composed stream with test data
		// Values chosen to test different base time scenarios
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | value_1 | value_2 | value_3 |
			|------------|---------|---------|---------|
			| 1000       | 100     | 200     | 300     |
			| 2000       | 150     | 250     | 350     |
			| 3000       | 200     | 300     | 400     |
			| 4000       | 250     | 350     | 450     |
			`,
			Height: 1,
		})
		require.NoError(t, err)

		// Populate cache with values that would give different index results
		// This ensures that base_time queries completely bypass cache
		err = populateCache(ctx, platform, deployer.Address(), composedStreamId.String(), []CacheEvent{
			{EventTime: 1000, Value: "500.000000000000000000"}, // Wrong cached value
			{EventTime: 2000, Value: "600.000000000000000000"}, // Wrong cached value
			{EventTime: 3000, Value: "700.000000000000000000"}, // Wrong cached value
			{EventTime: 4000, Value: "800.000000000000000000"}, // Wrong cached value
		})
		require.NoError(t, err)

		// Test scenario 1: Use base_time=2000 (should bypass cache)
		fromTime := int64(2000)
		toTime := int64(4000)
		baseTime := int64(2000) // Custom base time
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
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
		require.Len(t, result, 3, "Should return 3 records from original logic")

		// With base_time=2000, base value = 250 (composed average at time 2000)
		// Expected index values:
		// Event 2000: (250/250) * 100 = 100.0
		// Event 3000: (300/250) * 100 = 120.0
		// Event 4000: (350/250) * 100 = 140.0
		expectedIndexValues := []string{
			"100.000000000000000000", // 250/250 * 100
			"120.000000000000000000", // 300/250 * 100
			"140.000000000000000000", // 350/250 * 100
		}

		for i, record := range result {
			expectedTime := int64(2000 + i*1000)
			assert.Equal(t, expectedTime, record[0], "Event time should match")
			assert.Equal(t, expectedIndexValues[i], record[1], "Base time index value should be calculated from original data")
		}

		// Test scenario 2: Different base_time=1000 (should also bypass cache)
		baseTime2 := int64(1000)
		result2, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     composedStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime,
			ToTime:   &toTime,
			BaseTime: &baseTime2,
			Height:   1,
		})
		require.NoError(t, err)
		require.Len(t, result2, 3, "Should return 3 records from original logic")

		// With base_time=1000, base value = 200 (composed average at time 1000)
		// Expected index values:
		// Event 2000: (250/200) * 100 = 125.0
		// Event 3000: (300/200) * 100 = 150.0
		// Event 4000: (350/200) * 100 = 175.0
		expectedIndexValues2 := []string{
			"125.000000000000000000", // 250/200 * 100
			"150.000000000000000000", // 300/200 * 100
			"175.000000000000000000", // 350/200 * 100
		}

		for i, record := range result2 {
			expectedTime := int64(2000 + i*1000)
			assert.Equal(t, expectedTime, record[0], "Event time should match")
			assert.Equal(t, expectedIndexValues2[i], record[1], "Different base time should give different index values")
		}

		// TODO: Verify that cache-related extension functions were never called for base_time queries
		// TODO: Verify that no cache-related NOTICE messages were logged for base_time queries
		// TODO: Test edge cases like base_time before first event, after last event
		// TODO: Test base_time with metadata default values

		return nil
	}
}

// testIndexCacheDisabledFallback tests behavior when cache is disabled
// TODO: This test validates that:
// 1. tn_cache.is_enabled() returns false when extension is disabled/not loaded
// 2. Cache check logic is completely skipped when extension is disabled
// 3. Original get_index_composed logic works correctly without cache
// 4. No cache-related NOTICE messages are logged
// 5. System gracefully degrades to original behavior without errors
func testIndexCacheDisabledFallback(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup test data - this tests graceful degradation when cache is unavailable
		composedStreamId := util.GenerateStreamId("index_cache_disabled_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000127")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// Create composed stream with test data (2 child streams for simpler calculation)
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | value_1 | value_2 |
			|------------|---------|---------|
			| 1000       | 100     | 200     |
			| 2000       | 150     | 250     |
			| 3000       | 200     | 300     |
			`,
			Height: 1,
		})
		require.NoError(t, err)

		// Even if cache tables existed and had data, if extension is disabled, should use original logic
		// In this test environment, the extension is not loaded, so tn_cache.is_enabled() should return false
		// TODO: Verify that ext_tn_cache schema doesn't exist or extension functions are not available

		// Query using get_index - should use original logic due to disabled cache
		fromTime := int64(1000)
		toTime := int64(3000)
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
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
		require.Len(t, result, 3, "Should return 3 records from original logic")

		// Verify the data matches expected index calculations from composed stream
		// With 2 child streams, the raw values should be averaged: 150, 200, 250
		// Base = 150 (first value); Index values: 100.0, 133.33, 166.67
		expectedIndexValues := []string{
			"100.000000000000000000", // 150/150 * 100 = 100.0
			"133.333333333333333333", // 200/150 * 100 = 133.33
			"166.666666666666666667", // 250/150 * 100 = 166.67
		}

		for i, record := range result {
			expectedTime := int64(1000 + i*1000)
			assert.Equal(t, expectedTime, record[0], "Event time should match")
			assert.Equal(t, expectedIndexValues[i], record[1], "Should use original index logic")
		}

		// TODO: Verify that no cache-related NOTICE messages were logged
		// TODO: Verify that tn_cache extension functions were never called
		// TODO: Test with various extension configuration states (disabled vs not loaded)

		return nil
	}
}

// testIndexCalculationFromCache tests index calculation accuracy from cached data
// TODO: This test validates that:
// 1. Index calculations from cached data match original calculations exactly
// 2. Base value retrieval from cache works correctly (exact, before, after, default)
// 3. Edge cases are handled properly (zero base value, missing base data)
// 4. Cache-based index calculations are mathematically correct
// 5. Precision is maintained through the cache-to-index calculation pipeline
func testIndexCalculationFromCache(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup test data - this tests index calculation accuracy with cached data
		composedStreamId := util.GenerateStreamId("index_calculation_cache_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000128")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())
		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// Create composed stream with test data designed for edge case testing
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | value_1 | value_2 | value_3 |
			|------------|---------|---------|---------|
			| 500        | 50      | 100     | 150     |
			| 1000       | 100     | 200     | 300     |
			| 1500       | 125     | 225     | 325     |
			| 2000       | 150     | 250     | 350     |
			| 2500       | 175     | 275     | 375     |
			`,
			Height: 1,
		})
		require.NoError(t, err)

		// Populate cache with calculated composed values
		// Raw composed values: 100, 200, 225, 250, 275
		err = populateCache(ctx, platform, deployer.Address(), composedStreamId.String(), []CacheEvent{
			{EventTime: 500, Value: "100.000000000000000000"},  // (50+100+150)/3 = 100
			{EventTime: 1000, Value: "200.000000000000000000"}, // (100+200+300)/3 = 200
			{EventTime: 1500, Value: "225.000000000000000000"}, // (125+225+325)/3 = 225
			{EventTime: 2000, Value: "250.000000000000000000"}, // (150+250+350)/3 = 250
			{EventTime: 2500, Value: "275.000000000000000000"}, // (175+275+375)/3 = 275
		})
		require.NoError(t, err)

		// Test scenario 1: Base value from exact match in cache
		fromTime := int64(1000)
		toTime := int64(2500)
		// Default base_time=0, but should find base value from cached data at 500 (closest to 0)
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
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
		require.Len(t, result, 4, "Should return 4 records")

		// Base value should be 100 (from event at 500, closest to base_time=0)
		// Expected index values:
		// Event 1000: (200/100) * 100 = 200.0
		// Event 1500: (225/100) * 100 = 225.0
		// Event 2000: (250/100) * 100 = 250.0
		// Event 2500: (275/100) * 100 = 275.0
		expectedIndexValues := []string{
			"200.000000000000000000", // 200/100 * 100
			"225.000000000000000000", // 225/100 * 100
			"250.000000000000000000", // 250/100 * 100
			"275.000000000000000000", // 275/100 * 100
		}

		for i, record := range result {
			expectedTime := int64(1000 + i*500)
			assert.Equal(t, expectedTime, record[0], "Event time should match")
			assert.Equal(t, expectedIndexValues[i], record[1], "Cache-based index should be calculated correctly")
		}

		// Test scenario 2: Query range that tests base value fallback logic
		fromTime2 := int64(1500)
		toTime2 := int64(2500)
		// This should still use base value 100 from the cache logic
		result2, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     composedStreamId,
				DataProvider: deployer,
			},
			FromTime: &fromTime2,
			ToTime:   &toTime2,
			Height:   1,
		})
		require.NoError(t, err)
		require.Len(t, result2, 3, "Should return 3 records")

		// Base value should still be 100 (from cached base value logic)
		expectedIndexValues2 := []string{
			"225.000000000000000000", // 225/100 * 100
			"250.000000000000000000", // 250/100 * 100
			"275.000000000000000000", // 275/100 * 100
		}

		for i, record := range result2 {
			expectedTime := int64(1500 + i*500)
			assert.Equal(t, expectedTime, record[0], "Event time should match")
			assert.Equal(t, expectedIndexValues2[i], record[1], "Index calculation should be consistent")
		}

		// TODO: Test edge case where base value is zero in cache
		// TODO: Test case where no base value is found in cache (should default to 1)
		// TODO: Verify precision is maintained through all calculations
		// TODO: Test large numbers and very small decimal values
		// TODO: Compare results with non-cached get_index for identical inputs

		return nil
	}
}
