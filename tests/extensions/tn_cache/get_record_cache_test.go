// Package tn_cache_test contains integration tests for the get_record action cache functionality
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
// - Cache hit scenario: Validates cached data is returned without expensive computation
// - Cache miss scenario: Ensures fallback to original logic works correctly
// - Frozen query bypass: Tests that historical queries bypass cache for data integrity
// - Cache disabled fallback: Verifies graceful degradation when extension is disabled
//
// Each test includes detailed TODOs for validation points that should be checked
// when the tests can actually run with extension support.
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

// TestGetRecordCacheIntegration tests the cache integration in get_record action
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
func TestGetRecordCacheIntegration(t *testing.T) {
	t.Skip("Skipping test: kwilTesting framework doesn't support loading extensions")

	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "get_record_cache_integration",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testCacheHitScenario(t),
			testCacheMissScenario(t),
			testFrozenQueryBypassesCache(t),
			testCacheDisabledFallback(t),
		},
	}, testutils.GetTestOptions())
}

// testCacheHitScenario tests that cache hits return cached data
// TODO: This test validates that:
// 1. tn_cache.is_enabled() returns true when extension is active
// 2. tn_cache.has_cached_data() correctly identifies available cached data
// 3. tn_cache.get_cached_data() returns the correct cached events
// 4. Cache hit NOTICE is logged properly
// 5. Cached data is returned without invoking expensive composed stream calculations
func testCacheHitScenario(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup test data - create a composed stream with 3 child streams
		composedStreamId := util.GenerateStreamId("cache_hit_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Create composed stream with test data
		// This creates 3 child streams with values that average to the expected cached values
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

		// Simulate cache being populated by the tn_cache scheduler
		// In production, this happens automatically based on cron schedules
		err = populateCache(ctx, platform, deployer.Address(), composedStreamId.String(), []CacheEvent{
			{EventTime: 1000, Value: "200.000000000000000000"}, // (100+200+300)/3 = 200
			{EventTime: 2000, Value: "250.000000000000000000"}, // (150+250+350)/3 = 250
			{EventTime: 3000, Value: "300.000000000000000000"}, // (200+300+400)/3 = 300
		})
		require.NoError(t, err)

		// Query using get_record - should hit cache and return quickly
		// TODO: Add timing assertion to verify cache is faster than original calculation
		fromTime := int64(1000)
		toTime := int64(3000)
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
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

		// Verify the cached data matches expected aggregated values
		expectedValues := []string{
			"200.000000000000000000", // Average of child stream values at time 1000
			"250.000000000000000000", // Average of child stream values at time 2000
			"300.000000000000000000", // Average of child stream values at time 3000
		}

		for i, record := range result {
			assert.Equal(t, int64(1000+i*1000), record[0], "Event time should match")
			assert.Equal(t, expectedValues[i], record[1], "Cached value should match")
		}

		// TODO: Verify that cache hit NOTICE was logged
		// TODO: Verify that expensive composed stream logic was not executed

		return nil
	}
}

// testCacheMissScenario tests that cache misses fall back to original logic
// TODO: This test validates that:
// 1. tn_cache.is_enabled() returns true but tn_cache.has_cached_data() returns false
// 2. Cache miss NOTICE is logged properly
// 3. Original get_record_composed logic is executed correctly
// 4. Results are identical to non-cached execution
// 5. Performance is slower than cache hit (takes time to compute)
func testCacheMissScenario(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup test data - composed stream without any cached data
		composedStreamId := util.GenerateStreamId("cache_miss_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000124")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())

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
			`,
			Height: 1,
		})
		require.NoError(t, err)

		// Intentionally don't populate cache - this should result in cache miss
		// TODO: Verify that ext_tn_cache.cached_events table is empty for this stream

		// Query using get_record - should miss cache and fallback to original logic
		// TODO: Add timing assertion to verify this is slower than cache hit
		fromTime := int64(1000)
		toTime := int64(2000)
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
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
		require.Len(t, result, 2, "Should return 2 records from original logic")

		// Verify the data matches expected composed stream calculations
		// These values should be computed using get_record_composed action
		expectedValues := []string{
			"200.000000000000000000", // (100+200+300)/3 = 200
			"250.000000000000000000", // (150+250+350)/3 = 250
		}

		for i, record := range result {
			assert.Equal(t, int64(1000+i*1000), record[0], "Event time should match")
			assert.Equal(t, expectedValues[i], record[1], "Fallback value should match original calculation")
		}

		// TODO: Verify that cache miss NOTICE was logged
		// TODO: Verify that get_record_composed was actually executed
		// TODO: Compare execution time with cache hit scenario

		return nil
	}
}

// testFrozenQueryBypassesCache tests that frozen queries bypass the cache
// TODO: This test validates that:
// 1. Cache bypass logic works correctly when $frozen_at IS NOT NULL
// 2. tn_cache.is_enabled() is never called when frozen_at is provided
// 3. Original get_record_composed logic is used for historical consistency
// 4. Cached values are completely ignored even if they exist
// 5. Results are deterministic based on frozen timestamp, not current cache state
func testFrozenQueryBypassesCache(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup test data - this tests a critical feature for historical data integrity
		composedStreamId := util.GenerateStreamId("frozen_bypass_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000125")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())

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
			`,
			Height: 1,
		})
		require.NoError(t, err)

		// Populate cache with intentionally wrong values to ensure frozen queries don't use them
		// This simulates a scenario where cache has been updated but we want historical data
		err = populateCache(ctx, platform, deployer.Address(), composedStreamId.String(), []CacheEvent{
			{EventTime: 1000, Value: "999.000000000000000000"}, // Deliberately wrong cached value
			{EventTime: 2000, Value: "888.000000000000000000"}, // Deliberately wrong cached value
		})
		require.NoError(t, err)

		// Query with frozen_at parameter - should completely bypass cache
		// frozen_at=1500 means "show me data as it existed at timestamp 1500"
		fromTime := int64(1000)
		toTime := int64(2000)
		frozenAt := int64(1500) // This should only return data up to timestamp 1500
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
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
		assert.Equal(t, int64(1000), result[0][0], "Event time should match")
		assert.Equal(t, "200.000000000000000000", result[0][1], "Should use original composed logic, not cache")

		// TODO: Verify that cache-related extension functions were never called
		// TODO: Verify that no cache-related NOTICE messages were logged
		// TODO: Test with different frozen_at values to ensure time filtering works

		return nil
	}
}

// testCacheDisabledFallback tests behavior when cache is disabled
// TODO: This test validates that:
// 1. tn_cache.is_enabled() returns false when extension is disabled/not loaded
// 2. Cache check logic is completely skipped when extension is disabled
// 3. Original get_record_composed logic works correctly without cache
// 4. No cache-related NOTICE messages are logged
// 5. System gracefully degrades to original behavior without errors
func testCacheDisabledFallback(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup test data - this tests graceful degradation when cache is unavailable
		composedStreamId := util.GenerateStreamId("cache_disabled_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000126")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Create composed stream with test data (2 child streams for simpler calculation)
		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			MarkdownData: `
			| event_time | value_1 | value_2 |
			|------------|---------|---------|
			| 1000       | 100     | 200     |
			| 2000       | 150     | 250     |
			`,
			Height: 1,
		})
		require.NoError(t, err)

		// Even if cache tables existed and had data, if extension is disabled, should use original logic
		// In this test environment, the extension is not loaded, so tn_cache.is_enabled() should return false
		// TODO: Verify that ext_tn_cache schema doesn't exist or extension functions are not available

		// Query using get_record - should use original logic due to disabled cache
		fromTime := int64(1000)
		toTime := int64(2000)
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
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
		require.Len(t, result, 2, "Should return 2 records from original logic")

		// Verify the data matches expected composed stream calculations
		// With 2 child streams, the values should be averaged
		expectedValues := []string{
			"150.000000000000000000", // (100+200)/2 = 150
			"200.000000000000000000", // (150+250)/2 = 200
		}

		for i, record := range result {
			assert.Equal(t, int64(1000+i*1000), record[0], "Event time should match")
			assert.Equal(t, expectedValues[i], record[1], "Should use original composed logic")
		}

		// TODO: Verify that no cache-related NOTICE messages were logged
		// TODO: Verify that tn_cache extension functions were never called
		// TODO: Test with various extension configuration states (disabled vs not loaded)

		return nil
	}
}

// CacheEvent represents a cached event for testing
type CacheEvent struct {
	EventTime int64
	Value     string
}

// populateCache simulates cache population for testing
// TODO: This helper function needs to be updated when extension support is added:
// 1. Use proper tn_cache extension APIs instead of direct SQL
// 2. Test cache configuration validation
// 3. Test cache event insertion error handling
// 4. Verify cache eviction policies work correctly
// 5. Test concurrent cache operations
func populateCache(ctx context.Context, platform *kwilTesting.Platform, dataProvider, streamId string, events []CacheEvent) error {
	// Insert cache configuration
	err := platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB,
		`INSERT INTO ext_tn_cache.cached_streams (data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule)
		 VALUES ($data_provider, $stream_id, $from_timestamp, $last_refreshed, $cron_schedule)
		 ON CONFLICT (data_provider, stream_id) DO UPDATE SET 
		   from_timestamp = EXCLUDED.from_timestamp,
		   last_refreshed = EXCLUDED.last_refreshed,
		   cron_schedule = EXCLUDED.cron_schedule`,
		map[string]any{
			"data_provider":  dataProvider,
			"stream_id":      streamId,
			"from_timestamp": int64(1000),
			"last_refreshed": time.Now().UTC().Format(time.RFC3339),
			"cron_schedule":  "*/5 * * * *",
		},
		nil,
	)
	if err != nil {
		return err
	}

	// Insert cached events
	for _, event := range events {
		err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB,
			`INSERT INTO ext_tn_cache.cached_events (data_provider, stream_id, event_time, value)
			 VALUES ($data_provider, $stream_id, $event_time, $value)
			 ON CONFLICT (data_provider, stream_id, event_time) DO UPDATE SET value = EXCLUDED.value`,
			map[string]any{
				"data_provider": dataProvider,
				"stream_id":     streamId,
				"event_time":    event.EventTime,
				"value":         event.Value,
			},
			nil,
		)
		if err != nil {
			return err
		}
	}

	return nil
}
