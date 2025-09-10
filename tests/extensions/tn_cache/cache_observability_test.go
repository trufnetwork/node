package tn_cache_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_cache"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/cache"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestCacheObservability verifies cache monitoring and logging functionality:
// - JSON formatted cache hit/miss logs
// - Proper log fields (cache_hit, cached_at_height, current_height)
// - Log capture mechanism works correctly
func TestCacheObservability(t *testing.T) {
	deployer := "0x0000000000000000000000000000000000000123"
	streamId := util.GenerateStreamId("cache_observability_test")
	cacheConfig := testutils.SimpleCache(deployer, streamId.String())

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "cache_observability_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				// Cache is already set up by the wrapper, but we need the helper for RefreshCache
				helper := cache.SetupCacheTest(ctx, platform, cacheConfig)
				defer helper.Cleanup()

				deployerAddr, err := util.NewEthereumAddressFromString(deployer)
				require.NoError(t, err)

				platform = procedure.WithSigner(platform, deployerAddr.Bytes())
				err = setup.CreateDataProvider(ctx, platform, deployerAddr.Address())
				if err != nil {
					return errors.Wrap(err, "error registering data provider")
				}

				// Setup test data - use composed stream to test cache
				err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
					Platform: platform,
					StreamId: streamId,
					MarkdownData: `
					| event_time | value_1 | value_2 | value_3 |
					|------------|---------|---------|---------|
					| 1          | 100     | 200     | 300     |
					| 2          | 150     | 250     | 350     |
					`,
					Height: 1,
				})
				require.NoError(t, err)

				// Test 1: Verify cache miss log format
				fromTime := int64(1)
				toTime := int64(2)

				// First query - should miss cache
				useCache := true
				result, err := procedure.GetRecordWithLogs(ctx, procedure.GetRecordInput{
					Platform: platform,
					StreamLocator: types.StreamLocator{
						StreamId:     streamId,
						DataProvider: deployerAddr,
					},
					FromTime: &fromTime,
					ToTime:   &toTime,
					Height:   1,
					UseCache: &useCache,
				})
				require.NoError(t, err)
				require.NotNil(t, result)
				require.NotEmpty(t, result.Rows)

				// Find cache-related log
				var cacheLogs []string
				t.Logf("Total logs from first query: %d", len(result.Logs))
				for i, log := range result.Logs {
					t.Logf("Log %d: %s", i, log)
					if strings.Contains(log, "cache_hit") {
						cacheLogs = append(cacheLogs, log)
					}
				}

				// Verify cache miss log is valid JSON
				require.Len(t, cacheLogs, 1, "Should have one cache miss log")
				var missLog map[string]interface{}
				err = json.Unmarshal([]byte(cacheLogs[0]), &missLog)
				assert.NoError(t, err, "Cache miss log should be valid JSON")
				assert.Equal(t, false, missLog["cache_hit"], "First query should be cache miss")

				// Refresh cache for hit test
				recordsCached, err := tn_cache.GetTestHelper().RefreshAllStreamsSync(ctx)
				require.NoError(t, err)
				assert.Equal(t, 2, recordsCached, "Should cache 2 aggregated records")

				// Test 2: Verify cache hit log format
				cacheLogs = nil // Reset logs
				useCache = true
				result2, err := procedure.GetRecordWithLogs(ctx, procedure.GetRecordInput{
					Platform: platform,
					StreamLocator: types.StreamLocator{
						StreamId:     streamId,
						DataProvider: deployerAddr,
					},
					FromTime: &fromTime,
					ToTime:   &toTime,
					Height:   1,
					UseCache: &useCache,
				})
				require.NoError(t, err, "Cache hit query should not error")

				require.NotNil(t, result2, "Second query should return results")
				require.NotEmpty(t, result2.Rows)

				// Find cache log
				t.Logf("Total logs from second query: %d", len(result2.Logs))
				for i, log := range result2.Logs {
					t.Logf("Log %d: %s", i, log)
					if strings.Contains(log, "cache_hit") {
						cacheLogs = append(cacheLogs, log)
					}
				}

				// Verify cache hit log is valid JSON with height
				require.Len(t, cacheLogs, 1, "Should have one cache hit log")
				var hitLog map[string]interface{}
				err = json.Unmarshal([]byte(cacheLogs[0]), &hitLog)
				assert.NoError(t, err, "Cache hit log should be valid JSON")
				assert.Equal(t, true, hitLog["cache_hit"], "Second query should be cache hit")
				assert.NotNil(t, hitLog["cache_height"], "Cache hit should include cache_height")
				assert.NotNil(t, hitLog["cache_refreshed_at_timestamp"], "Cache hit should include cache_refreshed_at_timestamp")

				// Verify height values are valid
				cacheHeight, ok := hitLog["cache_height"].(float64)
				assert.True(t, ok, "cache_height should be a number")
				assert.Greater(t, cacheHeight, float64(0), "cache_height should be positive")

				refreshTimestamp, ok := hitLog["cache_refreshed_at_timestamp"].(float64)
				assert.True(t, ok, "cache_refreshed_at_timestamp should be a number")
				assert.Greater(t, refreshTimestamp, float64(0), "cache_refreshed_at_timestamp should be positive")

				// Test 3: Verify get_index cache miss log format
				cacheLogs = nil // Reset logs
				indexResult, err := procedure.GetIndexWithLogs(ctx, procedure.GetIndexInput{
					Platform: platform,
					StreamLocator: types.StreamLocator{
						StreamId:     streamId,
						DataProvider: deployerAddr,
					},
					UseCache: &useCache,
					FromTime: &fromTime,
					ToTime:   &toTime,
					Height:   1,
				})
				require.NoError(t, err)
				require.NotNil(t, indexResult)
				require.NotEmpty(t, indexResult.Rows)

				// Find cache-related logs for index query
				t.Logf("Total logs from index query: %d", len(indexResult.Logs))
				for i, log := range indexResult.Logs {
					t.Logf("Index Log %d: %s", i, log)
					if strings.Contains(log, "cache_hit") {
						cacheLogs = append(cacheLogs, log)
					}
				}

				// Verify index cache hit logs are valid JSON with height
				// get_index can generate multiple cache hit logs (for base value and current values)
				require.GreaterOrEqual(t, len(cacheLogs), 1, "Should have at least one index cache hit log")
				for i, logStr := range cacheLogs {
					var indexHitLog map[string]interface{}
					err = json.Unmarshal([]byte(logStr), &indexHitLog)
					assert.NoError(t, err, "Index cache hit log %d should be valid JSON", i)
					assert.Equal(t, true, indexHitLog["cache_hit"], "Index query %d should be cache hit", i)
					assert.NotNil(t, indexHitLog["cache_height"], "Index cache hit %d should include cache_height", i)
					assert.NotNil(t, indexHitLog["cache_refreshed_at_timestamp"], "Index cache hit %d should include cache_refreshed_at_timestamp", i)

					// Verify height values are valid for index
					cacheHeight, ok := indexHitLog["cache_height"].(float64)
					assert.True(t, ok, "Index cache_height %d should be a number", i)
					assert.Greater(t, cacheHeight, float64(0), "Index cache_height %d should be positive", i)

					refreshTimestamp, ok := indexHitLog["cache_refreshed_at_timestamp"].(float64)
					assert.True(t, ok, "Index cache_refreshed_at_timestamp %d should be a number", i)
					assert.Greater(t, refreshTimestamp, float64(0), "Index cache_refreshed_at_timestamp %d should be positive", i)
				}

				return nil
			},
		},
	}, testutils.GetTestOptionsWithCache(cacheConfig))
}

// TODO: Add TestCacheMetrics when metrics are exposed
// This would test:
// - Cache hit rate metrics
// - Cache refresh duration metrics
// - Cache size metrics
// - Error rate metrics
