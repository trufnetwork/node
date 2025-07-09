package tn_cache_test

import (
	"context"
	"encoding/json"
	"strings"
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

// TestCacheObservability verifies cache monitoring and logging functionality:
// - JSON formatted cache hit/miss logs
// - Proper log fields (cache_hit, cached_at)
// - Log capture mechanism works correctly
func TestCacheObservability(t *testing.T) {
	deployer := "0x0000000000000000000000000000000000000123"
	streamId := util.GenerateStreamId("cache_observability_test")
	cacheConfig := testutils.TestCache(deployer, streamId.String())

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "cache_observability_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				// Cache is already set up by the wrapper, but we need the helper for RefreshCache
				helper := testutils.SetupCacheTest(ctx, platform, cacheConfig)
				defer helper.Cleanup()

				deployerAddr, err := util.NewEthereumAddressFromString(deployer)
				require.NoError(t, err)

				platform = procedure.WithSigner(platform, deployerAddr.Bytes())

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
				result, err := procedure.GetRecordWithLogs(ctx, procedure.GetRecordInput{
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
				recordsCached, err := helper.RefreshCache(ctx, deployer, streamId.String())
				require.NoError(t, err)
				assert.Equal(t, 2, recordsCached, "Should cache 2 aggregated records")

				// Test 2: Verify cache hit log format
				cacheLogs = nil // Reset logs
				result2, err := procedure.GetRecordWithLogs(ctx, procedure.GetRecordInput{
					Platform: platform,
					StreamLocator: types.StreamLocator{
						StreamId:     streamId,
						DataProvider: deployerAddr,
					},
					FromTime: &fromTime,
					ToTime:   &toTime,
					Height:   1,
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

				// Verify cache hit log is valid JSON with timestamp
				require.Len(t, cacheLogs, 1, "Should have one cache hit log")
				var hitLog map[string]interface{}
				err = json.Unmarshal([]byte(cacheLogs[0]), &hitLog)
				assert.NoError(t, err, "Cache hit log should be valid JSON")
				assert.Equal(t, true, hitLog["cache_hit"], "Second query should be cache hit")
				assert.NotNil(t, hitLog["cached_at"], "Cache hit should include cached_at timestamp")

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