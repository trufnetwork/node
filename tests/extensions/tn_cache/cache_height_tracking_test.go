package tn_cache_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/kwil-db/node/meta"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestCacheHeightTracking verifies that cache stores and returns blockchain height information:
// - Cache stores the blockchain height when data is cached
// - has_cached_data returns cached_at (actually the height) and logs show both cached_at_height and current_height
// - Height values are properly tracked through refresh cycles
func TestCacheHeightTracking(t *testing.T) {
	deployer := "0x0000000000000000000000000000000000000456"
	streamId := util.GenerateStreamId("height_tracking_test")
	cacheConfig := testutils.TestCache(deployer, streamId.String())

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "cache_height_tracking_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				helper := testutils.SetupCacheTest(ctx, platform, cacheConfig)
				defer helper.Cleanup()

				deployerAddr, err := util.NewEthereumAddressFromString(deployer)
				require.NoError(t, err)

				platform = procedure.WithSigner(platform, deployerAddr.Bytes())

				// Setup test data - use composed stream to test cache (like other working tests)
				err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
					Platform: platform,
					StreamId: streamId,
					MarkdownData: `
					| event_time | value_1 | value_2 | value_3 |
					|------------|---------|---------|---------|
					| 100        | 10      | 20      | 30      |
					| 200        | 15      | 25      | 35      |
					`,
					Height: 1,
				})
				require.NoError(t, err)

				// Refresh cache to populate data with height
				recordsCached, err := helper.RefreshAllStreamsSync(ctx)
				require.NoError(t, err)
				assert.Greater(t, recordsCached, 0, "Should have cached some records")

				// Wait a moment to ensure the database has processed the cache update
				time.Sleep(100 * time.Millisecond)

				// Update blockchain height to 2 to simulate progression
				err = meta.SetChainState(ctx, platform.DB, 2, []byte("test-hash-height-2"), false)
				require.NoError(t, err)

				// Query to trigger cache check - this should log cache metadata including heights
				useCache := true
				fromTime := int64(50)
				toTime := int64(250)
				
				// Execute query that will use cache and return logs
				result, err := procedure.GetRecordWithLogs(ctx, procedure.GetRecordInput{
					Platform: platform,
					StreamLocator: types.StreamLocator{
						StreamId:     streamId,
						DataProvider: deployerAddr,
					},
					UseCache: &useCache,
					FromTime: &fromTime,
					ToTime:   &toTime,
					Height:   2, // Simulate being at a later block height
				})
				require.NoError(t, err)
				require.NotNil(t, result)

				// Check logs for height information
				var cacheLog string
				for _, log := range result.Logs {
					if strings.Contains(log, "cache_hit") {
						cacheLog = log
						break
					}
				}
				
				require.NotEmpty(t, cacheLog, "Should have cache hit log with height information")
				t.Logf("Cache log: %s", cacheLog)
				
				// Verify the log contains height fields
				assert.Contains(t, cacheLog, "cache_hit", "Log should contain cache_hit field")
				assert.Contains(t, cacheLog, "cache_height", "Log should contain cache_height")
				assert.Contains(t, cacheLog, "cache_refreshed_at_timestamp", "Log should contain cache_refreshed_at_timestamp")
				
				// The cache_height should be 1 (when we cached)
				assert.Contains(t, cacheLog, `"cache_hit": true`, "Should be a cache hit")
				assert.Contains(t, cacheLog, `"cache_height": 1`, "Cache height should be 1")
				// Verify cache_refreshed_at_timestamp is present and positive
				assert.Regexp(t, `"cache_refreshed_at_timestamp": \d+`, cacheLog, "Should contain valid cache_refreshed_at_timestamp")

				return nil
			},
		},
	}, testutils.GetTestOptionsWithCache(cacheConfig))
}

