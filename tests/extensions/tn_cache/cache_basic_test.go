package tn_cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TODO: This test is not passing because the kwilTesting framework doesn't load extensions.
// The tn_cache extension needs to be loaded during engine initialization to create the
// ext_tn_cache schema and tables. To make this test work, we would need either:
// 1. Modify the kwilTesting framework to support loading extensions, or
// 2. Create a different test harness that runs a full node with extensions enabled
// TestCacheBasic tests basic caching functionality for composed streams
func TestCacheBasic(t *testing.T) {
	t.Skip("Skipping test: kwilTesting framework doesn't support loading extensions")
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "cache_basic_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testCacheBasicFunctionality(t),
		},
	}, testutils.GetTestOptions())
}

func testCacheBasicFunctionality(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create a composed stream with child streams
		composedStreamId := util.GenerateStreamId("cache_test_composed")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())

		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

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
		require.NoError(t, err)

		// Test that cache schema tables exist by trying to query them
		// This validates the extension is loaded and initialized
		var tableExists bool
		err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB,
			`SELECT 1 FROM ext_tn_cache.cached_streams LIMIT 0`,
			nil,
			func(row *common.Row) error {
				// We don't expect any rows, just checking if table exists
				return nil
			},
		)
		// If no error, the table exists
		tableExists = (err == nil)
		assert.True(t, tableExists, "ext_tn_cache.cached_streams table should exist")

		// Insert a cache configuration for the composed stream
		err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB,
			`INSERT INTO ext_tn_cache.cached_streams (data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule)
			 VALUES ($data_provider, $stream_id, $from_timestamp, $last_refreshed, $cron_schedule)
			 ON CONFLICT (data_provider, stream_id) DO UPDATE SET 
			   from_timestamp = EXCLUDED.from_timestamp,
			   last_refreshed = EXCLUDED.last_refreshed,
			   cron_schedule = EXCLUDED.cron_schedule`,
			map[string]any{
				"data_provider":  deployer.Address(),
				"stream_id":      composedStreamId,
				"from_timestamp": int64(1),
				"last_refreshed": time.Now().UTC().Format(time.RFC3339),
				"cron_schedule":  "*/5 * * * *",
			},
			nil,
		)
		require.NoError(t, err)

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

		// Manually insert cached data (simulating what the scheduler would do)
		// In a real test with the scheduler running, this would happen automatically
		for _, record := range originalData {
			err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB,
				`INSERT INTO ext_tn_cache.cached_events (data_provider, stream_id, event_time, value)
				 VALUES ($data_provider, $stream_id, $event_time, $value)
				 ON CONFLICT (data_provider, stream_id, event_time) DO UPDATE SET value = EXCLUDED.value`,
				map[string]any{
					"data_provider": deployer.Address(),
					"stream_id":     composedStreamId,
					"event_time":    record[0],
					"value":         record[1],
				},
				nil,
			)
			require.NoError(t, err)
		}

		// Query cached data
		var cachedEvents [][]any
		err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB,
			`SELECT event_time, value FROM ext_tn_cache.cached_events 
			 WHERE data_provider = $data_provider AND stream_id = $stream_id 
			 AND event_time >= $from_time AND event_time <= $to_time
			 ORDER BY event_time`,
			map[string]any{
				"data_provider": deployer.Address(),
				"stream_id":     composedStreamId,
				"from_time":     fromTime,
				"to_time":       toTime,
			},
			func(row *common.Row) error {
				cachedEvents = append(cachedEvents, row.Values)
				return nil
			},
		)
		require.NoError(t, err)

		// Verify cached data matches original
		require.Len(t, cachedEvents, 3, "Should have 3 cached records")

		// Expected aggregated values (average of 3 child streams)
		expectedValues := []string{
			"200.000000000000000000", // (100+200+300)/3
			"250.000000000000000000", // (150+250+350)/3
			"300.000000000000000000", // (200+300+400)/3
		}

		for i, cached := range cachedEvents {
			assert.Equal(t, originalData[i][0], cached[0], "Event time should match")

			// Convert value to string for comparison
			cachedValue := cached[1].(string)
			assert.Equal(t, expectedValues[i], cachedValue, "Cached value should match expected aggregated value")
		}

		// Verify stream configuration exists
		var streamCount int64
		err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB,
			`SELECT COUNT(*) FROM ext_tn_cache.cached_streams 
			 WHERE data_provider = $data_provider AND stream_id = $stream_id`,
			map[string]any{
				"data_provider": deployer.Address(),
				"stream_id":     composedStreamId,
			},
			func(row *common.Row) error {
				if len(row.Values) > 0 {
					streamCount = row.Values[0].(int64)
				}
				return nil
			},
		)
		require.NoError(t, err)
		assert.Equal(t, int64(1), streamCount, "Should have one cached stream configuration")

		return nil
	}
}
