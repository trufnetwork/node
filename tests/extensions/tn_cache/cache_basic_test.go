package tn_cache_test

import (
	"context"
	"testing"
	"time"

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

// TestCacheBasic tests basic caching functionality for composed streams
func TestCacheBasic(t *testing.T) {
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
		
		// Test that cache schema was created by the extension
		// This validates the extension is loaded and initialized
		var schemaExists bool
		err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB,
			`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'ext_tn_cache')`,
			nil,
			func(row *common.Row) error {
				if len(row.Values) > 0 {
					schemaExists = row.Values[0].(bool)
				}
				return nil
			},
		)
		require.NoError(t, err)
		assert.True(t, schemaExists, "ext_tn_cache schema should exist")
		
		// Insert a cache configuration for the composed stream
		err = platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB,
			`INSERT INTO ext_tn_cache.cached_streams (data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule)
			 VALUES ($data_provider, $stream_id, $from_timestamp, $last_refreshed, $cron_schedule)
			 ON CONFLICT (data_provider, stream_id) DO UPDATE SET 
			   from_timestamp = EXCLUDED.from_timestamp,
			   last_refreshed = EXCLUDED.last_refreshed,
			   cron_schedule = EXCLUDED.cron_schedule`,
			map[string]any{
				"data_provider": deployer.String(),
				"stream_id": composedStreamId,
				"from_timestamp": int64(1),
				"last_refreshed": time.Now().UTC().Format(time.RFC3339),
				"cron_schedule": "*/5 * * * *",
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
					"data_provider": deployer.String(),
					"stream_id": composedStreamId,
					"event_time": record[0],
					"value": record[1],
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
				"data_provider": deployer.String(),
				"stream_id": composedStreamId,
				"from_time": fromTime,
				"to_time": toTime,
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
				"data_provider": deployer.String(),
				"stream_id": composedStreamId,
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