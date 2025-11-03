package tests

import (
	"context"
	"testing"
	"time"

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

func TestStreamCacheBaseTimeVariants(t *testing.T) {
	baseStreamID := util.GenerateStreamId("stream_cache_base_time_variants")
	deployer := "0x0000000000000000000000000000000000000123"
	explicitBaseTime := int64(5555)

	cacheConfig := testutils.NewCacheOptions().
		WithEnabled().
		WithMaxBlockAge(-1*time.Second).
		WithStream(deployer, baseStreamID.String(), "0 0 31 2 *").
		WithStreamBaseTime(deployer, baseStreamID.String(), "0 0 31 2 *", explicitBaseTime)

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "stream_cache_base_time_variants",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			runStreamCacheBaseTimeVariants(t, cacheConfig, baseStreamID, explicitBaseTime),
		},
	}, testutils.GetTestOptionsWithCache(cacheConfig))
}

func runStreamCacheBaseTimeVariants(t *testing.T, cacheConfig *testutils.CacheOptions, streamID util.StreamId, explicitBaseTime int64) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		helper := cache.SetupCacheTest(ctx, platform, cacheConfig)
		defer helper.Cleanup()

		deployerAddress, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployerAddress.Bytes())
		if err := setup.CreateDataProvider(ctx, platform, deployerAddress.Address()); err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: streamID,
			MarkdownData: `
			| event_time | value_1 |
			|------------|---------|
			| 1          | 100     |
			| 2          | 200     |
			| 3          | 400     |
			`,
			Height: 1,
		})
		require.NoError(t, err)

		recordsCached, err := tn_cache.GetTestHelper().RefreshAllStreamsSync(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, recordsCached, 3, "expected cache refresh to process events")

		result, err := platform.DB.Execute(ctx,
			`SELECT base_time 
			   FROM ext_tn_cache.cached_streams 
			  WHERE data_provider = $1 AND stream_id = $2 
			  ORDER BY base_time NULLS FIRST`,
			deployerAddress.Address(),
			streamID.String(),
		)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "expected sentinel and base_time shards")

		var sentinelSeen, baseTimeSeen bool
		for _, row := range result.Rows {
			if row[0] == nil {
				sentinelSeen = true
				continue
			}
			value, ok := row[0].(int64)
			require.True(t, ok, "base_time should decode as int64")
			if value == explicitBaseTime {
				baseTimeSeen = true
			}
		}
		assert.True(t, sentinelSeen, "sentinel cache shard missing")
		assert.True(t, baseTimeSeen, "base_time-specific shard missing")

		useCache := true
		from := int64(1)
		to := int64(3)

		// sentinel lookup
		_, err = procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     streamID,
				DataProvider: deployerAddress,
			},
			FromTime: &from,
			ToTime:   &to,
			Height:   1,
			UseCache: &useCache,
		})
		require.NoError(t, err)

		// explicit base_time lookup
		baseTimePtr := explicitBaseTime
		_, err = procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     streamID,
				DataProvider: deployerAddress,
			},
			FromTime: &from,
			ToTime:   &to,
			BaseTime: &baseTimePtr,
			Height:   1,
			UseCache: &useCache,
		})
		require.NoError(t, err)

		return nil
	}
}
