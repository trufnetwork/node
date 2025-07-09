package tn_cache_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

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

				// Setup test data
				err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
					Platform: platform,
					StreamId: streamId,
					MarkdownData: `
					| event_time | value |
					|------------|-------|
					| 1          | 100   |
					| 2          | 200   |
					`,
					Height: 1,
				})
				require.NoError(t, err)

				// Test 1: Verify cache miss log format
				fromTime := int64(1)
				toTime := int64(2)
				
				// First query - should miss cache
				result, logs := GetRecordWithLogs(ctx, platform, procedure.GetRecordInput{
					StreamLocator: types.StreamLocator{
						StreamId:     streamId,
						DataProvider: deployerAddr,
					},
					FromTime: &fromTime,
					ToTime:   &toTime,
					Height:   1,
				})
				require.NotNil(t, result)
				
				// Find cache-related log
				var cacheLogs []string
				for _, log := range logs {
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
				assert.Equal(t, 2, recordsCached)

				// Test 2: Verify cache hit log format
				cacheLogs = nil // Reset logs
				result2, logs2 := GetRecordWithLogs(ctx, platform, procedure.GetRecordInput{
					StreamLocator: types.StreamLocator{
						StreamId:     streamId,
						DataProvider: deployerAddr,
					},
					FromTime: &fromTime,
					ToTime:   &toTime,
					Height:   1,
				})
				require.NotNil(t, result2)

				// Find cache log
				for _, log := range logs2 {
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

// GetRecordWithLogs is a helper that captures logs from the engine call
func GetRecordWithLogs(ctx context.Context, platform *kwilTesting.Platform, input procedure.GetRecordInput) ([]procedure.ResultRow, []string) {
	deployer, _ := util.NewEthereumAddressFromBytes(platform.Deployer)

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: input.Height,
		},
		TxID:   platform.Txid(),
		Signer: platform.Deployer,
		Caller: deployer.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var resultRows [][]any
	r, _ := platform.Engine.Call(engineContext, platform.DB, "", "get_record", []any{
		input.StreamLocator.DataProvider.Address(),
		input.StreamLocator.StreamId.String(),
		input.FromTime,
		input.ToTime,
		input.FrozenAt,
	}, func(row *common.Row) error {
		values := make([]any, len(row.Values))
		copy(values, row.Values)
		resultRows = append(resultRows, values)
		return nil
	})

	// Process results
	results := make([]procedure.ResultRow, len(resultRows))
	for i, row := range resultRows {
		resultRow := procedure.ResultRow{}
		for _, value := range row {
			resultRow = append(resultRow, fmt.Sprintf("%v", value))
		}
		results[i] = resultRow
	}

	return results, r.Logs
}

// TODO: Add TestCacheMetrics when metrics are exposed
// This would test:
// - Cache hit rate metrics
// - Cache refresh duration metrics
// - Cache size metrics
// - Error rate metrics