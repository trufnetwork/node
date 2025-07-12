package tn_cache_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_cache"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestResolutionInTransaction tests that wildcard resolution finds streams created within a transaction
func TestResolutionInTransaction(t *testing.T) {
	// Use a fixed deployer address for consistency
	deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000456")
	cacheConfig := testutils.TestCache(deployer.Address(), "*") // Wildcard to cache all streams

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "resolution_transaction_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				platform = procedure.WithSigner(platform, deployer.Bytes())
				helper := testutils.SetupCacheTest(ctx, platform, cacheConfig)
				defer helper.Cleanup()

				// Test within transaction
				testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
					// Update helper to use transaction DB
					helper.SetDB(txPlatform.DB)

					// Create a new stream within the transaction
					streamId := util.GenerateStreamId("test_resolution_stream")
					err := setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
						Platform: txPlatform,
						StreamId: streamId,
						MarkdownData: `
						| event_time | value1 | value2 |
						| ---------- | ------ | ------ |
						| 100        | 10     | 20     |
						| 200        | 30     | 40     |
						`,
						Height: 1,
					})
					require.NoError(t, err)

					// Trigger resolution to discover the new stream
					err = helper.TriggerResolution(ctx)
					require.NoError(t, err)

					// Verify the stream was discovered and cached
					ext := tn_cache.GetExtension()
					require.NotNil(t, ext)

					scheduler := ext.Scheduler()
					require.NotNil(t, scheduler)

					// Get resolved directives
					resolvedSpecs := scheduler.GetResolvedDirectives()

					// Should have at least one resolved stream
					assert.GreaterOrEqual(t, len(resolvedSpecs), 1, "Should resolve at least one stream")

					// Verify our stream is in the resolved list
					found := false
					for _, spec := range resolvedSpecs {
						if spec.StreamID == streamId.String() && spec.DataProvider == deployer.Address() {
							found = true
							break
						}
					}
					assert.True(t, found, "Newly created stream should be resolved by wildcard")

					// Refresh cache for the discovered stream
					_, err = helper.RefreshAllStreamsSync(ctx)
					require.NoError(t, err)
				})

				return nil
			},
		},
	}, testutils.GetTestOptionsWithCache(cacheConfig))
}
