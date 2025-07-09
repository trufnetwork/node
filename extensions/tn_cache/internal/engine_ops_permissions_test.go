package internal_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestExtensionAgentPermissions verifies that the extension agent can access both public 
// and private streams for caching purposes. The SQL authorization functions check if 
// wallet_address = 'extension_agent' and grant unrestricted access.
func TestExtensionAgentPermissions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "extension_agent_permissions_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testExtensionAgentAccess(t),
		},
	}, testutils.GetTestOptions())
}

func testExtensionAgentAccess(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create test data provider
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000789")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())
		logger := log.NewStdoutLogger().New("extension_agent_test")

		// Test 1: Verify extension agent can access public streams
		t.Run("AccessPublicStream", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			// Create a public composed stream with unique ID
			timestamp := time.Now().UnixNano()
			publicStreamId := util.GenerateStreamId(fmt.Sprintf("public_composed_%d", timestamp))

			// Setup a public composed stream (public by default)
			err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
				Platform: txPlatform,
				StreamId: publicStreamId,
				MarkdownData: `
				| event_time | public_val_1 | public_val_2 |
				|------------|---------------|---------------|
				| 100        | 50            | 100           |
				| 200        | 75            | 125           |
				`,
				Height: 1,
			})
			require.NoError(t, err)

			// Stream is public by default (read_visibility = 0)

			// Test that extension agent can list the stream
			engineOps := internal.NewEngineOperations(txPlatform.Engine, txPlatform.DB, "main", logger)

			// This should work for public streams
			streams, err := engineOps.ListComposedStreams(ctx, deployer.Address())
			require.NoError(t, err, "Extension agent should be able to list streams")

			// Verify our public stream is in the list
			found := false
			for _, stream := range streams {
				if stream == publicStreamId.String() {
					found = true
					break
				}
			}
			assert.True(t, found, "Public stream should be accessible to extension agent")

			// Test that extension agent can get category streams
			// Note: Private visibility affects reading data, not listing components/taxonomy
			categoryStreams, err := engineOps.GetCategoryStreams(ctx, deployer.Address(), publicStreamId.String(), 0)
			require.NoError(t, err, "Extension agent should be able to get category streams")

			// Filter out parent stream
			var childStreams []internal.CategoryStream
			for _, cs := range categoryStreams {
				if cs.StreamID != publicStreamId.String() {
					childStreams = append(childStreams, cs)
				}
			}
			assert.Len(t, childStreams, 2, "Should access child streams of public stream")

			// Test that extension agent can get records
			fromTime := int64(100)
			toTime := int64(200)
			records, err := engineOps.GetRecordComposed(ctx, deployer.Address(), publicStreamId.String(), &fromTime, &toTime)
			require.NoError(t, err, "Extension agent should be able to get records")
			
			// Debug: Check what we got
			t.Logf("GetRecordComposed returned %d records", len(records))
			for i, record := range records {
				t.Logf("Record %d: EventTime=%d, Value=%v", i, record.EventTime, record.Value)
			}
			
			// The test data has event times 100 and 200, so we should get 2 records
			assert.GreaterOrEqual(t, len(records), 2, "Should access records from public stream")
		}))

		// Test 2: Verify extension agent can access private streams
		t.Run("AccessPrivateStream", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			
			// Create a private stream
			privateStreamId := util.GenerateStreamId("test_private_stream")
			err := setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
				Platform:     txPlatform,
				StreamId:     privateStreamId,
				MarkdownData: `
				| event_time | value |
				|------------|-------|
				| 100        | 50    |
				`,
				Height: 1,
			})
			require.NoError(t, err)
			
			// Make it private
			err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
				Platform: txPlatform,
				Locator: types.StreamLocator{
					StreamId:     privateStreamId,
					DataProvider: deployer,
				},
				Key:     "read_visibility",
				Value:   "1",
				ValType: "int",
				Height:  2,
			})
			require.NoError(t, err)
			
			// Try to access the private stream
			engineOps := internal.NewEngineOperations(txPlatform.Engine, txPlatform.DB, "main", logger)
			fromTime := int64(100)
			toTime := int64(100)
			records, err := engineOps.GetRecordComposed(ctx, deployer.Address(), privateStreamId.String(), &fromTime, &toTime)
			
			// Extension agent can access private streams due to SQL authorization check
			require.NoError(t, err, "Extension agent should be able to access private streams")
			assert.NotEmpty(t, records, "Extension agent can access private streams")
		}))

		return nil
	}
}
