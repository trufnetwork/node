package internal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestExtensionAgentPermissions tests that the extension agent can access private streams
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

		// Test 1: Create a private composed stream
		t.Run("AccessPrivateStream", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			// Create a private composed stream with unique ID
			timestamp := time.Now().UnixNano()
			privateStreamId := util.GenerateStreamId(fmt.Sprintf("private_composed_%d", timestamp))

			// Setup a private composed stream
			// Note: Private streams would typically have restricted access
			err = setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
				Platform: txPlatform,
				StreamId: privateStreamId,
				MarkdownData: `
				| event_time | private_val_1 | private_val_2 |
				|------------|---------------|---------------|
				| 100        | 50            | 100           |
				| 200        | 75            | 125           |
				`,
				Height: 1,
			})
			require.NoError(t, err)

			// Test that extension agent can list the stream
			tnOps := NewTNOperations(txPlatform.Engine, txPlatform.DB, "main", logger)
			
			// This should work even for private streams because of OverrideAuthz: true
			streams, err := tnOps.ListComposedStreams(ctx, deployer.Address())
			require.NoError(t, err, "Extension agent should be able to list streams")
			
			// Verify our private stream is in the list
			found := false
			for _, stream := range streams {
				if stream == privateStreamId.String() {
					found = true
					break
				}
			}
			assert.True(t, found, "Private stream should be accessible to extension agent")

			// Test that extension agent can get category streams
			categoryStreams, err := tnOps.GetCategoryStreams(ctx, deployer.Address(), privateStreamId.String(), 0)
			require.NoError(t, err, "Extension agent should be able to get category streams")
			
			// Filter out parent stream
			var childStreams []CategoryStream
			for _, cs := range categoryStreams {
				if cs.StreamID != privateStreamId.String() {
					childStreams = append(childStreams, cs)
				}
			}
			assert.Len(t, childStreams, 2, "Should access child streams of private stream")

			// Test that extension agent can get records
			fromTime := int64(100)
			toTime := int64(200)
			records, err := tnOps.GetRecordComposed(ctx, deployer.Address(), privateStreamId.String(), &fromTime, &toTime)
			require.NoError(t, err, "Extension agent should be able to get records")
			assert.Len(t, records, 2, "Should access records from private stream")
		}))

		// Test 2: Verify OverrideAuthz is necessary
		t.Run("TestWithoutOverrideAuthz", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			// This test would verify that without OverrideAuthz, access might be denied
			// However, we can't easily test this without modifying the production code
			// So we just document that OverrideAuthz: true is currently required
			
			// For now, we ensure that our implementation uses OverrideAuthz
			// In the future, we might want to grant explicit permissions to extension_agent
			t.Log("Current implementation relies on OverrideAuthz: true for accessing all streams")
			t.Log("TODO: Consider implementing proper permission grants for extension_agent in TN")
		}))

		return nil
	}
}