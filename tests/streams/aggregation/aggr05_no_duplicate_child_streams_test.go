package tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

/*
	AGGR05: For a single taxonomy version, there can't be duplicated child stream definitions.

	bare minimum test:
		composed stream with a child stream reference (without actually deploying it)
		we try to insert a duplicate child stream definition
		expect an error
*/

// TestAGGR05_NoDuplicateChildStreams tests AGGR05: For a single taxonomy version, there can't be duplicated child stream definitions.
func TestAGGR05_NoDuplicateChildStreams(t *testing.T) {
	cacheConfig := testutils.SimpleCache("0x0000000000000000000000000000000000000123", "*")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "aggr05_no_duplicate_child_streams_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			wrapTestWithCacheModes(t, "AGGR05_NoDuplicateChildStreams", testAGGR05_NoDuplicateChildStreams),
		},
	}, testutils.GetTestOptionsWithCache(cacheConfig))
}

func testAGGR05_NoDuplicateChildStreams(t *testing.T, useCache bool) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())
		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// Create a composed stream
		composedStreamId := util.GenerateStreamId("composed_stream_test")

		// Setup the composed stream
		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform,
			StreamId: composedStreamId,
			Height:   1,
		}); err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		// Create a stream ID reference and deploy it (required for testing duplicates)
		stream1 := util.GenerateStreamId("stream1")

		// Deploy stream1 so it exists (required for testing duplicates)
		if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: stream1,
			Height:   1,
			MarkdownData: `
				| event_time | value |
				| ---------- | ----- |
				| 1          | 5     |
			`,
		}); err != nil {
			return errors.Wrap(err, "error setting up child stream 1")
		}

		// Set up cache (only when useCache is true)
		if useCache {
			// Note: This test doesn't actually need cache refresh since it's testing constraint violations
			// The cache setup is handled by the test framework
		}

		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: deployer,
		}

		// Try to set a taxonomy with a duplicate child stream (same stream1 twice)
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address(), deployer.Address()},
			StreamIds:     []string{stream1.String(), stream1.String()}, // Duplicate stream1
			Weights:       []string{"1.0", "1.0"},
			StartTime:     nil,
		})

		// We expect a DB uniqueness error (duplicates should be enforced at DB level)
		assert.Error(t, err, "Expected error when adding duplicate child stream")
		assert.Contains(t, err.Error(), "violates unique constraint")

		return nil
	}
}
