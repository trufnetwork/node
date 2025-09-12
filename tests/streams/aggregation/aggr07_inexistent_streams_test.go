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
	AGGR07: When setting a taxonomy with non-existent streams, appropriate errors should be returned.

	Test cases:
	1. Setting a taxonomy with a non-existent primitive stream should return an error
	2. Setting a taxonomy with a non-existent composed stream should return an error
*/

// TestAGGR07_InexistentStreamsRejected tests that setting taxonomies with non-existent stream references results in errors
func TestAGGR07_InexistentStreamsRejected(t *testing.T) {
	cacheConfig := testutils.SimpleCache("0x0000000000000000000000000000000000000123", "*")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "aggr07_inexistent_streams_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			wrapTestWithCacheModes(t, "AGGR07_InexistentStreams", testAGGR07_InexistentStreams),
		},
	}, testutils.GetTestOptionsWithCache(cacheConfig))
}

func testAGGR07_InexistentStreams(t *testing.T, useCache bool) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Test 1: Non-existent primitive stream

		// Create a composed stream to use for the test
		composedStreamId := util.GenerateStreamId("composed_stream_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())
		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// Setup the composed stream
		err = setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform,
			StreamId: composedStreamId,
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		// Generate a stream ID for a non-existent primitive stream
		nonExistentPrimitiveId := util.GenerateStreamId("nonexistent_primitive")

		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: deployer,
		}

		// Try to set a taxonomy with a non-existent primitive stream
		// This should now fail immediately since we check for stream existence
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{nonExistentPrimitiveId.String()},
			Weights:       []string{"1.0"},
			StartTime:     nil,
		})

		// We expect an error when setting taxonomy because the primitive stream doesn't exist
		assert.Error(t, err, "Expected error when setting taxonomy with non-existent primitive stream")
		assert.Contains(t, err.Error(), "child stream does not exist", "Error should indicate the child stream was not found")

		// Test 2: Non-existent composed stream

		rootComposedStreamId := util.GenerateStreamId("root_composed_stream_test")
		deployer, err = util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())
		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// Setup the root composed stream
		err = setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform,
			StreamId: rootComposedStreamId,
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up root composed stream")
		}

		// Generate a stream ID for a non-existent composed stream
		nonExistentComposedId := util.GenerateStreamId("nonexistent_composed")

		// Create StreamLocator for the root composed stream
		rootStreamLocator := types.StreamLocator{
			StreamId:     rootComposedStreamId,
			DataProvider: deployer,
		}

		// Try to set a taxonomy with a non-existent composed stream
		// This should now fail immediately since we check for stream existence
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: rootStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{nonExistentComposedId.String()},
			Weights:       []string{"1.0"},
			StartTime:     nil,
		})

		// We expect an error when setting taxonomy because the composed stream doesn't exist
		assert.Error(t, err, "Expected error when setting taxonomy with non-existent composed stream")
		assert.Contains(t, err.Error(), "child stream does not exist", "Error should indicate the child stream was not found")

		return nil
	}
}
