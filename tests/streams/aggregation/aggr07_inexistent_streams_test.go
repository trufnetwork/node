package tests

import (
	"context"
	"testing"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

/*
	AGGR07: When querying a composed stream with non-existent streams in its taxonomy, appropriate errors should be returned.

	Test cases:
	1. Querying a composed stream with a non-existent primitive stream in its taxonomy should return an error
	2. Querying a composed stream with a non-existent composed stream in its taxonomy should return an error
*/

// TestAGGR07_InexistentStreamsRejected tests that querying composed streams with non-existent stream references results in errors
func TestAGGR07_InexistentStreamsRejected(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "aggr07_inexistent_streams_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testAGGR07_NonExistentPrimitive(t),
			testAGGR07_NonExistentComposed(t),
		},
	}, testutils.GetTestOptions())
}

// testAGGR07_NonExistentPrimitive tests that querying a composed stream with a non-existent primitive stream in its taxonomy returns an error
func testAGGR07_NonExistentPrimitive(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		/*
		   Test Structure:

		   ComposedStream
		        ↓
		   NonExistentPrimitiveStream (doesn't actually exist)

		   Expected behavior:
		   1. Creating the ComposedStream succeeds
		   2. Setting a taxonomy that references a non-existent primitive stream succeeds
		   3. Querying the ComposedStream fails with an error since it references a non-existent stream
		*/

		// Create a composed stream to use for the test
		composedStreamId := util.GenerateStreamId("composed_stream_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())

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

		// Set a taxonomy with a non-existent primitive stream
		// This should succeed since we're only registering the taxonomy, not querying it
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{nonExistentPrimitiveId.String()},
			Weights:       []string{"1.0"},
			StartTime:     nil,
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy with non-existent primitive stream")
		}

		// Now try to query the composed stream
		fromTime := int64(1)
		toTime := int64(3)
		_, err = procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &fromTime,
			ToTime:        &toTime,
			Height:        1,
		})

		// We expect an error when querying because the primitive stream doesn't exist
		assert.Error(t, err, "Expected error when querying composed stream with non-existent primitive stream")
		assert.Contains(t, err.Error(), "streams missing for stream", "Error should indicate the stream was not found")

		return nil
	}
}

// testAGGR07_NonExistentComposed tests that querying a composed stream with a non-existent composed stream in its taxonomy returns an error
func testAGGR07_NonExistentComposed(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		/*
		   Test Structure:

		   RootComposedStream
		        ↓
		   NonExistentComposedStream (doesn't actually exist)

		   Expected behavior:
		   1. Creating the RootComposedStream succeeds
		   2. Setting a taxonomy that references a non-existent composed stream succeeds
		   3. Querying the RootComposedStream fails with an error since it references a non-existent stream
		*/

		// Create a composed stream to use for the test
		rootComposedStreamId := util.GenerateStreamId("root_composed_stream_test")
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())

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

		// Set a taxonomy with a non-existent composed stream
		// This should succeed since we're only registering the taxonomy, not querying it
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: rootStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{nonExistentComposedId.String()},
			Weights:       []string{"1.0"},
			StartTime:     nil,
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy with non-existent composed stream")
		}

		// Now try to query the composed stream
		fromTime := int64(1)
		toTime := int64(3)
		_, err = procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: rootStreamLocator,
			FromTime:      &fromTime,
			ToTime:        &toTime,
			Height:        1,
		})

		// We expect an error when querying because the composed stream doesn't exist
		assert.Error(t, err, "Expected error when querying composed stream with non-existent composed stream")
		assert.Contains(t, err.Error(), "streams missing for stream", "Error should indicate the stream was not found")

		return nil
	}
}
