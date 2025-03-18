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
	AGGR05: For a single taxonomy version, there can't be duplicated child stream definitions.

	bare minimum test:
		composed stream with a child stream reference (without actually deploying it)
		we try to insert a duplicate child stream definition
		expect an error
*/

// TestAGGR05_NoDuplicateChildStreams tests AGGR05: For a single taxonomy version, there can't be duplicated child stream definitions.
func TestAGGR05_NoDuplicateChildStreams(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "aggr05_no_duplicate_child_streams_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testAGGR05_NoDuplicateChildStreams(t),
		},
	}, testutils.GetTestOptions())
}

func testAGGR05_NoDuplicateChildStreams(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())

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

		// Create a stream ID reference (without actually deploying the stream)
		stream1 := util.GenerateStreamId("stream1")

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

		// We expect an error because duplicate child streams are not allowed
		assert.Error(t, err, "Expected error when adding duplicate child stream")
		assert.Contains(t, err.Error(), "violates unique constraint")

		return nil
	}
}
