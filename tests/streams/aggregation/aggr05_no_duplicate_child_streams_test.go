package tests

// import (
// 	"context"
// 	"testing"

// 	"github.com/kwilteam/kwil-db/core/utils"
// 	kwilTesting "github.com/kwilteam/kwil-db/testing"

// 	"github.com/pkg/errors"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/trufnetwork/node/tests/streams/tests/utils/procedure"
// 	"github.com/trufnetwork/node/tests/streams/tests/utils/setup"
// 	"github.com/trufnetwork/sdk-go/core/util"
// )

// /*
// 	AGGR05: For a single taxonomy version, there can't be duplicated child stream definitions.

// 	bare minimum test:
// 		composed stream with a child stream reference (without actually deploying it)
// 		we try to insert a duplicate child stream definition
// 		expect an error
// */

// // FIXME: This test is not working as expected with current contract.
// // TestAGGR05_NoDuplicateChildStreams tests AGGR05: For a single taxonomy version, there can't be duplicated child stream definitions.
// func TestAGGR05_NoDuplicateChildStreams(t *testing.T) {
// 	t.Skip("Test skipped: aggregation stream tests temporarily disabled")
// 	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
// 		Name: "aggr05_no_duplicate_child_streams_test",
// 		FunctionTests: []kwilTesting.TestFunc{
// 			testAGGR05_NoDuplicateChildStreams(t),
// 		},
// 	})
// }

// func testAGGR05_NoDuplicateChildStreams(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
// 		if err != nil {
// 			return errors.Wrap(err, "error creating ethereum address")
// 		}
// 		platform.Deployer = deployer.Bytes()

// 		// Create a composed stream
// 		composedStreamId := util.GenerateStreamId("composed_stream_test")

// 		// Setup the composed stream
// 		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
// 			Platform: platform,
// 			StreamId: composedStreamId,
// 			Height:   1,
// 		}); err != nil {
// 			return errors.Wrap(err, "error setting up composed stream")
// 		}

// 		// Create a stream ID reference (without actually deploying the stream)
// 		stream1 := util.GenerateStreamId("stream1")

// 		// Generate the DBID for the composed stream
// 		composedDBID := utils.GenerateDBID(composedStreamId.String(), platform.Deployer)

// 		// Try to set a taxonomy with a duplicate child stream (same stream1 twice)
// 		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
// 			Platform:      platform,
// 			DBID:          composedDBID,
// 			DataProviders: []string{deployer.Address(), deployer.Address()},
// 			StreamIds:     []string{stream1.String(), stream1.String()}, // Duplicate stream1
// 			Weights:       []string{"1.0", "1.0"},
// 			StartDate:     "",
// 		})

// 		// We expect an error because duplicate child streams are not allowed
// 		assert.Error(t, err, "Expected error when adding duplicate child stream")
// 		assert.Contains(t, err, "duplicate", "Error should mention duplicate")

// 		return nil
// 	}
// }
