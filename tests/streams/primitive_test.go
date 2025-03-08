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

const primitiveStreamName = "primitive_stream_000000000000001"

var primitiveStreamId = util.GenerateStreamId(primitiveStreamName)

func TestPrimitiveStream(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "primitive_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testPRIMITIVE01_DataInsertion(t),
		},
	}, testutils.GetTestOptions())
}

func testPRIMITIVE01_DataInsertion(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		validAddress := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000001")
		platform = procedure.WithSigner(platform, validAddress.Bytes())
		streamLocator := types.StreamLocator{
			StreamId:     primitiveStreamId,
			DataProvider: validAddress,
		}
		_, err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: streamLocator,
		})
		if err != nil {
			return errors.Wrap(err, "valid address should be accepted")
		}
		assert.NoError(t, err, "valid address should be accepted")

		// Setup initial data
		err = setup.ExecuteInsertRecord(ctx, platform, streamLocator, setup.InsertRecordInput{
			EventTime: 1612137600,
			Value:     1,
		}, 0)
		if err != nil {
			return errors.Wrap(err, "error inserting initial data")
		}
		assert.NoError(t, err, "error inserting initial data")

		return nil
	}
}

func WithPrimitiveTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		platform.Deployer = deployer.Bytes()

		// Setup initial data
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: primitiveStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 1          | 1     |
			| 2          | 2     |
			| 3          | 4     |
			| 4          | 5     |
			| 5          | 3     |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up primitive stream")
		}

		// Run the actual test function
		return testFn(ctx, platform)
	}
}
