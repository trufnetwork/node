package tests

import (
	"context"
	"fmt"
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

var (
	streamName = "primitive_stream"
	streamId   = util.GenerateStreamId(streamName)
	deployer   = util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
)

func TestDatabaseSize(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "database_size_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithTestDatabaseSizeSetup(testDatabaseSize(t)),
		},
	}, testutils.GetTestOptions())
}

func WithTestDatabaseSizeSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set the platform signer
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Create the composed stream
		err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Locator: types.StreamLocator{
				StreamId:     streamId,
				DataProvider: deployer,
			},
			Type: setup.ContractTypePrimitive,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up stream")
		}

		return testFn(ctx, platform)
	}
}

func testDatabaseSize(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamLocator := types.StreamLocator{
			StreamId:     streamId,
			DataProvider: deployer,
		}

		result, err := procedure.GetDatabaseSize(ctx, procedure.GetDatabaseSizeInput{
			Platform:      platform,
			Locator: streamLocator,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting database size")
		}

		assert.Equal(t, int64(2118), result, fmt.Sprintf("Actual: %d, Expected: 2118", result))

		return nil
	}
}