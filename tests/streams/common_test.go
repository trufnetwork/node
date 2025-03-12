package tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	kwilTypes "github.com/kwilteam/kwil-db/core/types"
	kwilTesting "github.com/kwilteam/kwil-db/testing"

	"github.com/trufnetwork/node/internal/migrations"

	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	trufTypes "github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

var defaultDeployer = util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")

var (
	primitiveStreamLocator = trufTypes.StreamLocator{
		StreamId:     util.GenerateStreamId("primitive_stream_test"),
		DataProvider: defaultDeployer,
	}

	composedStreamLocator = trufTypes.StreamLocator{
		StreamId:     util.GenerateStreamId("composed_stream_test"),
		DataProvider: defaultDeployer,
	}

	primitiveStreamInfo = setup.StreamInfo{
		Locator: primitiveStreamLocator,
		Type:    setup.ContractTypePrimitive,
	}

	composedStreamInfo = setup.StreamInfo{
		Locator: composedStreamLocator,
		Type:    setup.ContractTypeComposed,
	}
)

func TestCOMMON03DisableMetadata(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "disable_metadata",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testDisableMetadata(t, primitiveStreamInfo),
			testDisableMetadata(t, composedStreamInfo),
		},
	}, testutils.GetTestOptions())
}

func testDisableMetadata(t *testing.T, streamInfo setup.StreamInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, defaultDeployer.Bytes())
		// Set up and initialize the contract
		err := setup.CreateStream(ctx, platform, streamInfo)
		if err != nil {
			return errors.Wrapf(err, "failed to create stream")
		}

		// Insert metadata
		key := "temp_key"
		value := "temporary value"
		valType := "string"

		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  streamInfo.Locator,
			Key:      key,
			Value:    value,
			ValType:  valType,
		})
		if err != nil {
			return errors.Wrapf(err, "Failed to insert metadata key %s", key)
		}

		// Retrieve the metadata to get the row_id
		result, err := procedure.GetMetadata(ctx, procedure.GetMetadataInput{
			Platform: platform,
			Locator:  streamInfo.Locator,
			Key:      key,
		})
		if err != nil {
			return errors.Wrapf(err, "Failed to get metadata key %s", key)
		}
		rowID := result[0].(*kwilTypes.UUID)

		// Disable the metadata
		err = procedure.DisableMetadata(ctx, procedure.DisableMetadataInput{
			Platform: platform,
			Locator:  streamInfo.Locator,
			RowID:    rowID,
		})
		if err != nil {
			return errors.Wrap(err, "Failed to disable metadata")
		}

		// Attempt to retrieve the disabled metadata
		result, err = procedure.GetMetadata(ctx, procedure.GetMetadataInput{
			Platform: platform,
			Locator:  streamInfo.Locator,
			Key:      key,
		})
		assert.NoError(t, err, "Expect no error when retrieving disabled metadata")
		assert.Equal(t, 0, len(result), "Should not be able to retrieve disabled metadata")

		return nil
	}
}

func TestCOMMON02ReadOnlyMetadataCannotBeModified(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "readonly_metadata_cannot_be_modified",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testReadOnlyMetadataCannotBeModified(t, primitiveStreamInfo),
			testReadOnlyMetadataCannotBeModified(t, composedStreamInfo),
		},
	}, testutils.GetTestOptions())
}

func testReadOnlyMetadataCannotBeModified(t *testing.T, streamInfo setup.StreamInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, defaultDeployer.Bytes())
		// Set up and initialize the contract
		err := setup.CreateStream(ctx, platform, streamInfo)
		if err != nil {
			return errors.Wrap(err, "failed to create stream for read-only metadata test")
		}

		readonlyKeys := []string{"stream_owner", "readonly_key", "taxonomy_version"}

		for _, key := range readonlyKeys {
			// Attempt to insert metadata with a read-only key
			err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
				Platform: platform,
				Locator:  streamInfo.Locator,
				Key:      key,
				Value:    "modified",
				ValType:  "string",
			})
			assert.Error(t, err, "Should not be able to modify read-only metadata")

			// Attempt to disable read-only metadata
			result, err := procedure.GetMetadata(ctx, procedure.GetMetadataInput{
				Platform: platform,
				Locator:  streamInfo.Locator,
				Key:      "stream_owner",
			})
			if err != nil {
				return errors.Wrap(err, "Failed to get read-only metadata")
			}
			rowID := result[0].(*kwilTypes.UUID)

			err = procedure.DisableMetadata(ctx, procedure.DisableMetadataInput{
				Platform: platform,
				Locator:  streamInfo.Locator,
				RowID:    rowID,
			})
			assert.Error(t, err, "Should not be able to disable read-only metadata")
		}

		return nil
	}
}

// func TestVisibilitySettings(t *testing.T) {
// 	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
// 		Name: "visibility_settings",
// 		FunctionTests: []kwilTesting.TestFunc{
// 			testVisibilitySettings(t, primitiveContractInfo),
// 			testVisibilitySettings(t, composedContractInfo),
// 		},
// 	})
// }

// func testVisibilitySettings(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		// Set up and initialize the contract
// 		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
// 			return err
// 		}
// 		dbid := setup.GetDBID(contractInfo)

// 		// Change read_visibility to private (1)
// 		err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
// 			Platform: platform,
// 			Deployer: contractInfo.Deployer,
// 			DBID:     dbid,
// 			Key:      "read_visibility",
// 			Value:    "1",
// 			ValType:  "int",
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "Failed to change read_visibility")
// 		}

// 		// Change deployer to a non-owner
// 		nonOwner := util.Unsafe_NewEthereumAddressFromString("0xcccccccccccccccccccccccccccccccccccccccc")
// 		platform.Deployer = nonOwner.Bytes()

// 		// Attempt to read data
// 		canRead, err := procedure.CheckReadPermissions(ctx, procedure.CheckReadPermissionsInput{
// 			Platform: platform,
// 			Deployer: contractInfo.Deployer,
// 			DBID:     dbid,
// 			Wallet:   nonOwner.Address(),
// 		})
// 		assert.False(t, canRead, "Non-owner should not be able to read when read_visibility is private")
// 		assert.NoError(t, err, "Error should not be returned when checking read permissions")

// 		return nil
// 	}
// }
