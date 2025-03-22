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
	primitiveStreamLocator = types.StreamLocator{
		StreamId:     util.GenerateStreamId("primitive_stream_test"),
		DataProvider: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123"),
	}

	composedStreamLocator = types.StreamLocator{
		StreamId:     util.GenerateStreamId("composed_stream_test"),
		DataProvider: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000456"),
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

// TestAUTH01_StreamOwnership tests AUTH01: Stream ownership is clearly defined and can be transferred to another valid wallet.
func TestAUTH01_StreamOwnership(t *testing.T) {
	// Test valid ownership transfer
	t.Run("ValidOwnershipTransfer", func(t *testing.T) {
		kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
			Name:        "stream_ownership_transfer_AUTH01",
			SeedScripts: migrations.GetSeedScriptPaths(),
			FunctionTests: []kwilTesting.TestFunc{
				testStreamOwnershipTransfer(t),
			},
		}, testutils.GetTestOptions())
	})

	//Test invalid address handling
	t.Run("InvalidAddressHandling", func(t *testing.T) {
		kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
			Name:        "invalid_address_ownership_transfer_AUTH01",
			SeedScripts: migrations.GetSeedScriptPaths(),
			FunctionTests: []kwilTesting.TestFunc{
				testInvalidAddressOwnershipTransfer(t),
			},
		}, testutils.GetTestOptions())
	})
}

func testStreamOwnershipTransfer(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		platform.Deployer = deployer.Bytes()
		streamId := util.GenerateStreamId("ownership_transfer_test")
		streamLocator := types.StreamLocator{
			StreamId:     streamId,
			DataProvider: deployer,
		}
		// Set up and initialize the contract
		if err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Locator: streamLocator,
			Type:    setup.ContractTypeComposed, // We can use composed for all since we're not testing actual values
		}); err != nil {
			return errors.Wrapf(err, "failed to setup and initialize contract %s for ownership transfer test", streamLocator.StreamId.String())
		}

		// Transfer ownership
		newOwner := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		err := procedure.TransferStreamOwnership(ctx, procedure.TransferStreamOwnershipInput{
			Platform: platform,
			Locator:  streamLocator,
			NewOwner: newOwner,
			Height:   0,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to transfer ownership of contract %s to %s", streamId.String(), newOwner)
		}

		// Attempt to perform an owner-only action with the old owner
		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  streamLocator,
			Key:      "new_key",
			Value:    "new_value",
			ValType:  "string",
			Height:   0,
		})
		assert.Error(t, err, "Old owner should not be able to insert metadata after ownership transfer")

		// Change platform deployer to the new owner
		newOwnerAddress := util.Unsafe_NewEthereumAddressFromString(newOwner)
		platform.Deployer = newOwnerAddress.Bytes()

		// Attempt to perform an owner-only action with the new owner
		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  streamLocator,
			Key:      "new_key",
			Value:    "new_value",
			ValType:  "string",
			Height:   0,
		})
		assert.NoError(t, err, "New owner should be able to insert metadata after ownership transfer")

		return nil
	}
}

func testInvalidAddressOwnershipTransfer(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		streamId := util.GenerateStreamId("ownership_transfer_test")
		streamLocator := types.StreamLocator{
			StreamId:     streamId,
			DataProvider: deployer,
		}
		// Set up and initialize the contract
		if err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Locator: streamLocator,
			Type:    setup.ContractTypeComposed, // We can use composed for all since we're not testing actual values
		}); err != nil {
			return errors.Wrapf(err, "failed to setup and initialize contract %s for invalid address test", streamLocator.StreamId.String())
		}

		// Attempt to transfer ownership to an invalid address
		invalidAddress := "invalid_address"
		err := procedure.TransferStreamOwnership(ctx, procedure.TransferStreamOwnershipInput{
			Platform: platform,
			Locator:  streamLocator,
			NewOwner: invalidAddress,
			Height:   0,
		})
		assert.Error(t, err, "Should not accept invalid Ethereum address")

		// Attempt to transfer ownership to an address that does not match ethereum address regex pattern
		invalidAddress = "0x0000000000000000000000000000000000000ZZZ"
		err = procedure.TransferStreamOwnership(ctx, procedure.TransferStreamOwnershipInput{
			Platform: platform,
			Locator:  streamLocator,
			NewOwner: invalidAddress,
			Height:   0,
		})
		assert.Error(t, err, "Should not accept invalid Ethereum address that does not match regex pattern")

		return nil
	}
}

// TestAUTH02_ReadPermissions tests AUTH02: A stream owner can control who is allowed to read data from its stream
func TestAUTH02_ReadPermissions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "read_permission_control_AUTH02",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testReadPermissionControl(t, primitiveStreamInfo),
			testReadPermissionControl(t, composedStreamInfo),
		},
	}, testutils.GetTestOptions())
}

func testReadPermissionControl(t *testing.T, streamInfo setup.StreamInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, streamInfo.Locator.DataProvider.Bytes())
		// Set up and initialize the contract
		err := setup.CreateStream(ctx, platform, streamInfo)
		if err != nil {
			return errors.Wrapf(err, "failed to create stream for read permission test")
		}

		// Initially, anyone should be able to read (public visibility)
		nonOwnerUnauthorized := util.Unsafe_NewEthereumAddressFromString("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
		nonOwnerAuthorized := util.Unsafe_NewEthereumAddressFromString("0xffffffffffffffffffffffffffffffffffffffff")

		// Helper function to check both single and all substreams read permissions
		checkBothPermissions := func(wallet util.EthereumAddress, expectedCanRead bool, scenario string) error {
			// Check single stream permissions
			canRead, err := procedure.CheckReadPermissions(ctx, procedure.CheckReadPermissionsInput{
				Platform: platform,
				Locator:  streamInfo.Locator,
				Wallet:   wallet.Address(),
			})
			if err != nil {
				return errors.Wrapf(err, "failed to check single stream read permissions for %s", scenario)
			}
			assert.Equal(t, expectedCanRead, canRead,
				fmt.Sprintf("%s should %s able to read private stream (single)",
					scenario, expectedVerb(expectedCanRead)))

			// Check all substreams permissions
			canReadAll, err := procedure.CheckReadAllPermissions(ctx, procedure.CheckReadAllPermissionsInput{
				Platform: platform,
				Locator:  streamInfo.Locator,
				Wallet:   wallet.Address(),
			})
			if err != nil {
				return errors.Wrapf(err, "failed to check all substreams read permissions for %s", scenario)
			}
			assert.Equal(t, expectedCanRead, canReadAll,
				fmt.Sprintf("%s should %s able to read private stream (all)",
					scenario, expectedVerb(expectedCanRead)))

			return nil
		}

		// Add non-owner authorized to read whitelist
		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  streamInfo.Locator,
			Key:      "allow_read_wallet",
			Value:    nonOwnerAuthorized.Address(),
			ValType:  "ref",
			Height:   1,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to add wallet %s to read whitelist for stream",
				nonOwnerAuthorized.Address())
		}

		// Test with public visibility (default)
		if err := checkBothPermissions(nonOwnerUnauthorized, true, "unauthorized wallet with public visibility"); err != nil {
			return err
		}

		// Change read_visibility to private (1)
		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  streamInfo.Locator,
			Key:      "read_visibility",
			Value:    "1", // 1 = private
			ValType:  "int",
			Height:   2,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to change read_visibility to private for stream")
		}

		// Test with private visibility
		if err := checkBothPermissions(nonOwnerUnauthorized, false, "unauthorized wallet"); err != nil {
			return err
		}

		if err := checkBothPermissions(nonOwnerAuthorized, true, "authorized wallet"); err != nil {
			return err
		}

		return nil
	}
}

// Helper function to return the appropriate verb based on expected permission
func expectedVerb(canRead bool) string {
	if canRead {
		return "be"
	}
	return "not be"
}

// TestAUTH02_NestedReadPermissions tests read permissions across a chain of composed streams
func TestAUTH02_NestedReadPermissions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "nested_read_permission_control_AUTH02",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testNestedReadPermissionControl(t),
		},
	}, testutils.GetTestOptions())
}

func testNestedReadPermissionControl(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create addresses for the test
		dataProvider := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000001")
		authorizedWallet := util.Unsafe_NewEthereumAddressFromString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		unauthorizedWallet := util.Unsafe_NewEthereumAddressFromString("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

		// Create a slice of streams in our hierarchy
		streamLocators := []types.StreamLocator{
			{ // Primitive stream (index 0)
				StreamId:     util.GenerateStreamId("nested_primitive_test"),
				DataProvider: dataProvider,
			},
			{ // First-level composed stream (index 1)
				StreamId:     util.GenerateStreamId("nested_composed_level1_test"),
				DataProvider: dataProvider,
			},
			{ // Second-level composed stream (index 2)
				StreamId:     util.GenerateStreamId("nested_composed_level2_test"),
				DataProvider: dataProvider,
			},
		}

		// Use a slice of stream types to match the streamLocators
		streamTypes := []setup.ContractType{
			setup.ContractTypePrimitive,
			setup.ContractTypeComposed,
			setup.ContractTypeComposed,
		}

		// 1. Create all streams
		platform = procedure.WithSigner(platform, dataProvider.Bytes())
		for i, locator := range streamLocators {
			err := setup.CreateStream(ctx, platform, setup.StreamInfo{
				Locator: locator,
				Type:    streamTypes[i],
			})
			if err != nil {
				return errors.Wrapf(err, "failed to create stream %s for nested test", locator.StreamId.String())
			}
		}

		// 2. Set up the taxonomy chain
		// Link first-level composed to primitive
		err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: streamLocators[1],                                  // First-level composed
			DataProviders: []string{streamLocators[0].DataProvider.Address()}, // Primitive
			StreamIds:     []string{streamLocators[0].StreamId.String()},
			Weights:       []string{"1.0"},
			StartTime:     nil,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to set taxonomy for first-level composed stream")
		}

		// Link second-level composed to first-level composed
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: streamLocators[2],                                  // Second-level composed
			DataProviders: []string{streamLocators[1].DataProvider.Address()}, // First-level composed
			StreamIds:     []string{streamLocators[1].StreamId.String()},
			Weights:       []string{"1.0"},
			StartTime:     nil,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to set taxonomy for second-level composed stream")
		}

		// 3. Add authorized wallet to primitive stream's read whitelist
		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  streamLocators[0], // Primitive
			Key:      "allow_read_wallet",
			Value:    authorizedWallet.Address(),
			ValType:  "ref",
			Height:   1,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to add wallet %s to read whitelist",
				authorizedWallet.Address())
		}

		// 4. Set primitive stream's read visibility to private
		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  streamLocators[0], // Primitive
			Key:      "read_visibility",
			Value:    "1", // 1 = private
			ValType:  "int",
			Height:   2,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to set read_visibility to private")
		}

		// 5. Test scenarios with a helper function
		checkReadPermission := func(locator types.StreamLocator, wallet util.EthereumAddress, expectCanRead bool, description string) error {
			canRead, err := procedure.CheckReadAllPermissions(ctx, procedure.CheckReadAllPermissionsInput{
				Platform: platform,
				Locator:  locator,
				Wallet:   wallet.Address(),
			})
			if err != nil {
				return errors.Wrapf(err, "failed to check read permissions for %s on stream %s",
					wallet.Address(), locator.StreamId.String())
			}
			assert.Equal(t, expectCanRead, canRead, description)
			return nil
		}

		// Test each stream with both authorized and unauthorized wallet
		for i, locator := range streamLocators {
			streamName := []string{"primitive", "first-level composed", "second-level composed"}[i]

			// Test authorized wallet (should be able to read all streams)
			err = checkReadPermission(
				locator,
				authorizedWallet,
				true,
				fmt.Sprintf("authorized wallet should be able to read %s stream", streamName),
			)
			if err != nil {
				return err
			}

			// Test unauthorized wallet (should not be able to read any stream due to permission inheritance)
			err = checkReadPermission(
				locator,
				unauthorizedWallet,
				false,
				fmt.Sprintf("unauthorized wallet should not be able to read %s stream", streamName),
			)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

// TestAUTH03_WritePermissions tests AUTH03: The stream owner can control which wallets are allowed to insert data into the stream.
// func TestAUTH03_WritePermissions(t *testing.T) {
// 	t.Skip("Test skipped: auth stream tests temporarily disabled")
// 	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
// 		Name: "write_permission_control_AUTH03",
// 		FunctionTests: []kwilTesting.TestFunc{
// 			testWritePermissionControl(t, primitiveStreamInfo),
// 			testWritePermissionControl(t, composedStreamInfo),
// 		},
// 	}, testutils.GetTestOptions())
// }

// func testWritePermissionControl(t *testing.T, streamInfo setup.StreamInfo) kwilTesting.TestFunc {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		// Set up and initialize the contract
// 		_, err := setup.CreateStream(ctx, platform, streamInfo)
// 		if err != nil {
// 			return errors.Wrapf(err, "failed to create stream for write permission test")
// 		}

// 		// Create a non-owner wallet
// 		nonOwner := util.Unsafe_NewEthereumAddressFromString("0xdddddddddddddddddddddddddddddddddddddddd")

// 		// Check if non-owner can write (should be false by default)
// 		canWrite, err := procedure.CheckWritePermissions(ctx, procedure.CheckWritePermissionsInput{
// 			Platform:     platform,
// 			Deployer:     streamInfo.Locator.DataProvider,
// 			StreamId:     streamInfo.Locator.StreamId.String(),
// 			DataProvider: streamInfo.Locator.DataProvider.Address(),
// 			Wallet:       nonOwner.Address(),
// 		})
// 		if err != nil {
// 			return errors.Wrapf(err, "failed to check write permissions")
// 		}
// 		assert.Equal(t, false, canWrite, "non-owner should not be able to write by default")

// 		// Add non-owner to write whitelist
// 		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
// 			Platform:     platform,
// 			Deployer:     streamInfo.Locator.DataProvider,
// 			StreamId:     streamInfo.Locator.StreamId.String(),
// 			DataProvider: streamInfo.Locator.DataProvider.Address(),
// 			Key:          "allow_write_wallet",
// 			Value:        nonOwner.Address(),
// 			ValType:      "ref",
// 		})
// 		if err != nil {
// 			return errors.Wrapf(err, "failed to add wallet %s to write whitelist for stream",
// 				nonOwner.Address())
// 		}

// 		// Verify non-owner can now write
// 		canWrite, err = procedure.CheckWritePermissions(ctx, procedure.CheckWritePermissionsInput{
// 			Platform:     platform,
// 			Deployer:     streamInfo.Locator.DataProvider,
// 			StreamId:     streamInfo.Locator.StreamId.String(),
// 			DataProvider: streamInfo.Locator.DataProvider.Address(),
// 			Wallet:       nonOwner.Address(),
// 		})
// 		if err != nil {
// 			return errors.Wrapf(err, "failed to check write permissions")
// 		}
// 		// TODO: right now, composed contract doesn't have this procedure to check write permission.
// 		//   however, in the next iteration it should be implemented.
// 		assert.Equal(t, true, canWrite, "whitelisted wallet should be able to write")

// 		return nil
// 	}
// }

// // TestAUTH04_ComposePermissions tests AUTH04: The stream owner can control which streams are allowed to compose from the stream.
func TestAUTH04_ComposePermissions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "compose_permission_control_AUTH04",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testComposePermissionControl(t, primitiveStreamInfo),
			testComposePermissionControl(t, composedStreamInfo),
		},
	}, testutils.GetTestOptions())
}

func testComposePermissionControl(t *testing.T, contractInfo setup.StreamInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up the platform.Deployer to the stream owner
		platform.Deployer = contractInfo.Locator.DataProvider.Bytes()

		// Set up and initialize the primary contract
		if err := setup.CreateStream(ctx, platform, contractInfo); err != nil {
			return errors.Wrapf(err, "failed to setup and initialize primary contract %s for compose permission test", contractInfo.Locator.StreamId.String())
		}

		// Set up a upper contract (the one attempting to compose)
		upperStreamInfo := setup.StreamInfo{
			Locator: types.StreamLocator{
				StreamId:     util.GenerateStreamId("upper_stream_test"),
				DataProvider: contractInfo.Locator.DataProvider,
			},
			Type: setup.ContractTypePrimitive,
		}

		if err := setup.CreateStream(ctx, platform, upperStreamInfo); err != nil {
			return errors.Wrapf(err, "failed to setup and initialize upper stream %s for compose permission test",
				upperStreamInfo.Locator.StreamId.String())
		}

		// Set compose_visibility to private (1)
		err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  contractInfo.Locator,
			Key:      "compose_visibility",
			Value:    "1",
			ValType:  "int",
		})
		if err != nil {
			return errors.Wrapf(err, "failed to change compose_visibility to private for contract %s", contractInfo.Locator.StreamId.String())
		}

		// Verify foreign stream cannot compose without permission
		canCompose, err := procedure.CheckComposePermissions(ctx, procedure.CheckComposePermissionsInput{
			Platform:          platform,
			Locator:           contractInfo.Locator,
			ComposingStreamId: upperStreamInfo.Locator.StreamId.String(),
			Height:            0,
		})
		assert.False(t, canCompose, "Upper stream should not be allowed to compose without permission")

		// Grant compose permission to the foreign stream
		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  contractInfo.Locator,
			Key:      "allow_compose_stream",
			Value:    upperStreamInfo.Locator.StreamId.String(),
			ValType:  "ref",
			Height:   0,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to grant compose permission to upper stream %s for contract %s",
				upperStreamInfo.Locator.StreamId.String(), contractInfo.Locator.StreamId.String())
		}

		// Verify upper stream can now compose
		platform.Deployer = upperStreamInfo.Locator.DataProvider.Bytes()
		canCompose, err = procedure.CheckComposePermissions(ctx, procedure.CheckComposePermissionsInput{
			Platform:          platform,
			Locator:           upperStreamInfo.Locator,
			ComposingStreamId: contractInfo.Locator.StreamId.String(),
			Height:            0,
		})
		assert.True(t, canCompose, "Upper stream should be allowed to compose after permission is granted")
		assert.NoError(t, err, "No error expected when composing with permission")

		return nil
	}
}

// TestAUTH04_NestedComposePermissions tests AUTH04:
// The stream owner can control which streams (and their nested substreams) are allowed to compose from the stream.
func TestAUTH04_NestedComposePermissions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "nested_compose_permission_control_AUTH04",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testNestedComposePermissionControl(t),
		},
	}, testutils.GetTestOptions())
}

func testNestedComposePermissionControl(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use a common data provider.
		provider := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000002222")

		// Create a three-level chain: parent -> child -> grandchild.
		parentLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("nested_parent"),
			DataProvider: provider,
		}
		childLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("nested_child"),
			DataProvider: provider,
		}
		grandchildLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("nested_grandchild"),
			DataProvider: provider,
		}

		parentInfo := setup.StreamInfo{
			Locator: parentLocator,
			Type:    setup.ContractTypeComposed,
		}
		childInfo := setup.StreamInfo{
			Locator: childLocator,
			Type:    setup.ContractTypeComposed,
		}
		grandchildInfo := setup.StreamInfo{
			Locator: grandchildLocator,
			Type:    setup.ContractTypePrimitive,
		}

		platform = procedure.WithSigner(platform, provider.Bytes())

		// Create all streams.
		if err := setup.CreateStream(ctx, platform, parentInfo); err != nil {
			return errors.Wrap(err, "failed to create parent")
		}
		if err := setup.CreateStream(ctx, platform, childInfo); err != nil {
			return errors.Wrap(err, "failed to create child")
		}
		if err := setup.CreateStream(ctx, platform, grandchildInfo); err != nil {
			return errors.Wrap(err, "failed to create grandchild")
		}

		// Set up taxonomy:
		// Link parent to child.
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: parentLocator,
			DataProviders: []string{childLocator.DataProvider.Address()},
			StreamIds:     []string{childLocator.StreamId.String()},
			Weights:       []string{"1.0"},
			Height:        1,
			StartTime:     nil,
		}); err != nil {
			return errors.Wrap(err, "failed to set taxonomy for child")
		}
		// Link child to grandchild.
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: childLocator,
			DataProviders: []string{grandchildLocator.DataProvider.Address()},
			StreamIds:     []string{grandchildLocator.StreamId.String()},
			Weights:       []string{"1.0"},
			Height:        1,
			StartTime:     nil,
		}); err != nil {
			return errors.Wrap(err, "failed to set taxonomy for grandchild")
		}

		// When compose_visibility is not set, the default is public. The chain should be authorized.
		canComposeAll, err := procedure.CheckComposeAllPermissions(ctx, procedure.CheckComposeAllPermissionsInput{
			Platform: platform,
			Locator:  parentLocator,
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "failed to check nested compose permissions initially")
		}
		assert.True(t, canComposeAll, "Expected nested compose to be allowed when compose_visibility is not set")

		// For the edge parent -> child:
		// Mark the child as private.
		if err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  childLocator,
			Key:      "compose_visibility",
			Value:    "1",
			ValType:  "int",
			Height:   1,
		}); err != nil {
			return errors.Wrap(err, "failed to set compose_visibility on child")
		}

		// Initially, without child's whitelist, nested compose should fail.
		canComposeAll, err = procedure.CheckComposeAllPermissions(ctx, procedure.CheckComposeAllPermissionsInput{
			Platform: platform,
			Locator:  parentLocator,
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "failed to check nested compose permissions initially")
		}
		assert.False(t, canComposeAll, "Expected nested compose to be disallowed when child's allow_compose_stream metadata (for parent) is missing")

		// Insert child's allow_compose_stream metadata whitelisting the parent.
		if err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  childLocator,
			Key:      "allow_compose_stream",
			Value:    parentLocator.StreamId.String(),
			ValType:  "ref",
			Height:   1,
		}); err != nil {
			return errors.Wrap(err, "failed to insert allow_compose_stream metadata on child")
		}

		// Now the edge parent->child should be authorized.
		canComposeAll, err = procedure.CheckComposeAllPermissions(ctx, procedure.CheckComposeAllPermissionsInput{
			Platform: platform,
			Locator:  parentLocator,
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "failed to check nested compose permissions after parent's whitelist on child")
		}
		assert.True(t, canComposeAll, "Expected nested compose to be allowed when child's allow_compose_stream metadata whitelists the parent")

		// For the edge child -> grandchild:
		// Mark the grandchild as private.
		if err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  grandchildLocator,
			Key:      "compose_visibility",
			Value:    "1",
			ValType:  "int",
			Height:   1,
		}); err != nil {
			return errors.Wrap(err, "failed to set compose_visibility on grandchild")
		}

		// Without grandchild's whitelist, the nested check should now fail.
		canComposeAll, err = procedure.CheckComposeAllPermissions(ctx, procedure.CheckComposeAllPermissionsInput{
			Platform: platform,
			Locator:  parentLocator,
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "failed to check nested compose permissions after marking grandchild private")
		}
		assert.False(t, canComposeAll, "Expected nested compose to be disallowed when grandchild's allow_compose_stream metadata (for child) is missing")

		// Insert grandchild's allow_compose_stream metadata whitelisting the child.
		if err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  grandchildLocator,
			Key:      "allow_compose_stream",
			Value:    childLocator.StreamId.String(),
			ValType:  "ref",
			Height:   1,
		}); err != nil {
			return errors.Wrap(err, "failed to insert allow_compose_stream metadata on grandchild")
		}

		// Now the full chain should be authorized.
		canComposeAll, err = procedure.CheckComposeAllPermissions(ctx, procedure.CheckComposeAllPermissionsInput{
			Platform: platform,
			Locator:  parentLocator,
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "failed to check nested compose permissions after all metadata inserted")
		}
		assert.True(t, canComposeAll, "Expected nested compose to be allowed when both child's and grandchild's allow_compose_stream metadata are correctly set")

		return nil
	}
}

// // TestAUTH05_StreamDeletion tests AUTH05: Stream owners are able to delete their streams and all associated data.
// func TestAUTH05_StreamDeletion(t *testing.T) {
// 	t.Skip("Test skipped: auth stream tests temporarily disabled")
// 	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
// 		Name: "stream_deletion_AUTH05",
// 		FunctionTests: []kwilTesting.TestFunc{
// 			testStreamDeletion(t, primitiveContractInfo),
// 			testStreamDeletion(t, composedContractInfo),
// 		},
// 	})
// }

func TestAUTH05_StreamDeletion(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "stream_deletion_test",
		SeedScripts: []string{
			"../../../internal/migrations/000-initial-data.sql",
			"../../../internal/migrations/001-common-actions.sql",
		},
		FunctionTests: []kwilTesting.TestFunc{
			testStreamDeletion(t),
		},
	}, testutils.GetTestOptions())
}

func testStreamDeletion(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dataProvider := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000001")
		streamLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("stream_deletion_test"),
			DataProvider: dataProvider,
		}

		// Set up and initialize the contract
		err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Locator: streamLocator,
			Type:    setup.ContractTypePrimitive,
		})
		if err != nil {
			return errors.Wrap(err, "failed to create stream for deletion test")
		}

		// Delete the stream
		_, err = setup.DeleteStream(ctx, platform, streamLocator)
		if err != nil {
			return errors.Wrap(err, "failed to delete stream")
		}
		assert.NoError(t, err, "Error should not be returned when deleting stream")

		// Verify the contract no longer exists
		//exists, err := procedure.CheckContractExists(ctx, procedure.CheckContractExistsInput{
		//	Platform: platform,
		//	Deployer: contractInfo.Deployer,
		//	DBID:     dbid,
		//})
		//assert.False(t, exists, "Contract should not exist after deletion")
		//assert.NoError(t, err, "Error should not be returned when checking contract existence")

		return nil
	}
}
