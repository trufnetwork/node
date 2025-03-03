package tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	kwilTesting "github.com/kwilteam/kwil-db/testing"

	"github.com/trufnetwork/node/internal/contracts"
	"github.com/trufnetwork/node/internal/contracts/tests/utils/procedure"
	"github.com/trufnetwork/node/internal/contracts/tests/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

var (
	primitiveContractInfo = setup.ContractInfo{
		Name:     "primitive_stream_test",
		StreamID: util.GenerateStreamId("primitive_stream_test"),
		Deployer: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123"),
		Content:  contracts.PrimitiveStreamContent,
	}

	composedContractInfo = setup.ContractInfo{
		Name:     "composed_stream_test",
		StreamID: util.GenerateStreamId("composed_stream_test"),
		Deployer: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000456"),
		Content:  contracts.ComposedStreamContent,
	}
)

// TestAUTH01_StreamOwnership tests AUTH01: Stream ownership is clearly defined and can be transferred to another valid wallet.
func TestAUTH01_StreamOwnership(t *testing.T) {
	// Test valid ownership transfer
	t.Run("ValidOwnershipTransfer", func(t *testing.T) {
		kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
			Name: "stream_ownership_transfer_AUTH01",
			FunctionTests: []kwilTesting.TestFunc{
				testStreamOwnershipTransfer(t, primitiveContractInfo),
				testStreamOwnershipTransfer(t, composedContractInfo),
			},
		})
	})

	// Test invalid address handling
	t.Run("InvalidAddressHandling", func(t *testing.T) {
		kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
			Name: "invalid_address_ownership_transfer_AUTH01",
			FunctionTests: []kwilTesting.TestFunc{
				testInvalidAddressOwnershipTransfer(t, primitiveContractInfo),
				testInvalidAddressOwnershipTransfer(t, composedContractInfo),
			},
		})
	})
}

func testStreamOwnershipTransfer(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return errors.Wrapf(err, "failed to setup and initialize contract %s for ownership transfer test", contractInfo.Name)
		}
		dbid := setup.GetDBID(contractInfo)

		// Transfer ownership
		newOwner := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		err := procedure.TransferStreamOwnership(ctx, procedure.TransferStreamOwnershipInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			NewOwner: newOwner,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to transfer ownership of contract %s to %s", contractInfo.Name, newOwner)
		}

		// Attempt to perform an owner-only action with the old owner
		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Key:      "new_key",
			Value:    "new_value",
			ValType:  "string",
		})
		assert.Error(t, err, "Old owner should not be able to insert metadata after ownership transfer")

		// Change platform deployer to the new owner
		newOwnerAddress := util.Unsafe_NewEthereumAddressFromString(newOwner)
		platform.Deployer = newOwnerAddress.Bytes()

		// Attempt to perform an owner-only action with the new owner
		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Deployer: newOwnerAddress,
			DBID:     dbid,
			Key:      "new_key",
			Value:    "new_value",
			ValType:  "string",
		})
		assert.NoError(t, err, "New owner should be able to insert metadata after ownership transfer")

		return nil
	}
}

func testInvalidAddressOwnershipTransfer(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return errors.Wrapf(err, "failed to setup and initialize contract %s for invalid address test", contractInfo.Name)
		}
		dbid := setup.GetDBID(contractInfo)

		// Attempt to transfer ownership to an invalid address
		invalidAddress := "invalid_address"
		err := procedure.TransferStreamOwnership(ctx, procedure.TransferStreamOwnershipInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			NewOwner: invalidAddress,
		})
		assert.Error(t, err, "Should not accept invalid Ethereum address")

		return nil
	}
}

// TestAUTH02_ReadPermissions tests AUTH02: The stream owner can control which wallets are allowed to read from the stream.
func TestAUTH02_ReadPermissions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "read_permission_control_AUTH02",
		FunctionTests: []kwilTesting.TestFunc{
			testReadPermissionControl(t, primitiveContractInfo),
			testReadPermissionControl(t, composedContractInfo),
		},
	})
}

func testReadPermissionControl(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return errors.Wrapf(err, "failed to setup and initialize contract %s for read permission test", contractInfo.Name)
		}
		dbid := setup.GetDBID(contractInfo)

		// Initially, anyone should be able to read (public visibility)
		nonOwnerUnauthorized := util.Unsafe_NewEthereumAddressFromString("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
		nonOwnerAuthorized := util.Unsafe_NewEthereumAddressFromString("0xffffffffffffffffffffffffffffffffffffffff")

		// Add non-owner authorized to read whitelist
		err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Key:      "allow_read_wallet",
			Value:    nonOwnerAuthorized.Address(),
			ValType:  "ref",
		})
		if err != nil {
			return errors.Wrapf(err, "failed to add wallet %s to read whitelist for contract %s",
				nonOwnerAuthorized.Address(), contractInfo.Name)
		}

		// Anyone should be able to read when read_visibility is public
		canRead, err := procedure.CheckReadPermissions(ctx, procedure.CheckReadPermissionsInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Wallet:   nonOwnerUnauthorized.Address(),
		})
		assert.True(t, canRead, "Should be able to read when read_visibility is public")
		assert.NoError(t, err, "Error should not be returned when checking read permissions")

		// Change read_visibility to private (1)
		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Key:      "read_visibility",
			Value:    "1",
			ValType:  "int",
		})
		if err != nil {
			return errors.Wrapf(err, "failed to change read_visibility to private for contract %s", contractInfo.Name)
		}

		// Verify non-owner unauthorized can't read
		canRead, err = procedure.CheckReadPermissions(ctx, procedure.CheckReadPermissionsInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Wallet:   nonOwnerUnauthorized.Address(),
		})
		assert.False(t, canRead, "Non-owner should not be able to read when read_visibility is private")
		assert.NoError(t, err, "Error should not be returned when checking read permissions")

		// Verify non-owner authorized to read
		canRead, err = procedure.CheckReadPermissions(ctx, procedure.CheckReadPermissionsInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Wallet:   nonOwnerAuthorized.Address(),
		})
		assert.True(t, canRead, "Whitelisted wallet should be able to read when read_visibility is private")
		assert.NoError(t, err, "Error should not be returned when checking read permissions")

		return nil
	}
}

// TestAUTH03_WritePermissions tests AUTH03: The stream owner can control which wallets are allowed to insert data into the stream.
func TestAUTH03_WritePermissions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "write_permission_control_AUTH03",
		FunctionTests: []kwilTesting.TestFunc{
			testWritePermissionControl(t, primitiveContractInfo),
			testWritePermissionControl(t, composedContractInfo),
		},
	})
}

func testWritePermissionControl(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return errors.Wrapf(err, "failed to setup and initialize contract %s for write permission test", contractInfo.Name)
		}
		dbid := setup.GetDBID(contractInfo)

		// Create a non-owner wallet
		nonOwner := util.Unsafe_NewEthereumAddressFromString("0xdddddddddddddddddddddddddddddddddddddddd")

		// Check if non-owner can write (should be false by default)
		canWrite, err := procedure.CheckWritePermissions(ctx, procedure.CheckWritePermissionsInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Wallet:   nonOwner.Address(),
		})
		assert.False(t, canWrite, "Non-owner should not be able to write by default")
		assert.NoError(t, err, "Error should not be returned when checking write permissions")

		// Add non-owner to write whitelist
		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Key:      "allow_write_wallet",
			Value:    nonOwner.Address(),
			ValType:  "ref",
		})
		if err != nil {
			return errors.Wrapf(err, "failed to add wallet %s to write whitelist for contract %s",
				nonOwner.Address(), contractInfo.Name)
		}

		// Verify non-owner can now write
		canWrite, err = procedure.CheckWritePermissions(ctx, procedure.CheckWritePermissionsInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Wallet:   nonOwner.Address(),
		})
		// TODO: right now, composed contract doesn't have this procedure to check write permission.
		//   however, in the next iteration it should be implemented.
		assert.True(t, canWrite, "Whitelisted wallet should be able to write")
		assert.NoError(t, err, "Error should not be returned when checking write permissions")

		return nil
	}
}

// TestAUTH04_ComposePermissions tests AUTH04: The stream owner can control which streams are allowed to compose from the stream.
func TestAUTH04_ComposePermissions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "compose_permission_control_AUTH04",
		FunctionTests: []kwilTesting.TestFunc{
			testComposePermissionControl(t, primitiveContractInfo),
			testComposePermissionControl(t, composedContractInfo),
		},
	})
}

func testComposePermissionControl(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the primary contract
		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return errors.Wrapf(err, "failed to setup and initialize primary contract %s for compose permission test", contractInfo.Name)
		}
		dbid := setup.GetDBID(contractInfo)

		// Set up a foreign contract (the one attempting to compose)
		foreignContractInfo := setup.ContractInfo{
			Name:     "foreign_stream_test",
			StreamID: util.GenerateStreamId("foreign_stream_test"),
			Deployer: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000abc"),
			Content:  contracts.PrimitiveStreamContent, // Using the same contract content for simplicity
		}

		if err := setup.SetupAndInitializeContract(ctx, platform, foreignContractInfo); err != nil {
			return errors.Wrapf(err, "failed to setup and initialize foreign contract %s for compose permission test",
				foreignContractInfo.Name)
		}

		// Set compose_visibility to private (1)
		err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Key:      "compose_visibility",
			Value:    "1",
			ValType:  "int",
		})
		if err != nil {
			return errors.Wrapf(err, "failed to change compose_visibility to private for contract %s", contractInfo.Name)
		}

		foreignDbid := setup.GetDBID(foreignContractInfo)

		// Verify foreign stream cannot compose without permission
		canCompose, err := procedure.CheckComposePermissions(ctx, procedure.CheckComposePermissionsInput{
			Platform:      platform,
			DBID:          dbid,
			ForeignCaller: foreignDbid,
		})
		assert.False(t, canCompose, "Foreign stream should not be allowed to compose without permission")
		assert.Error(t, err, "Expected permission error when composing without permission")

		// Grant compose permission to the foreign stream
		err = procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Key:      "allow_compose_stream",
			Value:    foreignDbid,
			ValType:  "ref",
		})
		if err != nil {
			return errors.Wrapf(err, "failed to grant compose permission to foreign stream %s for contract %s",
				foreignDbid, contractInfo.Name)
		}

		// Verify foreign stream can now compose
		platform.Deployer = foreignContractInfo.Deployer.Bytes()
		canCompose, err = procedure.CheckComposePermissions(ctx, procedure.CheckComposePermissionsInput{
			Platform:      platform,
			DBID:          dbid,
			ForeignCaller: foreignDbid,
		})
		assert.True(t, canCompose, "Foreign stream should be allowed to compose after permission is granted")
		assert.NoError(t, err, "No error expected when composing with permission")

		return nil
	}
}

// TestAUTH05_StreamDeletion tests AUTH05: Stream owners are able to delete their streams and all associated data.
func TestAUTH05_StreamDeletion(t *testing.T) {
	t.Skip("Stream deletion not supported at the contract level at the moment")
}
