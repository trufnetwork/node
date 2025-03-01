package tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/parse"
	kwilTesting "github.com/kwilteam/kwil-db/testing"

	"github.com/trufnetwork/node/internal/contracts"
	"github.com/trufnetwork/sdk-go/core/util"
)

// ContractInfo holds information about a contract for testing purposes.
type ContractInfo struct {
	Name     string
	StreamID util.StreamId
	Deployer util.EthereumAddress
	Content  []byte
}

var (
	primitiveContractInfo = ContractInfo{
		Name:     "primitive_stream_test",
		StreamID: util.GenerateStreamId("primitive_stream_test"),
		Deployer: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123"),
		Content:  contracts.PrimitiveStreamContent,
	}

	composedContractInfo = ContractInfo{
		Name:     "composed_stream_test",
		StreamID: util.GenerateStreamId("composed_stream_test"),
		Deployer: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000456"),
		Content:  contracts.ComposedStreamContent,
	}
)

// TestAUTH01_StreamOwnership tests AUTH.01: Stream ownership is clearly defined and can be transferred to another valid wallet.
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

func testStreamOwnershipTransfer(t *testing.T, contractInfo ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return err
		}
		dbid := getDBID(contractInfo)

		// Transfer ownership
		newOwner := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		err := transferStreamOwnership(ctx, platform, contractInfo.Deployer, dbid, newOwner)
		if err != nil {
			return errors.Wrap(err, "Failed to transfer ownership")
		}

		// Attempt to perform an owner-only action with the old owner
		err = insertMetadata(ctx, platform, contractInfo.Deployer, dbid, "new_key", "new_value", "string")
		assert.Error(t, err, "Old owner should not be able to insert metadata after ownership transfer")

		// Change platform deployer to the new owner
		newOwnerAddress := util.Unsafe_NewEthereumAddressFromString(newOwner)
		platform.Deployer = newOwnerAddress.Bytes()

		// Attempt to perform an owner-only action with the new owner
		err = insertMetadata(ctx, platform, newOwnerAddress, dbid, "new_key", "new_value", "string")
		assert.NoError(t, err, "New owner should be able to insert metadata after ownership transfer")

		return nil
	}
}

func testInvalidAddressOwnershipTransfer(t *testing.T, contractInfo ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return err
		}
		dbid := getDBID(contractInfo)

		// Attempt to transfer ownership to an invalid address
		invalidAddress := "invalid_address"
		err := transferStreamOwnership(ctx, platform, contractInfo.Deployer, dbid, invalidAddress)
		assert.Error(t, err, "Should not accept invalid Ethereum address")

		return nil
	}
}

// TestAUTH02_ReadPermissions tests AUTH.02: The stream owner can control which wallets are allowed to read from the stream.
func TestAUTH02_ReadPermissions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "read_permission_control_AUTH02",
		FunctionTests: []kwilTesting.TestFunc{
			testReadPermissionControl(t, primitiveContractInfo),
			testReadPermissionControl(t, composedContractInfo),
		},
	})
}

func testReadPermissionControl(t *testing.T, contractInfo ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return err
		}
		dbid := getDBID(contractInfo)

		// Initially, anyone should be able to read (public visibility)
		nonOwnerUnauthorized := util.Unsafe_NewEthereumAddressFromString("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
		nonOwnerAuthorized := util.Unsafe_NewEthereumAddressFromString("0xffffffffffffffffffffffffffffffffffffffff")

		// Add non-owner authorized to read whitelist
		err := insertMetadata(ctx, platform, contractInfo.Deployer, dbid, "allow_read_wallet", nonOwnerAuthorized.Address(), "ref")
		if err != nil {
			return errors.Wrap(err, "Failed to add wallet to read whitelist")
		}

		// Anyone should be able to read when read_visibility is public
		canRead, err := checkReadPermissions(ctx, platform, contractInfo.Deployer, dbid, nonOwnerUnauthorized.Address())
		assert.True(t, canRead, "Should be able to read when read_visibility is public")
		assert.NoError(t, err, "Error should not be returned when checking read permissions")

		// Change read_visibility to private (1)
		err = insertMetadata(ctx, platform, contractInfo.Deployer, dbid, "read_visibility", "1", "int")
		if err != nil {
			return errors.Wrap(err, "Failed to change read_visibility")
		}

		// Verify non-owner unauthorized can't read
		canRead, err = checkReadPermissions(ctx, platform, contractInfo.Deployer, dbid, nonOwnerUnauthorized.Address())
		assert.False(t, canRead, "Non-owner should not be able to read when read_visibility is private")
		assert.NoError(t, err, "Error should not be returned when checking read permissions")

		// Verify non-owner authorized to read
		canRead, err = checkReadPermissions(ctx, platform, contractInfo.Deployer, dbid, nonOwnerAuthorized.Address())
		assert.True(t, canRead, "Whitelisted wallet should be able to read when read_visibility is private")
		assert.NoError(t, err, "Error should not be returned when checking read permissions")

		return nil
	}
}

func checkReadPermissions(ctx context.Context, platform *kwilTesting.Platform, deployer util.EthereumAddress, dbid string, wallet string) (bool, error) {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	result, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
		Procedure: "is_wallet_allowed_to_read",
		Dataset:   dbid,
		Args:      []any{wallet},
	})
	if err != nil {
		return false, err
	}
	if len(result.Rows) == 0 {
		return false, errors.New("No result returned")
	}
	return result.Rows[0][0].(bool), nil
}

// TestAUTH03_WritePermissions tests AUTH.03: The stream owner can control which wallets are allowed to insert data into the stream.
func TestAUTH03_WritePermissions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "write_permission_control_AUTH03",
		FunctionTests: []kwilTesting.TestFunc{
			testWritePermissionControl(t, primitiveContractInfo),
			testWritePermissionControl(t, composedContractInfo),
		},
	})
}

func testWritePermissionControl(t *testing.T, contractInfo ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return err
		}
		dbid := getDBID(contractInfo)

		// Create a non-owner wallet
		nonOwner := util.Unsafe_NewEthereumAddressFromString("0xdddddddddddddddddddddddddddddddddddddddd")

		// Check if non-owner can write (should be false by default)
		canWrite, err := checkWritePermissions(ctx, platform, contractInfo.Deployer, dbid, nonOwner.Address())
		assert.False(t, canWrite, "Non-owner should not be able to write by default")
		assert.NoError(t, err, "Error should not be returned when checking write permissions")

		// Add non-owner to write whitelist
		err = insertMetadata(ctx, platform, contractInfo.Deployer, dbid, "allow_write_wallet", nonOwner.Address(), "ref")
		if err != nil {
			return errors.Wrap(err, "Failed to add wallet to write whitelist")
		}

		// Verify non-owner can now write
		canWrite, err = checkWritePermissions(ctx, platform, contractInfo.Deployer, dbid, nonOwner.Address())
		assert.True(t, canWrite, "Whitelisted wallet should be able to write")
		assert.NoError(t, err, "Error should not be returned when checking write permissions")

		return nil
	}
}

func checkWritePermissions(ctx context.Context, platform *kwilTesting.Platform, deployer util.EthereumAddress, dbid string, wallet string) (bool, error) {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	result, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
		Procedure: "is_wallet_allowed_to_write",
		Dataset:   dbid,
		Args:      []any{wallet},
	})
	if err != nil {
		return false, err
	}
	if len(result.Rows) == 0 {
		return false, errors.New("No result returned")
	}
	return result.Rows[0][0].(bool), nil
}

// TestAUTH04_ComposePermissions tests AUTH.04: The stream owner can control which streams are allowed to compose from the stream.
func TestAUTH04_ComposePermissions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "compose_permission_control_AUTH04",
		FunctionTests: []kwilTesting.TestFunc{
			testComposePermissionControl(t, primitiveContractInfo),
			testComposePermissionControl(t, composedContractInfo),
		},
	})
}

func testComposePermissionControl(t *testing.T, contractInfo ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the primary contract
		if err := setupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return err
		}
		dbid := getDBID(contractInfo)

		// Set up a foreign contract (the one attempting to compose)
		foreignContractInfo := ContractInfo{
			Name:     "foreign_stream_test",
			StreamID: util.GenerateStreamId("foreign_stream_test"),
			Deployer: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000abc"),
			Content:  contracts.PrimitiveStreamContent, // Using the same contract content for simplicity
		}

		if err := setupAndInitializeContract(ctx, platform, foreignContractInfo); err != nil {
			return err
		}

		// Set compose_visibility to private (1)
		err := insertMetadata(ctx, platform, contractInfo.Deployer, dbid, "compose_visibility", "1", "int")
		if err != nil {
			return errors.Wrap(err, "Failed to change compose_visibility")
		}

		foreignDbid := getDBID(foreignContractInfo)

		// Verify foreign stream cannot compose without permission
		canCompose, err := checkComposePermissions(ctx, platform, dbid, foreignDbid)
		assert.False(t, canCompose, "Foreign stream should not be allowed to compose without permission")
		assert.Error(t, err, "Expected permission error when composing without permission")

		// Grant compose permission to the foreign stream
		err = insertMetadata(ctx, platform, contractInfo.Deployer, dbid, "allow_compose_stream", foreignDbid, "ref")
		if err != nil {
			return errors.Wrap(err, "Failed to grant compose permission")
		}

		// Verify foreign stream can now compose
		platform.Deployer = foreignContractInfo.Deployer.Bytes()
		canCompose, err = checkComposePermissions(ctx, platform, dbid, foreignDbid)
		assert.True(t, canCompose, "Foreign stream should be allowed to compose after permission is granted")
		assert.NoError(t, err, "No error expected when composing with permission")

		return nil
	}
}

func checkComposePermissions(ctx context.Context, platform *kwilTesting.Platform, dbid string, foreignCaller string) (bool, error) {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return false, err
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}
	result, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
		Procedure: "is_stream_allowed_to_compose",
		Dataset:   dbid,
		Args:      []any{foreignCaller},
	})
	if err != nil {
		return false, err
	}
	if len(result.Rows) == 0 {
		return false, errors.New("No result returned")
	}
	return result.Rows[0][0].(bool), nil
}

// TestAUTH05_StreamDeletion tests AUTH.05: Stream owners are able to delete their streams and all associated data.
func TestAUTH05_StreamDeletion(t *testing.T) {
	t.Skip("Stream deletion not supported at the contract level at the moment")

	// This is a placeholder test for AUTH.05
}

// Helper functions

func setupAndInitializeContract(ctx context.Context, platform *kwilTesting.Platform, contractInfo ContractInfo) error {
	if err := setupContract(ctx, platform, contractInfo); err != nil {
		return err
	}
	dbid := getDBID(contractInfo)
	return initializeContract(ctx, platform, dbid, contractInfo.Deployer)
}

func setupContract(ctx context.Context, platform *kwilTesting.Platform, contractInfo ContractInfo) error {
	schema, err := parse.Parse(contractInfo.Content)
	if err != nil {
		return errors.Wrapf(err, "Failed to parse contract %s", contractInfo.Name)
	}
	schema.Name = contractInfo.StreamID.String()

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       contractInfo.Deployer.Bytes(),
		Caller:       contractInfo.Deployer.Address(),
		TxID:         platform.Txid(),
	}

	return platform.Engine.CreateDataset(txContext, platform.DB, schema)
}

func initializeContract(ctx context.Context, platform *kwilTesting.Platform, dbid string, deployer util.EthereumAddress) error {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	_, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
		Procedure: "initialize",
		Dataset:   dbid,
		Args:      []any{deployer.Address(), deployer.Address(), deployer.Address()},
	})
	return err
}

func getDBID(contractInfo ContractInfo) string {
	return contractInfo.Name + "#" + contractInfo.Deployer.Address()
}

func insertMetadata(ctx context.Context, platform *kwilTesting.Platform, deployer util.EthereumAddress, dbid string, key, value, valType string) error {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	_, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
		Procedure: "insert_metadata",
		Dataset:   dbid,
		Args:      []any{key, value, valType},
	})
	return err
}

func transferStreamOwnership(ctx context.Context, platform *kwilTesting.Platform, deployer util.EthereumAddress, dbid, newOwner string) error {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	_, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
		Procedure: "transfer_stream_ownership",
		Dataset:   dbid,
		Args:      []any{newOwner},
	})
	return err
}
