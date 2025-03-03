package tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/core/types/decimal"
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
		Type:     setup.ContractTypePrimitive,
	}

	composedContractInfo = setup.ContractInfo{
		Name:     "composed_stream_test",
		StreamID: util.GenerateStreamId("composed_stream_test"),
		Deployer: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000456"),
		Content:  contracts.ComposedStreamContent,
		Type:     setup.ContractTypeComposed,
	}
)

func TestMetadataInsertionAndRetrieval(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "metadata_insertion_and_retrieval",
		FunctionTests: []kwilTesting.TestFunc{
			testMetadataInsertionAndRetrieval(t, primitiveContractInfo),
			testMetadataInsertionAndRetrieval(t, composedContractInfo),
		},
	})
}

func testMetadataInsertionAndRetrieval(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return err
		}
		dbid := setup.GetDBID(contractInfo)

		// Insert metadata of various types
		metadataItems := []struct {
			Key     string
			Value   string
			ValType string
			Expect  interface{}
		}{
			{"int_key", "42", "int", int64(42)},
			{"string_key", "hello world", "string", "hello world"},
			{"bool_key", "true", "bool", true},
			{"float_key", "3.1415", "float", "3.141500000000000000"},
			{"ref_key", "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd", "ref", "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"},
		}

		for _, item := range metadataItems {
			err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
				Platform: platform,
				Deployer: contractInfo.Deployer,
				DBID:     dbid,
				Key:      item.Key,
				Value:    item.Value,
				ValType:  item.ValType,
			})
			if err != nil {
				return errors.Wrapf(err, "Failed to insert metadata key %s", item.Key)
			}
		}

		// Retrieve and verify metadata
		for _, item := range metadataItems {
			result, err := procedure.GetMetadata(ctx, procedure.GetMetadataInput{
				Platform: platform,
				Deployer: contractInfo.Deployer,
				DBID:     dbid,
				Key:      item.Key,
			})
			if err != nil {
				return errors.Wrapf(err, "Failed to get metadata key %s", item.Key)
			}
			assertMetadataValue(t, item.ValType, item.Expect, result, item.Key)
		}

		return nil
	}
}

// Helper function to assert metadata values based on type
func assertMetadataValue(t *testing.T, valType string, expected interface{}, result []any, key string) {
	switch valType {
	case "int":
		assert.Equal(t, expected, result[1].(int64), "Metadata value mismatch for key %s", key)
	case "string":
		assert.Equal(t, expected, result[4].(string), "Metadata value mismatch for key %s", key)
	case "bool":
		assert.Equal(t, expected, result[3].(bool), "Metadata value mismatch for key %s", key)
	case "float":
		assert.Equal(t, expected, result[2].(*decimal.Decimal).String(), "Metadata value mismatch for key %s", key)
	case "ref":
		assert.Equal(t, expected, result[5].(string), "Metadata value mismatch for key %s", key)
	}
}

func TestCOMMON01OnlyOwnerCanInsertMetadata(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "only_owner_can_insert_metadata",
		FunctionTests: []kwilTesting.TestFunc{
			testOnlyOwnerCanInsertMetadata(t, primitiveContractInfo),
			testOnlyOwnerCanInsertMetadata(t, composedContractInfo),
		},
	})
}

func testOnlyOwnerCanInsertMetadata(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return err
		}
		dbid := setup.GetDBID(contractInfo)

		// Change the deployer to a non-owner
		nonOwner := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")

		// Attempt to insert metadata
		err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Deployer: nonOwner,
			DBID:     dbid,
			Key:      "unauthorized_key",
			Value:    "value",
			ValType:  "string",
		})
		assert.Error(t, err, "Non-owner should not be able to insert metadata")
		return nil
	}
}

func TestCOMMON03DisableMetadata(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "disable_metadata",
		FunctionTests: []kwilTesting.TestFunc{
			testDisableMetadata(t, primitiveContractInfo),
			testDisableMetadata(t, composedContractInfo),
		},
	})
}

func testDisableMetadata(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return err
		}
		dbid := setup.GetDBID(contractInfo)

		// Insert metadata
		key := "temp_key"
		value := "temporary value"
		valType := "string"

		err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
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
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Key:      key,
		})
		if err != nil {
			return errors.Wrapf(err, "Failed to get metadata key %s", key)
		}
		rowID := result[0].(*types.UUID)

		// Disable the metadata
		err = procedure.DisableMetadata(ctx, procedure.DisableMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			RowID:    rowID,
		})
		if err != nil {
			return errors.Wrap(err, "Failed to disable metadata")
		}

		// Attempt to retrieve the disabled metadata
		_, err = procedure.GetMetadata(ctx, procedure.GetMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Key:      key,
		})
		assert.Error(t, err, "Disabled metadata should not be retrievable")

		return nil
	}
}

func TestCOMMON02ReadOnlyMetadataCannotBeModified(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "readonly_metadata_cannot_be_modified",
		FunctionTests: []kwilTesting.TestFunc{
			testReadOnlyMetadataCannotBeModified(t, primitiveContractInfo),
			testReadOnlyMetadataCannotBeModified(t, composedContractInfo),
		},
	})
}

func testReadOnlyMetadataCannotBeModified(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return err
		}
		dbid := setup.GetDBID(contractInfo)

		// Attempt to insert metadata with a read-only key
		err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Key:      "type",
			Value:    "modified",
			ValType:  "string",
		})
		assert.Error(t, err, "Should not be able to modify read-only metadata")
		// Attempt to disable read-only metadata
		result, err := procedure.GetMetadata(ctx, procedure.GetMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Key:      "type",
		})
		if err != nil {
			return errors.Wrap(err, "Failed to get read-only metadata")
		}
		rowID := result[0].(*types.UUID)

		err = procedure.DisableMetadata(ctx, procedure.DisableMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			RowID:    rowID,
		})
		assert.Error(t, err, "Should not be able to disable read-only metadata")

		return nil
	}
}

func TestInitializationLogic(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "initialization_logic",
		FunctionTests: []kwilTesting.TestFunc{
			testInitializationLogic(t, primitiveContractInfo),
			testInitializationLogic(t, composedContractInfo),
		},
	})
}

func testInitializationLogic(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return err
		}
		dbid := setup.GetDBID(contractInfo)

		txContext := &common.TxContext{
			Ctx:          ctx,
			BlockContext: &common.BlockContext{Height: 0},
			Signer:       contractInfo.Deployer.Bytes(),
			TxID:         platform.Txid(),
		}

		// Attempt to re-initialize the contract
		_, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
			Procedure: "init",
			Dataset:   dbid,
			Args:      []any{},
		})
		assert.Error(t, err, "Contract should not be re-initializable")

		return nil
	}
}

func TestVisibilitySettings(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "visibility_settings",
		FunctionTests: []kwilTesting.TestFunc{
			testVisibilitySettings(t, primitiveContractInfo),
			testVisibilitySettings(t, composedContractInfo),
		},
	})
}

func testVisibilitySettings(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set up and initialize the contract
		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
			return err
		}
		dbid := setup.GetDBID(contractInfo)

		// Change read_visibility to private (1)
		err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Key:      "read_visibility",
			Value:    "1",
			ValType:  "int",
		})
		if err != nil {
			return errors.Wrap(err, "Failed to change read_visibility")
		}

		// Change deployer to a non-owner
		nonOwner := util.Unsafe_NewEthereumAddressFromString("0xcccccccccccccccccccccccccccccccccccccccc")
		platform.Deployer = nonOwner.Bytes()

		// Attempt to read data
		canRead, err := procedure.CheckReadPermissions(ctx, procedure.CheckReadPermissionsInput{
			Platform: platform,
			Deployer: contractInfo.Deployer,
			DBID:     dbid,
			Wallet:   nonOwner.Address(),
		})
		assert.False(t, canRead, "Non-owner should not be able to read when read_visibility is private")
		assert.NoError(t, err, "Error should not be returned when checking read permissions")

		return nil
	}
}
