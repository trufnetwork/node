package tests

// import (
// 	"context"
// 	"testing"

// 	"github.com/pkg/errors"
// 	"github.com/stretchr/testify/assert"

// 	kwilTesting "github.com/kwilteam/kwil-db/testing"

// 	testutils "github.com/trufnetwork/node/tests/streams/utils"
// 	"github.com/trufnetwork/node/tests/streams/utils/procedure"
// 	"github.com/trufnetwork/node/tests/streams/utils/setup"
// 	"github.com/trufnetwork/sdk-go/core/util"
// )

// var (
// 	primitiveContractInfo = setup.ContractInfo{
// 		Name:     "primitive_stream_test",
// 		StreamID: util.GenerateStreamId("primitive_stream_test"),
// 		Deployer: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123"),
// 		Type:     setup.ContractTypePrimitive,
// 	}

// 	composedContractInfo = setup.ContractInfo{
// 		Name:     "composed_stream_test",
// 		StreamID: util.GenerateStreamId("composed_stream_test"),
// 		Deployer: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000456"),
// 		Type:     setup.ContractTypeComposed,
// 	}
// )

// // TestQUERY04MetadataInsertionAndRetrieval tests the insertion and retrieval of metadata
// // for both primitive and composed streams. Also tests disabling metadata and attempting to retrieve it.
// func TestQUERY04MetadataInsertionAndRetrieval(t *testing.T) {
// 	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
// 		Name: "metadata_insertion_and_retrieval",
// 		FunctionTests: []kwilTesting.TestFunc{
// 			testMetadataInsertionAndRetrieval(t, primitiveContractInfo),
// 			testMetadataInsertionAndRetrieval(t, composedContractInfo),
// 			testMetadataInsertionThenDisableAndRetrieval(t, primitiveContractInfo),
// 		},
// 	}, testutils.GetTestOptions())
// }

// func testMetadataInsertionAndRetrieval(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		// Set up and initialize the contract
// 		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
// 			return err
// 		}

// 		// Insert metadata of various types
// 		metadataItems := []struct {
// 			Key     string
// 			Value   string
// 			ValType string
// 		}{
// 			{"string_key", "string_value", "string"},
// 			{"int_key", "42", "int"},
// 			{"bool_key", "true", "bool"},
// 		}

// 		for _, item := range metadataItems {
// 			err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
// 				Platform: platform,
// 				Deployer: contractInfo.Deployer,
// 				StreamId: contractInfo.StreamID,
// 				Key:      item.Key,
// 				Value:    item.Value,
// 				ValType:  item.ValType,
// 			})
// 			if err != nil {
// 				return errors.Wrapf(err, "error inserting metadata with key %s", item.Key)
// 			}
// 		}

// 		// Retrieve and verify metadata
// 		for _, item := range metadataItems {
// 			result, err := procedure.GetMetadata(ctx, procedure.GetMetadataInput{
// 				Platform: platform,
// 				Deployer: contractInfo.Deployer,
// 				StreamId: contractInfo.StreamID,
// 				Key:      item.Key,
// 			})
// 			if err != nil {
// 				return errors.Wrapf(err, "error retrieving metadata with key %s", item.Key)
// 			}
// 			assert.Equal(t, item.Value, result.Value, "Metadata value should match")
// 			assert.Equal(t, item.ValType, result.ValType, "Metadata type should match")
// 		}

// 		return nil
// 	}
// }

// func testMetadataInsertionThenDisableAndRetrieval(t *testing.T, contractInfo setup.ContractInfo) kwilTesting.TestFunc {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		// Set up and initialize the contract
// 		if err := setup.SetupAndInitializeContract(ctx, platform, contractInfo); err != nil {
// 			return err
// 		}

// 		// Insert metadata
// 		key := "temp_key"
// 		value := "temp_value"
// 		valType := "string"

// 		err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
// 			Platform: platform,
// 			Deployer: contractInfo.Deployer,
// 			StreamId: contractInfo.StreamID,
// 			Key:      key,
// 			Value:    value,
// 			ValType:  valType,
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error inserting metadata")
// 		}

// 		// Retrieve metadata and get row ID
// 		result, err := procedure.GetMetadata(ctx, procedure.GetMetadataInput{
// 			Platform: platform,
// 			Deployer: contractInfo.Deployer,
// 			StreamId: contractInfo.StreamID,
// 			Key:      key,
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error retrieving metadata")
// 		}
// 		rowID := result.RowID

// 		// Disable metadata
// 		err = procedure.DisableMetadata(ctx, procedure.DisableMetadataInput{
// 			Platform: platform,
// 			Deployer: contractInfo.Deployer,
// 			StreamId: contractInfo.StreamID,
// 			RowID:    rowID,
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error disabling metadata")
// 		}

// 		// Try to retrieve disabled metadata
// 		_, err = procedure.GetMetadata(ctx, procedure.GetMetadataInput{
// 			Platform: platform,
// 			Deployer: contractInfo.Deployer,
// 			StreamId: contractInfo.StreamID,
// 			Key:      key,
// 		})
// 		assert.Error(t, err, "Disabled metadata should not be retrievable")

// 		return nil
// 	}
// }
