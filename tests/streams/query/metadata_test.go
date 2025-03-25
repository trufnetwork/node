package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	kwilTesting "github.com/kwilteam/kwil-db/testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

var (
	// we only need one kind, because now the setup is shared between stream types
	primitiveContractInfo = setup.StreamInfo{
		Locator: types.StreamLocator{
			StreamId:     util.GenerateStreamId("primitive_stream_test"),
			DataProvider: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123"),
		},
		Type: setup.ContractTypePrimitive,
	}
)

// TestQUERY04MetadataInsertionAndRetrieval tests the insertion and retrieval of metadata
// for both primitive and composed streams. Also tests disabling metadata and attempting to retrieve it.
func TestQUERY04MetadataInsertionAndRetrieval(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "metadata_insertion_and_retrieval",
		FunctionTests: []kwilTesting.TestFunc{
			WithMetadataTestSetup(testMetadataInsertionAndRetrieval(t, primitiveContractInfo)),
			WithMetadataTestSetup(testMetadataInsertionThenDisableAndRetrieval(t, primitiveContractInfo)),
		},
		SeedScripts: migrations.GetSeedScriptPaths(),
	}, testutils.GetTestOptions())
}

func WithMetadataTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, primitiveContractInfo.Locator.DataProvider.Bytes())
		if err := setup.CreateStream(ctx, platform, primitiveContractInfo); err != nil {
			return err
		}
		return testFn(ctx, platform)
	}
}

func testMetadataInsertionAndRetrieval(t *testing.T, contractInfo setup.StreamInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Insert metadata of various types
		metadataItems := []struct {
			Key     string
			Value   string
			ValType string
		}{
			{"string_key", "string_value", "string"},
			{"int_key", "42", "int"},
			{"bool_key", "true", "bool"},
		}

		for _, item := range metadataItems {
			err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
				Platform: platform,
				Locator:  contractInfo.Locator,
				Key:      item.Key,
				Value:    item.Value,
				ValType:  item.ValType,
				Height:   1,
			})
			if err != nil {
				return errors.Wrapf(err, "error inserting metadata with key %s", item.Key)
			}
		}

		// Retrieve and verify metadata
		for _, item := range metadataItems {
			result, err := procedure.GetMetadata(ctx, procedure.GetMetadataInput{
				Platform: platform,
				Locator:  contractInfo.Locator,
				Key:      item.Key,
				Height:   1,
			})
			if err != nil {
				return errors.Wrapf(err, "error retrieving metadata with key %s", item.Key)
			}

			if item.ValType == "int" {
				assert.Equal(t, item.Value, fmt.Sprintf("%d", *result[0].ValueI), "Metadata value should match")
			} else if item.ValType == "bool" {
				assert.Equal(t, item.Value, fmt.Sprintf("%t", *result[0].ValueB), "Metadata value should match")
			} else if item.ValType == "string" {
				assert.Equal(t, item.Value, *result[0].ValueS, "Metadata value should match")
			}
		}

		return nil
	}
}

func testMetadataInsertionThenDisableAndRetrieval(t *testing.T, contractInfo setup.StreamInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {

		// Insert metadata
		key := "temp_key"
		value := "temp_value"
		valType := "string"

		err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
			Platform: platform,
			Locator:  contractInfo.Locator,
			Key:      key,
			Value:    value,
			ValType:  valType,
		})
		if err != nil {
			return errors.Wrap(err, "error inserting metadata")
		}

		// Retrieve metadata and get row ID
		result, err := procedure.GetMetadata(ctx, procedure.GetMetadataInput{
			Platform: platform,
			Locator:  contractInfo.Locator,
			Key:      key,
		})
		if err != nil {
			return errors.Wrap(err, "error retrieving metadata")
		}
		rowID := result[0].RowID

		// Disable metadata
		err = procedure.DisableMetadata(ctx, procedure.DisableMetadataInput{
			Platform: platform,
			Locator:  contractInfo.Locator,
			RowID:    rowID,
		})
		if err != nil {
			return errors.Wrap(err, "error disabling metadata")
		}

		// Try to retrieve disabled metadata
		v, err := procedure.GetMetadata(ctx, procedure.GetMetadataInput{
			Platform: platform,
			Locator:  contractInfo.Locator,
			Key:      key,
		})
		// expect to be an empty slice
		assert.Equal(t, 0, len(v), "Disabled metadata should not be retrievable")
		assert.NoError(t, err)

		return nil
	}
}
