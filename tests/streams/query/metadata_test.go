package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
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

// TestQUERY04Metadata tests the insertion and retrieval of metadata
// for both primitive and composed streams. Also tests disabling metadata and attempting to retrieve it.
func TestQUERY04Metadata(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "metadata_insertion_and_retrieval",
		FunctionTests: []kwilTesting.TestFunc{
			WithMetadataTestSetup(testMetadataInsertionAndRetrieval(t, primitiveContractInfo)),
			WithMetadataTestSetup(testMetadataInsertionThenDisableAndRetrieval(t, primitiveContractInfo)),
			WithMetadataTestSetup(testListMetadataByHeight(t, primitiveContractInfo)),
			WithMetadataTestSetup(testListMetadataByHeightNoKey(t, primitiveContractInfo)),
			WithMetadataTestSetup(testListMetadataByHeightPagination(t, primitiveContractInfo)),
			WithMetadataTestSetup(testListMetadataByHeightInvalidRange(t, primitiveContractInfo)),
			WithMetadataTestSetup(testListMetadataByHeightInvalidPagination(t, primitiveContractInfo)),
		},
		SeedScripts: migrations.GetSeedScriptPaths(),
	}, testutils.GetTestOptionsWithCache())
}

func WithMetadataTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, primitiveContractInfo.Locator.DataProvider.Bytes())
		err := setup.CreateDataProvider(ctx, platform, primitiveContractInfo.Locator.DataProvider.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}
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

func testListMetadataByHeight(t *testing.T, contractInfo setup.StreamInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		metadataKey := "test_metadata"
		metadataItems := []struct {
			Key     string
			Value   string
			ValType string
		}{
			{metadataKey, "1", "int"},
			{metadataKey, "0", "int"},
		}

		for i, item := range metadataItems {
			err := procedure.InsertMetadata(ctx, procedure.InsertMetadataInput{
				Platform: platform,
				Locator:  contractInfo.Locator,
				Key:      item.Key,
				Value:    item.Value,
				ValType:  item.ValType,
				Height:   int64(i + 1),
			})
			if err != nil {
				return errors.Wrapf(err, "error inserting metadata with key %s", item.Key)
			}
		}

		result, err := procedure.ListMetadataByHeight(ctx, procedure.ListMetadataByHeightInput{
			Platform: platform,
			Key:      metadataKey,
			Height:   int64(len(metadataItems)),
		})
		if err != nil {
			return errors.Wrapf(err, "error listing metadata")
		}

		expected := `
		| value_i | value_f | value_b | value_s | value_ref | created_at |
		|---------|---------|---------|---------|-----------|------------|
		| 0 | <nil> | <nil> | <nil> | <nil> | 1 |
		| 1 | <nil> | <nil> | <nil> | <nil> | 2 |`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:          result,
			Expected:        expected,
			ExcludedColumns: []int{0, 1, 2},
		})

		return nil
	}
}

func testListMetadataByHeightNoKey(t *testing.T, contractInfo setup.StreamInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		metadataKey := "test_metadata"
		metadataItems := []struct {
			Key     string
			Value   string
			ValType string
		}{
			{metadataKey, "1", "int"},
			{metadataKey, "0", "int"},
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

		result, err := procedure.ListMetadataByHeight(ctx, procedure.ListMetadataByHeightInput{
			Platform: platform,
			Height:   1,
		})
		if err != nil {
			return errors.Wrapf(err, "error listing metadata")
		}

		if len(result) > 0 { // should return no rows
			return errors.New("expected empty results")
		}

		return nil
	}
}

func testListMetadataByHeightPagination(t *testing.T, contractInfo setup.StreamInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		metadataKey := "test_metadata"
		metadataItems := []struct {
			Key     string
			Value   string
			ValType string
		}{
			{metadataKey, "1", "int"},
			{metadataKey, "0", "int"},
			{metadataKey, "1", "int"},
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

		limit := 2
		offset := 0
		result, err := procedure.ListMetadataByHeight(ctx, procedure.ListMetadataByHeightInput{
			Platform: platform,
			Key:      metadataKey,
			Limit:    &limit,
			Offset:   &offset,
			Height:   1,
		})
		if err != nil {
			return errors.Wrapf(err, "error listing metadata")
		}

		if len(result) != 2 {
			return errors.Errorf("expected 2 results, got %d", len(result))
		}

		offset = 2
		result, err = procedure.ListMetadataByHeight(ctx, procedure.ListMetadataByHeightInput{
			Platform: platform,
			Key:      metadataKey,
			Limit:    &limit,
			Offset:   &offset,
			Height:   1,
		})
		if err != nil {
			return errors.Wrapf(err, "error listing metadata")
		}

		if len(result) != 1 {
			return errors.Errorf("expected 1 result, got %d", len(result))
		}

		return nil
	}
}

func testListMetadataByHeightInvalidRange(t *testing.T, contractInfo setup.StreamInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		metadataKey := "test_metadata"
		metadataItems := []struct {
			Key     string
			Value   string
			ValType string
		}{
			{metadataKey, "1", "int"},
			{metadataKey, "0", "int"},
			{metadataKey, "1", "int"},
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

		fromHeight := int64(10)
		toHeight := int64(5)
		_, err := procedure.ListMetadataByHeight(ctx, procedure.ListMetadataByHeightInput{
			Platform:   platform,
			Key:        metadataKey,
			FromHeight: &fromHeight,
			ToHeight:   &toHeight,
			Height:     1,
		})

		if err == nil {
			return errors.New("expected error for invalid height range (from_height > to_height), but got none")
		}

		expectedError := "Invalid height range"
		if !strings.Contains(err.Error(), expectedError) {
			return errors.Errorf("expected error message to contain '%s', got: %s", expectedError, err.Error())
		}

		return nil
	}
}

func testListMetadataByHeightInvalidPagination(t *testing.T, contractInfo setup.StreamInfo) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		metadataKey := "test_metadata"
		metadataItems := []struct {
			Key     string
			Value   string
			ValType string
		}{
			{metadataKey, "1", "int"},
			{metadataKey, "0", "int"},
			{metadataKey, "1", "int"},
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

		// negative limit
		limit := -10
		result, err := procedure.ListMetadataByHeight(ctx, procedure.ListMetadataByHeightInput{
			Platform: platform,
			Key:      metadataKey,
			Limit:    &limit,
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "unexpected error with negative limit")
		}

		if len(result) != 0 {
			return errors.Errorf("expected empty results with negative limit (converted to 0), got %d results", len(result))
		}

		negativeOffset := -5
		_, err = procedure.ListMetadataByHeight(ctx, procedure.ListMetadataByHeightInput{
			Platform: platform,
			Key:      metadataKey,
			Offset:   &negativeOffset,
			Height:   1,
		})
		if err != nil {
			return errors.Wrap(err, "unexpected error with negative offset")
		}

		return nil
	}
}
