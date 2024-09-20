package tests

import (
	"context"
	"testing"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/parse"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/truflation/tsn-db/internal/contracts"
	"github.com/truflation/tsn-db/internal/contracts/tests/utils/setup"
	"github.com/truflation/tsn-sdk/core/util"
)

const systemContractName = "system_contract"

func TestSystemContract(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "system_contract_test",
		FunctionTests: []kwilTesting.TestFunc{
			testDeployContract(t),
			testAcceptAndRevokeStream(t),
			testCannotAcceptInexistentStream(t),
			testGetUnsafeMethods(t),
			testGetSafeMethods(t),
		},
	})
}

func testDeployContract(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		err := deploySystemContract(ctx, platform)
		assert.NoError(t, err, "Failed to deploy system contract")

		exists, err := checkContractExists(ctx, platform, systemContractName)
		assert.NoError(t, err, "Error checking contract existence")
		assert.True(t, exists, "System contract should be deployed")

		return nil
	}
}

func testAcceptAndRevokeStream(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dataProvider := getDataProvider(platform)
		streamID := "primitive_stream_000000000000001"

		// Accept the stream
		err := executeAcceptStream(ctx, platform, dataProvider, streamID)
		assert.NoError(t, err, "Failed to accept stream")

		// Verify acceptance
		accepted, err := isStreamAccepted(ctx, platform, dataProvider, streamID)
		assert.NoError(t, err, "Error verifying stream acceptance")
		assert.True(t, accepted, "Stream should be accepted")

		// Revoke the stream
		err = executeRevokeStream(ctx, platform, dataProvider, streamID)
		assert.NoError(t, err, "Failed to revoke stream")

		// Verify revocation
		accepted, err = isStreamAccepted(ctx, platform, dataProvider, streamID)
		assert.NoError(t, err, "Error verifying stream revocation")
		assert.False(t, accepted, "Stream should be revoked")

		return nil
	}
}

func testCannotAcceptInexistentStream(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dataProvider := "fC43f5F9dd45258b3AFf31Bdbe6561D97e8B71de"
		inexistentStreamID := "st123456789012345678901234567890"

		err := executeAcceptStream(ctx, platform, dataProvider, inexistentStreamID)
		assert.Error(t, err, "Should not be able to accept an inexistent stream")

		return nil
	}
}

func testGetUnsafeMethods(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dataProvider := getDataProvider(platform)
		streamID := "primitive_stream_000000000000001"

		// Get unsafe record
		recordResult, err := executeGetUnsafeRecord(ctx, platform, dataProvider, streamID, "2021-01-01", "2021-01-05", 0)
		assert.NoError(t, err, "Failed to get unsafe record")
		assert.NotEmpty(t, recordResult, "Unsafe record should return data")

		// Get unsafe index
		indexResult, err := executeGetUnsafeIndex(ctx, platform, dataProvider, streamID, "2021-01-01", "2021-01-05", 0)
		assert.NoError(t, err, "Failed to get unsafe index")
		assert.NotEmpty(t, indexResult, "Unsafe index should return data")

		return nil
	}
}

func testGetSafeMethods(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dataProvider := getDataProvider(platform)
		streamID := "primitive_stream_000000000000001"

		// Ensure the stream is accepted
		err := executeAcceptStream(ctx, platform, dataProvider, streamID)
		assert.NoError(t, err, "Failed to accept stream for safe methods")

		err := deployPrimitiveStreamWithData(ctx, platform, dataProvider, streamID)
		assert.NoError(t, err, "Failed to deploy primitive stream")

		// Get safe record
		recordResult, err := executeGetRecord(ctx, platform, dataProvider, streamID, "2021-01-01", "2021-01-05", 0)
		assert.NoError(t, err, "Failed to get safe record")
		assert.NotEmpty(t, recordResult, "Safe record should return data")

		// Get safe index
		indexResult, err := executeGetIndex(ctx, platform, dataProvider, streamID, "2021-01-01", "2021-01-05", 0)
		assert.NoError(t, err, "Failed to get safe index")
		assert.NotEmpty(t, indexResult, "Safe index should return data")

		// Revoke the stream to test failure
		err = executeRevokeStream(ctx, platform, dataProvider, streamID)
		assert.NoError(t, err, "Failed to revoke stream for safe methods test")

		// Attempt to get safe record after revocation
		_, err = executeGetRecord(ctx, platform, dataProvider, streamID, "2021-01-01", "2021-01-05", 0)
		assert.Error(t, err, "Should not get safe record from a revoked stream")

		return nil
	}
}

// Helper functions

func deploySystemContract(ctx context.Context, platform *kwilTesting.Platform) error {
	err := platform.Engine.DeleteDataset(ctx, platform.DB, systemContractName, &common.TransactionData{
		Signer: platform.Deployer,
		TxID:   platform.Txid(),
		Height: 1,
	})
	if err != nil {
		// It's okay if the dataset doesn't exist
	}

	schema, err := parse.Parse(contracts.SystemContractContent)
	if err != nil {
		return errors.Wrap(err, "failed to parse system contract")
	}

	err = platform.Engine.CreateDataset(ctx, platform.DB, schema, &common.TransactionData{
		Signer: platform.Deployer,
		TxID:   platform.Txid(),
		Height: 2,
	})
	return err
}

func checkContractExists(ctx context.Context, platform *kwilTesting.Platform, contractName string) (bool, error) {
	schemas, err := platform.Engine.ListDatasets(platform.Deployer)
	if err != nil {
		return false, err
	}
	for _, schema := range schemas {
		if schema.Name == contractName {
			return true, nil
		}
	}
	return false, nil
}

func getDataProvider(platform *kwilTesting.Platform) string {
	return "fC43f5F9dd45258b3AFf31Bdbe6561D97e8B71de"
}

func executeAcceptStream(ctx context.Context, platform *kwilTesting.Platform, dataProvider, streamID string) error {
	_, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: "accept_stream",
		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
		Args:      []any{dataProvider, streamID},
		TransactionData: common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 3,
		},
	})
	return err
}

func executeRevokeStream(ctx context.Context, platform *kwilTesting.Platform, dataProvider, streamID string) error {
	_, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: "revoke_stream",
		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
		Args:      []any{dataProvider, streamID},
		TransactionData: common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 4,
		},
	})
	return err
}

func isStreamAccepted(ctx context.Context, platform *kwilTesting.Platform, dataProvider, streamID string) (bool, error) {
	result, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: "get_official_stream",
		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
		Args:      []any{dataProvider, streamID},
		TransactionData: common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 5,
		},
	})
	if err != nil {
		return false, err
	}
	if len(result.Rows) == 0 {
		return false, nil
	}
	return result.Rows[0][0].(bool), nil
}

func executeGetUnsafeRecord(ctx context.Context, platform *kwilTesting.Platform, dataProvider, streamID, dateFrom, dateTo string, frozenAt int64) ([][]any, error) {
	result, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: "get_unsafe_record",
		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
		Args:      []any{dataProvider, streamID, dateFrom, dateTo, frozenAt},
		TransactionData: common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 6,
		},
	})
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
}

func executeGetUnsafeIndex(ctx context.Context, platform *kwilTesting.Platform, dataProvider, streamID, dateFrom, dateTo string, frozenAt int64) ([][]any, error) {
	result, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: "get_unsafe_index",
		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
		Args:      []any{dataProvider, streamID, dateFrom, dateTo, frozenAt},
		TransactionData: common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 7,
		},
	})
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
}

func executeGetRecord(ctx context.Context, platform *kwilTesting.Platform, dataProvider, streamID, dateFrom, dateTo string, frozenAt int64) ([][]any, error) {
	result, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: "get_record",
		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
		Args:      []any{dataProvider, streamID, dateFrom, dateTo, frozenAt},
		TransactionData: common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 8,
		},
	})
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
}

func executeGetIndex(ctx context.Context, platform *kwilTesting.Platform, dataProvider, streamID, dateFrom, dateTo string, frozenAt int64) ([][]any, error) {
	result, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: "get_index",
		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
		Args:      []any{dataProvider, streamID, dateFrom, dateTo, frozenAt},
		TransactionData: common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 9,
		},
	})
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
}

func deployPrimitiveStreamWithData(ctx context.Context, platform *kwilTesting.Platform, dataProvider string, streamName string) error {
	// Setup initial data
	err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
		Platform:            platform,
		PrimitiveStreamName: streamName,
		Height:              1,
		MarkdownData: `
			| date       | value |
			|------------|-------|
			| 2021-01-01 | 1     |
			| 2021-01-02 | 2     |
			| 2021-01-03 | 4     |
			| 2021-01-04 | 5     |
			| 2021-01-05 | 3     |
			`,
	})
	if err != nil {
		return errors.Wrap(err, "error setting up primitive stream")
	}
}
