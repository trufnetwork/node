package tests

// import (
// 	"context"
// 	"testing"

// 	"github.com/pkg/errors"
// 	"github.com/stretchr/testify/assert"

// 	"github.com/kwilteam/kwil-db/common"
// 	"github.com/kwilteam/kwil-db/core/utils"
// 	"github.com/kwilteam/kwil-db/parse"
// 	kwilTesting "github.com/kwilteam/kwil-db/testing"

// 	"github.com/trufnetwork/node/tests/streams/tests/utils/procedure"
// 	"github.com/trufnetwork/node/tests/streams/tests/utils/setup"
// 	"github.com/trufnetwork/sdk-go/core/util"
// )

// const (
// 	systemContractName = "system_contract"
// )

// var systemContractDeployer = util.Unsafe_NewEthereumAddressFromString("0x1234567890123456789012345678901234567890")

// func TestSystemContract(t *testing.T) {
// 	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
// 		Name: "system_contract_test",
// 		FunctionTests: []kwilTesting.TestFunc{
// 			testDeployContract(t),
// 			testAcceptAndRevokeStream(t),
// 			testCannotAcceptInexistentStream(t),
// 			testGetUnsafeMethods(t),
// 			testGetSafeMethods(t),
// 		},
// 	})
// }

// // setupSystemContract initializes the system contract for testing.
// func setupSystemContract(ctx context.Context, platform *kwilTesting.Platform) error {
// 	platform.Deployer = systemContractDeployer.Bytes()
// 	return deploySystemContract(ctx, platform)
// }

// func testDeployContract(t *testing.T) kwilTesting.TestFunc {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		t.Skip("Test skipped: system contract tests temporarily disabled")
// 		if err := setupSystemContract(ctx, platform); err != nil {
// 			return err
// 		}

// 		exists, err := checkContractExists(ctx, platform, systemContractName)
// 		assert.NoError(t, err, "Error checking contract existence")
// 		assert.True(t, exists, "System contract should be deployed")

// 		return nil
// 	}
// }

// func testAcceptAndRevokeStream(t *testing.T) kwilTesting.TestFunc {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		t.Skip("Test skipped: system contract tests temporarily disabled")
// 		if err := setupSystemContract(ctx, platform); err != nil {
// 			return err
// 		}

// 		dataProvider := getDataProvider()
// 		streamID := util.GenerateStreamId("primitive_stream")

// 		// Deploy primitive stream with data
// 		if err := deployPrimitiveStreamWithData(ctx, procedure.WithSigner(platform, dataProvider.Bytes()), "primitive_stream", 1); err != nil {
// 			return errors.Wrap(err, "Failed to deploy primitive stream")
// 		}

// 		// Accept the stream
// 		if err := executeAcceptStream(ctx, platform, dataProvider, streamID); err != nil {
// 			return errors.Wrap(err, "Failed to accept stream")
// 		}

// 		// Verify acceptance
// 		accepted, err := isStreamAccepted(ctx, platform, dataProvider, streamID)
// 		assert.NoError(t, err, "Error verifying stream acceptance")
// 		assert.True(t, accepted, "Stream should be accepted")

// 		// Revoke the stream
// 		if err := executeRevokeStream(ctx, platform, dataProvider, streamID); err != nil {
// 			return errors.Wrap(err, "Failed to revoke stream")
// 		}

// 		// Verify revocation
// 		accepted, err = isStreamAccepted(ctx, platform, dataProvider, streamID)
// 		assert.NoError(t, err, "Error verifying stream revocation")
// 		assert.False(t, accepted, "Stream should be revoked")

// 		return nil
// 	}
// }

// func testCannotAcceptInexistentStream(t *testing.T) kwilTesting.TestFunc {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		t.Skip("Test skipped: system contract tests temporarily disabled")
// 		if err := setupSystemContract(ctx, platform); err != nil {
// 			return err
// 		}

// 		dataProvider := getDataProvider()
// 		nonExistentStreamID := util.GenerateStreamId("inexistent_stream")

// 		err := executeAcceptStream(ctx, platform, dataProvider, nonExistentStreamID)
// 		assert.Error(t, err, "Should not be able to accept a nonexistent stream")

// 		return nil
// 	}
// }

// func testGetUnsafeMethods(t *testing.T) kwilTesting.TestFunc {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		t.Skip("Test skipped: system contract tests temporarily disabled")
// 		if err := setupSystemContract(ctx, platform); err != nil {
// 			return err
// 		}

// 		dataProvider := getDataProvider()
// 		streamID := util.GenerateStreamId("primitive_stream")

// 		// Deploy the stream
// 		if err := deployPrimitiveStreamWithData(ctx, procedure.WithSigner(platform, dataProvider.Bytes()), "primitive_stream", 1); err != nil {
// 			return errors.Wrap(err, "Failed to deploy primitive stream")
// 		}

// 		// Get unsafe record
// 		recordResult, err := executeGetUnsafeRecord(ctx, platform, dataProvider, streamID, "2021-01-01", "2021-01-05", 0)
// 		assert.NoError(t, err, "Failed to get unsafe record")
// 		assert.NotEmpty(t, recordResult, "Unsafe record should return data")

// 		// Get unsafe index
// 		indexResult, err := executeGetUnsafeIndex(ctx, platform, dataProvider, streamID, "2021-01-01", "2021-01-05", 0)
// 		assert.NoError(t, err, "Failed to get unsafe index")
// 		assert.NotEmpty(t, indexResult, "Unsafe index should return data")

// 		return nil
// 	}
// }

// func testGetSafeMethods(t *testing.T) kwilTesting.TestFunc {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		t.Skip("Test skipped: system contract tests temporarily disabled")
// 		if err := setupSystemContract(ctx, platform); err != nil {
// 			return err
// 		}

// 		dataProvider := getDataProvider()
// 		streamName := "primitive_stream"
// 		streamID := util.GenerateStreamId(streamName)

// 		// Deploy the stream
// 		if err := deployPrimitiveStreamWithData(ctx, procedure.WithSigner(platform, dataProvider.Bytes()), streamName, 1); err != nil {
// 			return errors.Wrap(err, "Failed to deploy primitive stream")
// 		}

// 		// Accept the stream
// 		if err := executeAcceptStream(ctx, platform, dataProvider, streamID); err != nil {
// 			return errors.Wrap(err, "Failed to accept stream for safe methods")
// 		}

// 		// Get safe record
// 		recordResult, err := executeGetRecord(ctx, platform, dataProvider, streamID, "2021-01-01", "2021-01-05", 0)
// 		assert.NoError(t, err, "Failed to get safe record")
// 		assert.NotEmpty(t, recordResult, "Safe record should return data")

// 		// Get safe index
// 		indexResult, err := executeGetIndex(ctx, platform, dataProvider, streamID, "2021-01-01", "2021-01-05", 0)
// 		assert.NoError(t, err, "Failed to get safe index")
// 		assert.NotEmpty(t, indexResult, "Safe index should return data")

// 		// Revoke the stream
// 		if err := executeRevokeStream(ctx, platform, dataProvider, streamID); err != nil {
// 			return errors.Wrap(err, "Failed to revoke stream for safe methods test")
// 		}

// 		// Attempt to get safe record after revocation
// 		_, err = executeGetRecord(ctx, platform, dataProvider, streamID, "2021-01-01", "2021-01-05", 0)
// 		assert.Error(t, err, "Should not get safe record from a revoked stream")

// 		return nil
// 	}
// }

// // Helper functions for deploying contracts and executing procedures

// func deploySystemContract(ctx context.Context, platform *kwilTesting.Platform) error {
// 	schema, err := parse.Parse(contracts.SystemContractContent)
// 	if err != nil {
// 		return errors.Wrap(err, "Failed to parse system contract")
// 	}
// 	schema.Name = systemContractName

// 	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
// 	if err != nil {
// 		return errors.Wrap(err, "Failed to create system contract deployer")
// 	}

// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: 2},
// 		Signer:       deployer.Bytes(),
// 		Caller:       deployer.Address(),
// 		TxID:         platform.Txid(),
// 	}

// 	return platform.Engine.CreateDataset(txContext, platform.DB, schema)
// }

// func checkContractExists(ctx context.Context, platform *kwilTesting.Platform, contractName string) (bool, error) {
// 	schemas, err := platform.Engine.ListDatasets(platform.Deployer)
// 	if err != nil {
// 		return false, err
// 	}
// 	for _, schema := range schemas {
// 		if schema.Name == contractName {
// 			return true, nil
// 		}
// 	}
// 	return false, nil
// }

// func getDataProvider() util.EthereumAddress {
// 	return util.Unsafe_NewEthereumAddressFromString("0xfC43f5F9dd45258b3AFf31Bdbe6561D97e8B71de")
// }

// func deployPrimitiveStreamWithData(ctx context.Context, platform *kwilTesting.Platform, streamName string, height int64) error {
// 	return setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
// 		Platform: platform,
// 		Height:   height,
// 		StreamId: util.GenerateStreamId(streamName),
// 		MarkdownData: `
// | date       | value |
// |------------|-------|
// | 2021-01-01 | 1     | # Minimal data for testing
// `,
// 	})
// }

// func executeAcceptStream(ctx context.Context, platform *kwilTesting.Platform, dataProvider util.EthereumAddress, streamID util.StreamId) error {
// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: 3},
// 		Signer:       platform.Deployer,
// 		TxID:         platform.Txid(),
// 	}

// 	_, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
// 		Procedure: "accept_stream",
// 		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
// 		Args:      []any{dataProvider.Address(), streamID.String()},
// 	})
// 	return err
// }

// func executeRevokeStream(ctx context.Context, platform *kwilTesting.Platform, dataProvider util.EthereumAddress, streamID util.StreamId) error {
// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: 4},
// 		Signer:       platform.Deployer,
// 		TxID:         platform.Txid(),
// 	}

// 	_, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
// 		Procedure: "revoke_stream",
// 		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
// 		Args:      []any{dataProvider.Address(), streamID.String()},
// 	})
// 	return err
// }

// func isStreamAccepted(ctx context.Context, platform *kwilTesting.Platform, dataProvider util.EthereumAddress, streamID util.StreamId) (bool, error) {
// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: 5},
// 		Signer:       platform.Deployer,
// 		TxID:         platform.Txid(),
// 	}

// 	result, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
// 		Procedure: "get_official_stream",
// 		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
// 		Args:      []any{dataProvider.Address(), streamID.String()},
// 	})
// 	if err != nil {
// 		return false, err
// 	}
// 	if len(result.Rows) == 0 {
// 		return false, nil
// 	}
// 	return result.Rows[0][0].(bool), nil
// }

// func executeGetUnsafeRecord(ctx context.Context, platform *kwilTesting.Platform, dataProvider util.EthereumAddress, streamID util.StreamId, dateFrom, dateTo string, frozenAt int64) ([][]any, error) {
// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: 6},
// 		Signer:       platform.Deployer,
// 		TxID:         platform.Txid(),
// 	}

// 	result, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
// 		Procedure: "get_unsafe_record",
// 		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
// 		Args:      []any{dataProvider.Address(), streamID.String(), dateFrom, dateTo, frozenAt},
// 	})
// 	return result.Rows, err
// }

// func executeGetUnsafeIndex(ctx context.Context, platform *kwilTesting.Platform, dataProvider util.EthereumAddress, streamID util.StreamId, dateFrom, dateTo string, frozenAt int64) ([][]any, error) {
// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: 7},
// 		Signer:       platform.Deployer,
// 		TxID:         platform.Txid(),
// 	}

// 	result, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
// 		Procedure: "get_unsafe_index",
// 		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
// 		Args:      []any{dataProvider.Address(), streamID.String(), dateFrom, dateTo, frozenAt, nil},
// 	})
// 	return result.Rows, err
// }

// func executeGetRecord(ctx context.Context, platform *kwilTesting.Platform, dataProvider util.EthereumAddress, streamID util.StreamId, dateFrom, dateTo string, frozenAt int64) ([][]any, error) {
// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: 8},
// 		Signer:       platform.Deployer,
// 		TxID:         platform.Txid(),
// 	}

// 	result, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
// 		Procedure: "get_record",
// 		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
// 		Args:      []any{dataProvider.Address(), streamID.String(), dateFrom, dateTo, frozenAt},
// 	})

// 	// can't just return result.Rows, err, otherwise we get a nil pointer dereference
// 	if err != nil {
// 		return nil, err
// 	}

// 	return result.Rows, nil
// }

// func executeGetIndex(ctx context.Context, platform *kwilTesting.Platform, dataProvider util.EthereumAddress, streamID util.StreamId, dateFrom, dateTo string, frozenAt int64) ([][]any, error) {
// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: 9},
// 		Signer:       platform.Deployer,
// 		TxID:         platform.Txid(),
// 	}

// 	result, err := platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
// 		Procedure: "get_index",
// 		Dataset:   utils.GenerateDBID(systemContractName, platform.Deployer),
// 		Args:      []any{dataProvider.Address(), streamID.String(), dateFrom, dateTo, frozenAt, nil},
// 	})
// 	return result.Rows, err
// }
