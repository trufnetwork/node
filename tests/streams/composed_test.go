package tests

// import (
// 	"context"
// 	"strconv"
// 	"testing"

// 	"github.com/trufnetwork/sdk-go/core/types"

// 	"github.com/kwilteam/kwil-db/common"
// 	"github.com/kwilteam/kwil-db/core/utils"
// 	kwilTesting "github.com/kwilteam/kwil-db/testing"
// 	"github.com/pkg/errors"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/trufnetwork/node/tests/streams/tests/utils/procedure"
// 	"github.com/trufnetwork/node/tests/streams/tests/utils/setup"
// 	"github.com/trufnetwork/node/tests/streams/tests/utils/table"
// 	"github.com/trufnetwork/sdk-go/core/util"
// )

// func TestComposed(t *testing.T) {
// 	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
// 		Name: "composed_test",
// 		FunctionTests: []kwilTesting.TestFunc{
// 			WithComposedTestSetup(testComposedLastAvailable(t)),
// 			WithComposedTestSetup(testCOMPOSED01SetTaxonomyWithValidData(t)),
// 			WithComposedTestSetup(testCOMPOSED02OnlyOwnerCanSetTaxonomy(t)),
// 			WithComposedTestSetup(testCOMPOSED04DisableTaxonomy(t)),
// 			WithComposedTestSetup(testOnlyOwnerCanDisableTaxonomy(t)),
// 			WithComposedTestSetup(testCOMPOSED03SetReadOnlyMetadataToComposedStream(t)),
// 		},
// 	})
// }

// func WithComposedTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) kwilTesting.TestFunc {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		// Define a valid deployer address
// 		return testFn(ctx, procedure.WithSigner(platform, composedContractInfo.Deployer.Bytes()))
// 	}
// }

// func testComposedLastAvailable(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		t.Skip("Test skipped: composed stream tests temporarily disabled")
// 		composedDBID := utils.GenerateDBID(composedStreamId.String(), platform.Deployer)

// 		// Setup data for the test
// 		err := setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
// 			Platform: platform,
// 			StreamId: composedStreamId.String(),
// 			Height:   1,
// 			MarkdownData: `
// 				| date       | Stream 1 | Stream 2 | Stream 3 |
// 				| ---------- | -------- | -------- | -------- |
// 				| 2024-08-29 | 1        |          | 4        |
// 				| 2024-08-30 |          |          |          |
// 				| 2024-08-31 |          | 2        | 5        |
// 				| 2024-09-01 |          |          | 3        |
// 			`,
// 			Weights: []string{"1", "2", "3"},
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error setting up last available test data")
// 		}

// 		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
// 			Platform: platform,
// 			DBID:     composedDBID,
// 			DateFrom: "2024-08-29",
// 			DateTo:   "2024-09-01",
// 			Height:   0,
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "error in testComposedLastAvailable")
// 		}

// 		expected := `
// 		| date       | value                  |
// 		| ---------- | ---------------------- |
// 		| 2024-08-29 | 3.250000000000000000   | # 1 & 4
// 		| 2024-08-30 |                        |
// 		| 2024-08-31 | 3.333333333333333333   | # 1 & 2 & 5
// 		| 2024-09-01 | 2.333333333333333333   | # 1 & 2 & 3
// 		`

// 		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

// 		return nil
// 	}
// }

// func testCOMPOSED01SetTaxonomyWithValidData(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		t.Skip("Test skipped: composed stream tests temporarily disabled")
// 		// Initialize contract
// 		if err := setup.SetupAndInitializeContract(ctx, platform, composedContractInfo); err != nil {
// 			return err
// 		}
// 		dbid := setup.GetDBID(composedContractInfo)

// 		stream1 := util.GenerateStreamId("stream1")
// 		stream2 := util.GenerateStreamId("stream2")

// 		// deploy child streams
// 		if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
// 			Platform: platform,
// 			StreamId: stream1,
// 			Height:   1,
// 			MarkdownData: `
// 				| date       | value |
// 				| ---------- | ----- |
// 				| 2024-01-01 | 5     |
// 				| 2024-01-05 | 15    |
// 			`,
// 		}); err != nil {
// 			return errors.Wrap(err, "error setting up child stream 1")
// 		}

// 		if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
// 			Platform: platform,
// 			StreamId: stream2,
// 			Height:   1,
// 			MarkdownData: `
// 				| date       | value |
// 				| ---------- | ----- |
// 				| 2024-01-01 | 2     |
// 				| 2024-01-05 | 10    |
// 			`,
// 		}); err != nil {
// 			return errors.Wrap(err, "error setting up child stream 2")
// 		}

// 		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
// 		if err != nil {
// 			return errors.Wrap(err, "error creating ethereum address from bytes")
// 		}

// 		// Set up child streams
// 		childStreams := types.Taxonomy{
// 			TaxonomyItems: []types.TaxonomyItem{
// 				{ChildStream: types.StreamLocator{DataProvider: deployer, StreamId: stream1}, Weight: 1.0},
// 				{ChildStream: types.StreamLocator{DataProvider: deployer, StreamId: stream2}, Weight: 2.0},
// 			},
// 		}

// 		// Set taxonomy
// 		err = setTaxonomy(ctx, platform, dbid, childStreams)
// 		if err != nil {
// 			return errors.Wrap(err, "Failed to set taxonomy")
// 		}

// 		// Verify taxonomy is applied in get_record
// 		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
// 			Platform: platform,
// 			DBID:     dbid,
// 			DateFrom: "2024-01-01",
// 			DateTo:   "2024-01-31",
// 			Height:   0,
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "Failed to get record after setting taxonomy")
// 		}

// 		// Expected results based on child streams and weights
// 		// Assuming child stream1 has weight 1.0 and stream2 has weight 2.0
// 		// The composed value should be (value_stream1 * 1.0) + (value_stream2 * 2.0)
// 		expected := `
// 		| date       | value                  |
// 		| ---------- | ---------------------- |
// 		| 2024-01-01 | 3.000000000000000000   | # (5 * 1.0 + 2 * 2.0) / (1.0 + 2.0) = 9.0 / 3.0 = 3.0
// 		| 2024-01-05 | 11.666666666666666667   | # (15 * 1.0 + 10 * 2.0) / (1.0 + 2.0) = 35.0 / 3.0 = 11.666666666666666667
// 		`

// 		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

// 		return nil
// 	}
// }

// func testCOMPOSED02OnlyOwnerCanSetTaxonomy(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		t.Skip("Test skipped: composed stream tests temporarily disabled")
// 		// Initialize contract
// 		if err := setup.SetupAndInitializeContract(ctx, platform, composedContractInfo); err != nil {
// 			return err
// 		}
// 		dbid := setup.GetDBID(composedContractInfo)

// 		// Use a non-owner account
// 		nonOwner := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000001000101")

// 		// Attempt to set taxonomy
// 		childStreams := types.Taxonomy{
// 			TaxonomyItems: []types.TaxonomyItem{
// 				{ChildStream: types.StreamLocator{DataProvider: util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000001"), StreamId: util.GenerateStreamId("stream1")}, Weight: 1.0},
// 			},
// 		}

// 		err := setTaxonomy(ctx, procedure.WithSigner(platform, nonOwner.Bytes()), dbid, childStreams)
// 		assert.Error(t, err, "Non-owner should not be able to set taxonomy")
// 		assert.Contains(t, err.Error(), "Stream owner only procedure", "Expected owner-only error")

// 		return nil
// 	}
// }

// func testCOMPOSED04DisableTaxonomy(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		t.Skip("Test skipped: composed stream tests temporarily disabled")
// 		// Initialize contract
// 		if err := setup.SetupAndInitializeContract(ctx, platform, composedContractInfo); err != nil {
// 			return err
// 		}
// 		dbid := setup.GetDBID(composedContractInfo)

// 		//  setup primitive streams
// 		stream1 := util.GenerateStreamId("stream1")
// 		if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
// 			Platform: platform,
// 			StreamId: util.GenerateStreamId("stream1"),
// 			Height:   1,
// 			MarkdownData: `
// 				| date       | value |
// 				| ---------- | ----- |
// 				| 2024-01-01 | 5     |
// 			`,
// 		}); err != nil {
// 			return errors.Wrap(err, "error setting up child stream 1")
// 		}

// 		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
// 		if err != nil {
// 			return errors.Wrap(err, "error creating ethereum address from bytes")
// 		}

// 		// Set taxonomy version 1
// 		childStreams := types.Taxonomy{
// 			TaxonomyItems: []types.TaxonomyItem{
// 				{ChildStream: types.StreamLocator{DataProvider: deployer, StreamId: stream1}, Weight: 1.0},
// 			},
// 		}

// 		err = setTaxonomy(ctx, platform, dbid, childStreams)
// 		if err != nil {
// 			return errors.Wrap(err, "Failed to set taxonomy version 1")
// 		}

// 		// Disable taxonomy version 1
// 		err = disableTaxonomy(ctx, platform, dbid, 1)
// 		if err != nil {
// 			return errors.Wrap(err, "Failed to disable taxonomy version 1")
// 		}

// 		// Attempt to retrieve data after disabling taxonomy
// 		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
// 			Platform: platform,
// 			DBID:     dbid,
// 			DateFrom: "2024-01-01",
// 			DateTo:   "2024-01-31",
// 			Height:   0,
// 		})
// 		if err != nil {
// 			return errors.Wrap(err, "Failed to get record after disabling taxonomy")
// 		}

// 		// Assert that no data is returned or matches expectations
// 		expected := `
// 		| date       | value |
// 		| ---------- | ----- |
// 		`

// 		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

// 		return nil
// 	}
// }

// func testOnlyOwnerCanDisableTaxonomy(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		t.Skip("Test skipped: composed stream tests temporarily disabled")
// 		// Initialize contract
// 		if err := setup.SetupAndInitializeContract(ctx, platform, composedContractInfo); err != nil {
// 			return err
// 		}
// 		dbid := setup.GetDBID(composedContractInfo)

// 		// Use a non-owner account
// 		nonOwner := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000001000001")

// 		// Attempt to disable taxonomy
// 		err := disableTaxonomy(ctx, procedure.WithSigner(platform, nonOwner.Bytes()), dbid, 1)
// 		assert.Error(t, err, "Non-owner should not be able to disable taxonomy")
// 		assert.Contains(t, err.Error(), "Stream owner only procedure", "Expected owner-only error")

// 		return nil
// 	}
// }

// // setTaxonomy sets the taxonomy for a composed stream
// func setTaxonomy(ctx context.Context, platform *kwilTesting.Platform, dbid string, taxonomies types.Taxonomy) error {
// 	dataProviders := make([]string, len(taxonomies.TaxonomyItems))
// 	streamIDs := make([]string, len(taxonomies.TaxonomyItems))
// 	decimalWeights := make([]string, len(taxonomies.TaxonomyItems))
// 	for i, cs := range taxonomies.TaxonomyItems {
// 		dataProviders[i] = cs.ChildStream.DataProvider.Address()
// 		streamIDs[i] = cs.ChildStream.StreamId.String()
// 		decimalWeights[i] = strconv.FormatFloat(cs.Weight, 'f', -1, 64)
// 	}

// 	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
// 	if err != nil {
// 		return errors.Wrap(err, "Failed to create Ethereum address from bytes")
// 	}

// 	var startDate string
// 	if taxonomies.StartDate != nil {
// 		startDate = taxonomies.StartDate.String()
// 	}

// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: 0},
// 		Signer:       deployer.Bytes(),
// 		Caller:       deployer.Address(),
// 		TxID:         platform.Txid(),
// 	}

// 	_, err = platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
// 		Procedure: "set_taxonomy",
// 		Dataset:   dbid,
// 		Args:      []any{dataProviders, streamIDs, decimalWeights, startDate},
// 	})
// 	if err != nil {
// 		return errors.Wrap(err, "Failed to execute set_taxonomy procedure")
// 	}
// 	return nil
// }

// // disableTaxonomy disables a specific taxonomy version
// func disableTaxonomy(ctx context.Context, platform *kwilTesting.Platform, dbid string, version int) error {
// 	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
// 	if err != nil {
// 		return errors.Wrap(err, "Failed to create Ethereum address from bytes")
// 	}

// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: 0},
// 		Signer:       deployer.Bytes(),
// 		Caller:       deployer.Address(),
// 		TxID:         platform.Txid(),
// 	}

// 	_, err = platform.Engine.Procedure(txContext, platform.DB, &common.ExecutionData{
// 		Procedure: "disable_taxonomy",
// 		Dataset:   dbid,
// 		Args:      []any{version},
// 	})
// 	if err != nil {
// 		return errors.Wrap(err, "Failed to execute disable_taxonomy procedure")
// 	}
// 	return nil
// }

// func testCOMPOSED03SetReadOnlyMetadataToComposedStream(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
// 	return func(ctx context.Context, platform *kwilTesting.Platform) error {
// 		t.Skip("Test skipped: composed stream tests temporarily disabled")
// 		dbid := utils.GenerateDBID(composedStreamId.String(), platform.Deployer)

// 		// Attempt to set metadata
// 		err := procedure.SetMetadata(ctx, procedure.SetMetadataInput{
// 			Platform: platform,
// 			DBID:     dbid,
// 			Key:      "type",
// 			Value:    "other",
// 			ValType:  "string",
// 			Height:   0,
// 		})
// 		assert.Error(t, err, "Cannot insert metadata for read-only key")
// 		return nil
// 	}
// }
