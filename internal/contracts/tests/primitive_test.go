package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	testutils "github.com/trufnetwork/node/internal/contracts/tests/utils"
	"github.com/trufnetwork/node/internal/contracts/tests/utils/procedure"
	"github.com/trufnetwork/node/internal/contracts/tests/utils/setup"
	"github.com/trufnetwork/node/internal/contracts/tests/utils/table"

	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"

	"github.com/pkg/errors"

	"github.com/kwilteam/kwil-db/core/utils"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
)

const primitiveStreamName = "primitive_stream_000000000000001"

var primitiveStreamId = util.GenerateStreamId(primitiveStreamName)

func TestPrimitiveStream(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "primitive_test",
		FunctionTests: []kwilTesting.TestFunc{
			WithPrimitiveTestSetup(testPRIMITIVE01_InsertAndGetRecord(t)),
			WithPrimitiveTestSetup(testPRIMITIVE02_UnauthorizedInserts(t)),
			WithPrimitiveTestSetup(testGetIndex(t)),
			WithPrimitiveTestSetup(testGetIndexChange(t)),
			WithPrimitiveTestSetup(testGetFirstRecord(t)),
			WithPrimitiveTestSetup(testDuplicateDate(t)),
			WithPrimitiveTestSetup(testPRIMITIVE04GetRecordWithBaseDate(t)),
			WithPrimitiveTestSetup(testFrozenDataRetrieval(t)),
			WithPrimitiveTestSetup(testPRIMITIVE03_SetReadOnlyMetadataToPrimitiveStream(t)),
		},
	})
}

func WithPrimitiveTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Setup initial data
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: primitiveStreamId,
			Height:   1,
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

		// Run the actual test function
		return testFn(ctx, platform)
	}
}

func testPRIMITIVE01_InsertAndGetRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		// Get records
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			DBID:     dbid,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-05",
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records")
		}

		expected := `
		| date       | value |
		|------------|-------|
		| 2021-01-01 | 1.000000000000000000 |
		| 2021-01-02 | 2.000000000000000000 |
		| 2021-01-03 | 4.000000000000000000 |
		| 2021-01-04 | 5.000000000000000000 |
		| 2021-01-05 | 3.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testGetIndex(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			DBID:     dbid,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-05",
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting index")
		}

		expected := `
		| date       | value  |
		|------------|--------|
		| 2021-01-01 | 100.000000000000000000 |
		| 2021-01-02 | 200.000000000000000000 |
		| 2021-01-03 | 400.000000000000000000 |
		| 2021-01-04 | 500.000000000000000000 |
		| 2021-01-05 | 300.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testGetIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		result, err := procedure.GetIndexChange(ctx, procedure.GetIndexChangeInput{
			Platform: platform,
			DBID:     dbid,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-05",
			Interval: 1,
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting index change")
		}

		expected := `
		| date       | value  |
		|------------|--------|
		| 2021-01-02 | 100.000000000000000000 |
		| 2021-01-03 | 100.000000000000000000 |
		| 2021-01-04 | 25.000000000000000000  |
		| 2021-01-05 | -40.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testGetFirstRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		result, err := procedure.GetFirstRecord(ctx, procedure.GetFirstRecordInput{
			Platform:  platform,
			DBID:      dbid,
			AfterDate: nil,
			Height:    0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting first record")
		}

		expected := `
		| date       | value |
		|------------|-------|
		| 2021-01-01 | 1.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		// get the first record with a date after 2021-01-02
		result, err = procedure.GetFirstRecord(ctx, procedure.GetFirstRecordInput{
			Platform:  platform,
			DBID:      dbid,
			AfterDate: testutils.Ptr("2021-01-02"),
			Height:    0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting first record")
		}

		expected = `
		| date       | value |
		|------------|-------|
		| 2021-01-02 | 2.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		// get the first record with a date after 2021-01-10 (it doesn't exist)
		result, err = procedure.GetFirstRecord(ctx, procedure.GetFirstRecordInput{
			Platform:  platform,
			DBID:      dbid,
			AfterDate: testutils.Ptr("2021-01-10"),
			Height:    0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting first record")
		}

		expected = `
		| date       | value |
		|------------|-------|
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testDuplicateDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		primitiveStreamProvider, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// insert a duplicate date
		err = setup.InsertMarkdownPrimitiveData(ctx, setup.InsertMarkdownDataInput{
			Platform: platform,
			Height:   2, // later height
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: primitiveStreamProvider,
			},
			MarkdownData: `
			| date       | value |
			|------------|-------|
			| 2021-01-01 | 9.000000000000000000 |
			`,
		})

		if err != nil {
			return errors.Wrap(err, "error inserting duplicate date")
		}

		expected := `
		| date       | value |
		|------------|-------|
		| 2021-01-01 | 9.000000000000000000 |
		| 2021-01-02 | 2.000000000000000000 |
		| 2021-01-03 | 4.000000000000000000 |
		| 2021-01-04 | 5.000000000000000000 |
		| 2021-01-05 | 3.000000000000000000 |
		`

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			DBID:     dbid,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-05",
		})

		if err != nil {
			return errors.Wrap(err, "error getting records")
		}

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testPRIMITIVE04GetRecordWithBaseDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		// Define the base_date
		baseDate := "2021-01-03"

		// Get records with base_date
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			DBID:     dbid,
			DateFrom: "2021-01-01",
			DateTo:   "2021-01-05",
			BaseDate: baseDate,
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records with base_date")
		}

		expected := `
		| date       | value |
		|------------|-------|
		| 2021-01-01 | 25.000000000000000000 |
		| 2021-01-02 | 50.000000000000000000 |
		| 2021-01-03 | 100.000000000000000000 | # this is the base date
		| 2021-01-04 | 125.000000000000000000 |
		| 2021-01-05 | 75.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testFrozenDataRetrieval(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		primitiveStreamProvider, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Insert initial data at height 1
		err = setup.InsertMarkdownPrimitiveData(ctx, setup.InsertMarkdownDataInput{
			Platform: platform,
			Height:   1,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: primitiveStreamProvider,
			},
			// we set in 2022 not to mix up with the initial data set in 2021
			MarkdownData: `
            | date       | value |
            |------------|-------|
            | 2022-01-01 | 1     |
            | 2022-01-02 | 2     |
            `,
		})
		if err != nil {
			return errors.Wrap(err, "error inserting initial data")
		}

		// Insert additional data at height 2
		err = setup.InsertMarkdownPrimitiveData(ctx, setup.InsertMarkdownDataInput{
			Platform: platform,
			Height:   2,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: primitiveStreamProvider,
			},
			MarkdownData: `
            | date       | value |
            |------------|-------|
            | 2022-01-01 | 3     |
            | 2022-01-03 | 4     |
            `,
		})
		if err != nil {
			return errors.Wrap(err, "error inserting additional data")
		}

		// Retrieve data frozen at height 1
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			DBID:     dbid,
			DateFrom: "2022-01-01",
			DateTo:   "2022-01-03",
			FrozenAt: 1,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records frozen at height 1")
		}

		expected := `
        | date       | value |
        |------------|-------|
        | 2022-01-01 | 1.000000000000000000 |
        | 2022-01-02 | 2.000000000000000000 |
        `

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		// Retrieve data frozen at height 2
		result, err = procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			DBID:     dbid,
			DateFrom: "2022-01-01",
			DateTo:   "2022-01-03",
			FrozenAt: 2,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records frozen at height 2")
		}

		expected = `
        | date       | value |
        |------------|-------|
        | 2022-01-01 | 3.000000000000000000 |
        | 2022-01-02 | 2.000000000000000000 |
        | 2022-01-03 | 4.000000000000000000 |
        `

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testPRIMITIVE02_UnauthorizedInserts(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Change deployer to a non-authorized wallet
		unauthorizedWallet := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")

		primitiveStreamProvider, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Attempt to insert a record
		err = setup.InsertMarkdownPrimitiveData(ctx, setup.InsertMarkdownDataInput{
			Platform: procedure.WithSigner(platform, unauthorizedWallet.Bytes()),
			Height:   2,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: primitiveStreamProvider,
			},
			MarkdownData: `
            | date       | value |
            |------------|-------|
            | 2021-01-06 | 10    |
            `,
		})

		assert.Error(t, err, "Unauthorized wallet should not be able to insert records")
		assert.Contains(t, err.Error(), "wallet not allowed to write", "Expected permission error")

		return nil
	}
}

func testPRIMITIVE03_SetReadOnlyMetadataToPrimitiveStream(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error { // Change deployer to a non-authorized wallet
		dbid := utils.GenerateDBID(primitiveStreamId.String(), platform.Deployer)

		// Attempt to set metadata
		err := procedure.SetMetadata(ctx, procedure.SetMetadataInput{
			Platform: platform,
			DBID:     dbid,
			Key:      "type",
			Value:    "other",
			ValType:  "string",
			Height:   0,
		})
		assert.Error(t, err, "Cannot insert metadata for read-only key")
		return nil
	}
}
