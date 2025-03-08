package tests

import (
	"context"
	"testing"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	testutils "github.com/trufnetwork/node/tests/streams/utils"

	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

const primitiveStreamName = "primitive_stream_000000000000001"

var primitiveStreamId = util.GenerateStreamId(primitiveStreamName)

func TestPrimitiveStream(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "primitive_test",
		SeedScripts: testutils.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testPRIMITIVE01_DataInsertion(t),
			WithPrimitiveTestSetup(testPRIMITIVE01_InsertAndGetRecord(t)),
			// WithPrimitiveTestSetup(testPRIMITIVE07_GetRecordWithFutureDate(t)),
			// //WithPrimitiveTestSetup(testPRIMITIVE02_UnauthorizedInserts(t)),
			// WithPrimitiveTestSetup(testPRIMITIVE05GetIndex(t)),
			// WithPrimitiveTestSetup(testPRIMITIVE06GetIndexChange(t)),
			// WithPrimitiveTestSetup(testPRIMITIVE07GetFirstRecord(t)),
			// WithPrimitiveTestSetup(testDuplicateDate(t)),
			// WithPrimitiveTestSetup(testPRIMITIVE04GetRecordWithBaseDate(t)),
			// WithPrimitiveTestSetup(testFrozenDataRetrieval(t)),
			// //WithPrimitiveTestSetup(testPRIMITIVE03_SetReadOnlyMetadataToPrimitiveStream(t)),
			// WithPrimitiveTestSetup(testPRIMITIVE08_AdditionalInsertWillFetchLatestRecord(t)),
		},
	}, testutils.GetTestOptions())
}

func testPRIMITIVE01_DataInsertion(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		validAddress := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000001")
		platform = procedure.WithSigner(platform, validAddress.Bytes())
		streamLocator := types.StreamLocator{
			StreamId:     primitiveStreamId,
			DataProvider: validAddress,
		}
		_, err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: streamLocator,
		})
		if err != nil {
			return errors.Wrap(err, "valid address should be accepted")
		}
		assert.NoError(t, err, "valid address should be accepted")

		// Setup initial data
		err = setup.ExecuteInsertRecord(ctx, platform, streamLocator, setup.InsertRecordInput{
			EventTime: 1612137600,
			Value:     1,
		}, 0)
		if err != nil {
			return errors.Wrap(err, "error inserting initial data")
		}
		assert.NoError(t, err, "error inserting initial data")

		return nil
	}
}

func WithPrimitiveTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		platform.Deployer = deployer.Bytes()

		// Setup initial data
		err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: primitiveStreamId,
			Height:   1,
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 1          | 1     |
			| 2          | 2     |
			| 3          | 4     |
			| 4          | 5     |
			| 5          | 3     |
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
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get records
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 1,
			ToTime:   5,
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records")
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 1.000000000000000000 |
		| 2          | 2.000000000000000000 |
		| 3          | 4.000000000000000000 |
		| 4          | 5.000000000000000000 |
		| 5          | 3.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testPRIMITIVE07_GetRecordWithFutureDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get records
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 6,
			ToTime:   6,
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records")
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 5          | 3.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testPRIMITIVE05GetIndex(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 1,
			ToTime:   5,
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting index")
		}

		expected := `
		| event_time | value  |
		|------------|--------|
		| 1          | 100.000000000000000000 |
		| 2          | 200.000000000000000000 |
		| 3          | 400.000000000000000000 |
		| 4          | 500.000000000000000000 |
		| 5          | 300.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testPRIMITIVE06GetIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		result, err := procedure.GetIndexChange(ctx, procedure.GetIndexChangeInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 1,
			ToTime:   5,
			Interval: 1,
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting index change")
		}

		expected := `
		| event_time | value  |
		|------------|--------|
		| 2          | 100.000000000000000000 |
		| 3          | 100.000000000000000000 |
		| 4          | 25.000000000000000000  |
		| 5          | -40.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testPRIMITIVE07GetFirstRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		result, err := procedure.GetFirstRecord(ctx, procedure.GetFirstRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			AfterTime: 0,
			Height:    0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting first record")
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 1.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		// get the first record with a time after 2
		result, err = procedure.GetFirstRecord(ctx, procedure.GetFirstRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			AfterTime: 2,
			Height:    0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting first record")
		}

		expected = `
		| event_time | value |
		|------------|-------|
		| 2          | 2.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		// get the first record with a time after 10 (it doesn't exist)
		result, err = procedure.GetFirstRecord(ctx, procedure.GetFirstRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			AfterTime: 10,
			Height:    0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting first record")
		}

		expected = `
		| event_time | value |
		|------------|-------|
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testDuplicateDate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// insert a duplicate date
		err = setup.InsertMarkdownPrimitiveData(ctx, setup.InsertMarkdownDataInput{
			Platform: platform,
			Height:   2, // later height
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 1          | 9     |
			`,
		})

		if err != nil {
			return errors.Wrap(err, "error inserting duplicate date")
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 9.000000000000000000 |
		| 2          | 2.000000000000000000 |
		| 3          | 4.000000000000000000 |
		| 4          | 5.000000000000000000 |
		| 5          | 3.000000000000000000 |
		`

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 1,
			ToTime:   5,
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
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Define the base_time
		baseTime := int64(3)

		// Get records with base_time
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 1,
			ToTime:   5,
			BaseTime: baseTime,
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records with base_time")
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 1          | 25.000000000000000000 |
		| 2          | 50.000000000000000000 |
		| 3          | 100.000000000000000000 | # this is the base time
		| 4          | 125.000000000000000000 |
		| 5          | 75.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testFrozenDataRetrieval(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Insert initial data at height 1
		err = setup.InsertMarkdownPrimitiveData(ctx, setup.InsertMarkdownDataInput{
			Platform: platform,
			Height:   1,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			// we set different event times not to mix up with the initial data
			MarkdownData: `
            | event_time | value |
            |------------|-------|
            | 101        | 1     |
            | 102        | 2     |
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
				DataProvider: deployer,
			},
			MarkdownData: `
            | event_time | value |
            |------------|-------|
            | 101        | 3     |
            | 103        | 4     |
            `,
		})
		if err != nil {
			return errors.Wrap(err, "error inserting additional data")
		}

		// Retrieve data frozen at height 1
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 101,
			ToTime:   103,
			FrozenAt: 1,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records frozen at height 1")
		}

		expected := `
        | event_time | value |
        |------------|-------|
        | 101        | 1.000000000000000000 |
        | 102        | 2.000000000000000000 |
        `

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		// Retrieve data frozen at height 2
		result, err = procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 101,
			ToTime:   103,
			FrozenAt: 2,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records frozen at height 2")
		}

		expected = `
        | event_time | value |
        |------------|-------|
        | 101        | 3.000000000000000000 |
        | 102        | 2.000000000000000000 |
        | 103        | 4.000000000000000000 |
        `

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

func testPRIMITIVE08_AdditionalInsertWillFetchLatestRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Insert additional data at height 2
		err = setup.InsertMarkdownPrimitiveData(ctx, setup.InsertMarkdownDataInput{
			Platform: platform,
			Height:   2,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			MarkdownData: `
			| event_time | value |
			|------------|-------|
			| 5          | 5     |
			`,
		})
		if err != nil {
			return errors.Wrap(err, "error inserting additional data")
		}

		// Get records
		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     primitiveStreamId,
				DataProvider: deployer,
			},
			FromTime: 5,
			ToTime:   5,
			Height:   0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting records")
		}

		expected := `
		| event_time | value |
		|------------|-------|
		| 5          | 5.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}
