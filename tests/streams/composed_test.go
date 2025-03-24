package tests

import (
	"context"
	"testing"

	"github.com/trufnetwork/sdk-go/core/types"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/util"
)

var (
	composedStreamName = "composed_stream"
	composedStreamId   = util.GenerateStreamId(composedStreamName)
	composedDeployer   = util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
)

func TestComposed(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "composed_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testComposedLastAvailable(t),
			testCOMPOSED01SetTaxonomyWithValidData(t),
			WithComposedTestSetup(testCOMPOSED02OnlyOwnerCanSetTaxonomy(t)),
			WithComposedTestSetup(testCOMPOSED04DisableTaxonomy(t)),
			WithComposedTestSetup(testOnlyOwnerCanDisableTaxonomy(t)),
			WithComposedTestSetup(testCOMPOSED03SetReadOnlyMetadataToComposedStream(t)),
		},
	}, testutils.GetTestOptions())
}

func WithComposedTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set the platform signer
		platform = procedure.WithSigner(platform, composedDeployer.Bytes())

		// Create the composed stream
		err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Locator: types.StreamLocator{
				StreamId:     composedStreamId,
				DataProvider: composedDeployer,
			},
			Type: setup.ContractTypeComposed,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		return testFn(ctx, platform)
	}
}

func testComposedLastAvailable(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup the deployer since its not using WithComposedTestSetup
		platform = procedure.WithSigner(platform, composedDeployer.Bytes())

		// Setup data for the test
		err := setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
			Platform: platform,
			StreamId: composedStreamId,
			Height:   1,
			MarkdownData: `
				| event_time | Stream 1 | Stream 2 | Stream 3 |
				| ---------- | -------- | -------- | -------- |
				| 1          | 1        |          | 4        |
				| 2          |          |          |          |
				| 3          |          | 2        | 5        |
				| 4          |          |          | 3        |
			`,
			Weights: []string{"1", "2", "3"},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up last available test data")
		}

		dateFrom := int64(1)
		dateTo := int64(4)

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComposedLastAvailable")
		}

		expected := `
		| event_time | value                  |
		| ---------- | ---------------------- |
		| 1          | 3.250000000000000000   | # 1 & 4
		| 2          |                        |
		| 3          | 3.333333333333333333   | # 1 & 2 & 5
		| 4          | 2.333333333333333333   | # 1 & 2 & 3
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

func testCOMPOSED01SetTaxonomyWithValidData(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup the deployer since its not using WithComposedTestSetup
		platform = procedure.WithSigner(platform, composedDeployer.Bytes())

		// Create the composed stream
		err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Locator: composedStreamLocator,
			Type:    setup.ContractTypeComposed,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		stream1 := util.GenerateStreamId("stream1")
		stream2 := util.GenerateStreamId("stream2")

		// deploy child streams
		if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: stream1,
			Height:   1,
			MarkdownData: `
				| event_time | value |
				| ---------- | ----- |
				| 1          | 5     |
				| 5          | 15    |
			`,
		}); err != nil {
			return errors.Wrap(err, "error setting up child stream 1")
		}

		if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: stream2,
			Height:   1,
			MarkdownData: `
				| event_time | value |
				| ---------- | ----- |
				| 1          | 2     |
				| 5          | 10    |
			`,
		}); err != nil {
			return errors.Wrap(err, "error setting up child stream 2")
		}

		// Set up child streams
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{composedDeployer.Address(), composedDeployer.Address()},
			StreamIds:     []string{stream1.String(), stream2.String()},
			Weights:       []string{"1.0", "2.0"},
			Height:        1,
		})
		if err != nil {
			return errors.Wrap(err, "Failed to set taxonomy")
		}

		// Verify taxonomy is applied in get_record
		dateFrom := int64(1)
		dateTo := int64(31)

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "Failed to get record after setting taxonomy")
		}

		// Expected results based on child streams and weights
		// Assuming child stream1 has weight 1.0 and stream2 has weight 2.0
		// The composed value should be (value_stream1 * 1.0) + (value_stream2 * 2.0)
		expected := `
		| event_time | value                  |
		| ---------- | ---------------------- |
		| 1          | 3.000000000000000000   | # (5 * 1.0 + 2 * 2.0) / (1.0 + 2.0) = 9.0 / 3.0 = 3.0
		| 5          | 11.666666666666666667   | # (15 * 1.0 + 10 * 2.0) / (1.0 + 2.0) = 35.0 / 3.0 = 11.666666666666666667
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

func testCOMPOSED02OnlyOwnerCanSetTaxonomy(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: composedDeployer,
		}

		// Use a non-owner account
		nonOwner := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000001000101")
		platform = procedure.WithSigner(platform, nonOwner.Bytes())

		stream1 := util.GenerateStreamId("stream1")
		// Attempt to set taxonomy with non-owner account
		err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{composedDeployer.Address()},
			StreamIds:     []string{stream1.String()},
			Weights:       []string{"1.0"},
			Height:        1,
		})

		assert.Error(t, err, "Non-owner should not be able to set taxonomy")
		assert.Contains(t, err.Error(), "wallet not allowed to write", "Expected owner-only error")

		return nil
	}
}

func testCOMPOSED04DisableTaxonomy(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: composedDeployer,
		}

		//  setup primitive streams
		stream1 := util.GenerateStreamId("stream1")
		if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: stream1,
			Height:   1,
			MarkdownData: `
				| event_time | value |
				| ---------- | ----- |
				| 1          | 5     |
			`,
		}); err != nil {
			return errors.Wrap(err, "error setting up child stream 1")
		}

		// Set taxonomy version 1
		err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{composedDeployer.Address()},
			StreamIds:     []string{stream1.String()},
			Weights:       []string{"1.0"},
			Height:        1,
		})
		if err != nil {
			return errors.Wrap(err, "Failed to set taxonomy version 1")
		}

		// Disable taxonomy version 1
		err = procedure.DisableTaxonomy(ctx, procedure.DisableTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			GroupSequence: 1,
			Height:        1,
		})
		if err != nil {
			return errors.Wrap(err, "Failed to disable taxonomy version 1")
		}

		// Attempt to retrieve data after disabling taxonomy
		dateFrom := int64(1)
		dateTo := int64(31)

		result, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "Failed to get record after disabling taxonomy")
		}

		// Assert that no data is returned or matches expectations
		expected := `
		| event_time | value |
		| ---------- | ----- |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

func testOnlyOwnerCanDisableTaxonomy(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: composedDeployer,
		}

		// Use a non-owner account
		nonOwner := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000001000001")
		platform = procedure.WithSigner(platform, nonOwner.Bytes())

		// Attempt to disable taxonomy with non-owner account
		err := procedure.DisableTaxonomy(ctx, procedure.DisableTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			GroupSequence: 1,
			Height:        1,
		})

		assert.Error(t, err, "Non-owner should not be able to disable taxonomy")
		assert.Contains(t, err.Error(), "wallet not allowed to write", "Expected owner-only error")

		return nil
	}
}

func testCOMPOSED03SetReadOnlyMetadataToComposedStream(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the composed stream
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: composedDeployer,
		}

		// Attempt to set metadata
		err := procedure.SetMetadata(ctx, procedure.SetMetadataInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			Key:           "type",
			Value:         "other",
			ValType:       "string",
			Height:        0,
		})
		assert.Error(t, err, "Cannot insert metadata for read-only key")
		return nil
	}
}
