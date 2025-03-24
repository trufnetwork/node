package tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/stretchr/testify/assert"
)

var (
	defaultDeployer = util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
)

func TestIndexChange(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "index_change_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			withTestIndexChangeSetup(testIndexChange(t)),
			withTestIndexChangeSetup(testYoYIndexChange(t)),
		},
	}, testutils.GetTestOptions())
}

func withTestIndexChangeSetup(test func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// setup deployer
		return test(ctx, procedure.WithSigner(platform, defaultDeployer.Bytes()))
	}
}

func testIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamName := "primitive_stream_db_name"
		streamId := util.GenerateStreamId(streamName)

		// Create StreamLocator for the stream
		streamLocator := types.StreamLocator{
			StreamId:     streamId,
			DataProvider: defaultDeployer,
		}

		if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			StreamId: streamId,
			Height:   0,

			MarkdownData: `
			| event_time | value  |
			|------------|--------|
			| 1          | 100.00 |
			| 2          | 102.00 |
			| 3          | 103.00 |
			| 4          | 101.00 |
			# add a gap here just to test the logic
			| 6          | 106.00 |
			| 7          | 105.00 |
			| 8          | 108.00 |
			`,
		}); err != nil {
			return errors.Wrap(err, "error setting up primitive stream")
		}

		// Set up parameters for GetIndexChange
		fromTime := int64(2)
		toTime := int64(8)
		interval := 1

		// Get index change for the specified period
		result, err := procedure.GetIndexChange(ctx, procedure.GetIndexChangeInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &fromTime,
			ToTime:        &toTime,
			Interval:      &interval,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting index change")
		}

		// Assert the correct output
		expected := `
		| event_time | value                   |
		| ---------- | ----------------------- |
		| 2          | 2.000000000000000000    |
		| 3          | 0.980392156862745098    |
		| 4          | -1.941747572815533981   |
		| 6          | 4.950495049504950495    | # it is now using the previous value
		| 7          | -0.943396226415094340   |
		| 8          | 2.857142857142857143    |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

// testing https://system.docs.trufnetwork.com/backend/cpi-calculations/workflow/yoy-values specification
func testYoYIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamName := "primitive_stream_db_name"
		streamId := util.GenerateStreamId(streamName)

		// Create StreamLocator for the stream
		streamLocator := types.StreamLocator{
			StreamId:     streamId,
			DataProvider: defaultDeployer,
		}

		/*
			Here's an example calculation for corn inflation for May 22nd 2023:

			- `Date Target`: May 22nd, 2023
			- `Latest`: We search, starting at May 22nd, 2023, going backward in time, eventually finding an entry at May 1st, 2023
			- `Year Ago`: We search, starting at May 1st, 2022, going backward in time, eventually finding an entry at April 23rd, 2022

			In this example we would perform our math using April 23rd, 2022 and May 1st, 2023
		*/

		// Insert test data for two years
		if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform: platform,
			Height:   0,
			StreamId: streamId,
			MarkdownData: `
        | event_time | value  |
        |------------|--------|
        | 1          | 100.00 |
        | 113        | 102.00 |
        | 365        | 105.00 |
        | 366        | 106.00 |
        | 486        | 108.00 |
			`,
		}); err != nil {
			return errors.Wrap(err, "error setting up primitive stream")
		}

		// Set up parameters for GetIndexChange
		fromTime := int64(507) // 2023-05-22
		toTime := int64(507)   // 2023-05-22
		interval := 365        // 365 days interval for YoY

		// Test YoY calculation
		result, err := procedure.GetIndexChange(ctx, procedure.GetIndexChangeInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &fromTime,
			ToTime:        &toTime,
			Interval:      &interval,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting index change")
		}

		// Check if the date is correct
		assert.Equal(t, 1, len(result), "Expected 1 row in the result")
		assert.Equal(t, "486", result[0][0], "Expected event_time to be 486 (2023-05-01)")

		// 05-01 idx: 8%
		// 04-23 idx: 2%
		// YoY% = (index current - index year ago) / index year ago * 100.0
		// 05-01 yoyChange: 108 - 102 / 102 * 100.0 = 5.882
		expected := `
		| event_time | value                   |
		| ---------- | ----------------------- |
		| 486        | 5.882352941176470588    |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}
