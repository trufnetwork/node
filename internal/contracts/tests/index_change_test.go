package tests

import (
	"context"
	"github.com/truflation/tsn-db/internal/contracts/tests/utils/setup"
	"testing"

	"github.com/pkg/errors"
	"github.com/truflation/tsn-sdk/core/util"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/types/decimal"
	"github.com/kwilteam/kwil-db/core/utils"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/stretchr/testify/assert"
)

func TestIndexChange(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "index_change_test",
		SchemaFiles: []string{"../primitive_stream_template.kf"},
		FunctionTests: []kwilTesting.TestFunc{
			testIndexChange(t),
			testYoYIndexChange(t),
		},
	})
}

func testIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamName := "primitive_stream_db_name"
		streamId := util.GenerateStreamId(streamName)
		dbid := utils.GenerateDBID(streamId.String(), platform.Deployer)

		if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform:            platform,
			Height:              0,
			PrimitiveStreamName: streamName,
			MarkdownData: `
			| date       | value  |
			|------------|--------|
			| 2023-01-01 | 100.00 |
			| 2023-01-02 | 102.00 |
			| 2023-01-03 | 103.00 |
			| 2023-01-04 | 101.00 |
			# add a gap here just to test the logic
			| 2023-01-06 | 106.00 |
			| 2023-01-07 | 105.00 |
			| 2023-01-08 | 108.00 |
			`,
		}); err != nil {
			return errors.Wrap(err, "error setting up primitive stream")
		}

		// Get index change for 7 days with 1 day interval
		result, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
			Procedure: "get_index_change",
			Dataset:   dbid,
			Args:      []any{"2023-01-02", "2023-01-08", nil, 1},
			TransactionData: common.TransactionData{
				Signer: platform.Deployer,
				TxID:   platform.Txid(),
				Height: 0,
			},
		})
		if err != nil {
			return errors.Wrap(err, "error getting index change")
		}

		// Convert decimal.Decimal values to strings
		convertedResult := make([][]any, len(result.Rows))
		for i, row := range result.Rows {
			convertedRow := make([]any, len(row))
			convertedRow[0] = row[0] // Date remains as string
			if dec, ok := row[1].(*decimal.Decimal); ok {
				convertedRow[1] = dec.String() // Convert to string with 6 decimal places
			} else {
				convertedRow[1] = row[1] // Keep as is if not a decimal.Decimal
			}
			convertedResult[i] = convertedRow
		}

		// Assert the correct output
		expected := [][]any{
			{"2023-01-02", "2.000"},
			{"2023-01-03", "0.980"},
			{"2023-01-04", "-1.942"},
			// remember the gap
			{"2023-01-06", "4.950"}, // it is now using the previous value
			{"2023-01-07", "-0.943"},
			{"2023-01-08", "2.857"},
		}

		assert.Equal(t, expected, convertedResult, "Index change results do not match expected values")

		return nil
	}
}

// testing https://system.docs.truflation.com/backend/cpi-calculations/workflow/yoy-values specification

func testYoYIndexChange(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamName := "primitive_stream_db_name"
		streamId := util.GenerateStreamId(streamName)
		dbid := utils.GenerateDBID(streamId.String(), platform.Deployer)

		/*
			Here’s an example calculation for corn inflation for May 22nd 2023:

			- `Date Target`: May 22nd, 2023
			- `Latest`: We search, starting at May 22nd, 2023, going backward in time, eventually finding an entry at May 1st, 2023
			- `Year Ago`: We search, starting at May 1st, 2022, going backward in time, eventually finding an entry at April 23rd, 2022

			In this example we would perform our math using April 23rd, 2022 and May 1st, 2023
		*/

		// Insert test data for two years
		if err := setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
			Platform:            platform,
			Height:              0,
			PrimitiveStreamName: streamName,
			MarkdownData: `
        | date       | value  |
        |------------|--------|
        | 2022-01-01 | 100.00 |
        | 2022-04-23 | 102.00 |
        | 2022-12-31 | 105.00 |
        | 2023-01-01 | 106.00 |
        | 2023-05-01 | 108.00 |
			`,
		}); err != nil {
			return errors.Wrap(err, "error setting up primitive stream")
		}

		// Test YoY calculation
		result, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
			Procedure: "get_index_change",
			Dataset:   dbid,
			Args:      []any{"2023-05-22", "2023-05-22", nil, 365}, // 365 days interval for YoY
			TransactionData: common.TransactionData{
				Signer: platform.Deployer,
				TxID:   platform.Txid(),
				Height: 0,
			},
		})
		if err != nil {
			return errors.Wrap(err, "error getting index change")
		}

		results := make([][]struct {
			date  string
			value string
		}, len(result.Rows))

		for i, row := range result.Rows {
			results[i] = []struct {
				date  string
				value string
			}{{row[0].(string), row[1].(*decimal.Decimal).String()}}
		}

		// Check if the date is correct
		latestDate := result.Rows[0][0].(string)
		if latestDate != "2023-05-01" {
			return errors.Errorf("incorrect latest date: got %s, expected 2023-05-01", latestDate)
		}

		// 05-01 idx: 8%
		// 04-23 idx: 2%
		// YoY% = (index current - index year ago) / index year ago * 100.0
		// 05-01 yoyChange: 108 - 102 / 102 * 100.0 = 5.882
		// check if 5.882 is in the result
		latestYoyChange := results[0][0].value
		if latestYoyChange != "5.882" {
			return errors.Errorf("incorrect latest yoy change: got %s, expected 5.882", latestYoyChange)
		}

		return nil
	}
}
